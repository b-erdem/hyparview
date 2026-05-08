defmodule HyParView.Transport.TCP do
  @moduledoc """
  Plain TCP transport using `:gen_tcp` and one `HyParView.Connection`
  process per peer.

  ## Address format

  `peer.address` must be a `{ip, port}` tuple where `ip` is one of:

    * a 4-tuple (IPv4) or 8-tuple (IPv6) suitable for `:gen_tcp.connect/3`,
    * a charlist (`~c"127.0.0.1"`) or binary (`"127.0.0.1"`) — both are
      passed through `:inet.parse_address/1`.

  ## Lifecycle

    1. `c:HyParView.Transport.listen/2` starts a transport `GenServer` that
       opens a listening socket and an accept loop.
    2. Inbound: each accepted socket spawns an inbound `Connection` which
       performs the handshake and registers itself with the transport.
    3. Outbound: `c:HyParView.Transport.send_message/3` looks up an existing
       Connection by peer id; if none, starts an outbound Connection. The
       send is postponed inside the Connection until handshake completes.
    4. On TCP close, the Connection emits `{:peer_lost, peer}` through the
       events callback registered at `c:HyParView.Transport.listen/2`, which
       the Server translates into `HyParView.State.connection_lost/2`. The
       protocol-level repair (NEIGHBOR to a passive peer) fires automatically.

  ## Notes & limitations

    * No reconnection logic. If a connection breaks, the protocol layer
      handles repair via NEIGHBOR.
    * No connection auth. Authentication and TLS are out of scope; wrap
      this transport (or replace it) for those concerns.
    * A peer that simultaneously initiates *and* accepts a connection from
      the same counterpart will get two parallel connections; the second
      one drops on idle. A pickier handshake could resolve this.
  """

  @behaviour HyParView.Transport

  use GenServer

  require Logger

  alias HyParView.{Connection, Peer}

  @type state :: %{
          required(:pid) => pid(),
          required(:address) => term()
        }

  @impl HyParView.Transport
  def listen(%Peer{} = local_peer, events) when is_function(events, 1) do
    case GenServer.start_link(__MODULE__, %{local_peer: local_peer, events: events}) do
      {:ok, pid} -> {:ok, %{pid: pid, address: local_peer.address}}
      {:error, _} = err -> err
    end
  end

  @impl HyParView.Transport
  def send_message(%{pid: pid}, %Peer{} = target, message) do
    GenServer.call(pid, {:send_message, target, message})
  end

  @impl HyParView.Transport
  def close(%{pid: pid}) do
    try do
      if Process.alive?(pid), do: GenServer.stop(pid)
    catch
      :exit, _ -> :ok
    end

    :ok
  end

  # ── GenServer ───────────────────────────────────────────────────────

  @impl GenServer
  def init(%{local_peer: peer, events: events}) do
    Process.flag(:trap_exit, true)
    {ip, port} = peer.address

    listen_opts = [
      :binary,
      packet: 4,
      active: false,
      reuseaddr: true,
      nodelay: true,
      ip: parse_ip(ip)
    ]

    case :gen_tcp.listen(port, listen_opts) do
      {:ok, listener} ->
        me = self()
        accept_pid = spawn_link(fn -> accept_loop(listener, me) end)

        {:ok,
         %{
           local_peer: peer,
           events: events,
           listener: listener,
           accept_pid: accept_pid,
           connections: %{},
           pids_to_id: %{}
         }}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  defp accept_loop(listener, parent) do
    case :gen_tcp.accept(listener) do
      {:ok, socket} ->
        # The accept_loop is the current owner of the accepted socket. We
        # must transfer ownership to the Transport (parent) before sending
        # the socket reference, so the Transport can in turn transfer it
        # to a freshly-spawned Connection process.
        :ok = :gen_tcp.controlling_process(socket, parent)
        send(parent, {:accepted, socket})
        accept_loop(listener, parent)

      {:error, :closed} ->
        :ok

      {:error, _other} ->
        accept_loop(listener, parent)
    end
  end

  @impl GenServer
  def handle_info({:accepted, socket}, state) do
    case Connection.start_link_inbound(
           socket: socket,
           local_peer: state.local_peer,
           events: state.events,
           transport: self()
         ) do
      {:ok, conn_pid} ->
        # Transfer the socket BEFORE telling Connection it can start
        # `setopts`-ing. The two-phase dance avoids a race where
        # Connection's `init/1` would touch the socket before this
        # transfer landed (manifesting as `controlling_process/2`
        # returning `{:error, :badarg}` because Connection died from a
        # `:setopts_failed` shutdown).
        :ok = :gen_tcp.controlling_process(socket, conn_pid)
        :ok = Connection.start_receiving(conn_pid)
        ref = Process.monitor(conn_pid)
        {:noreply, %{state | pids_to_id: Map.put(state.pids_to_id, ref, conn_pid)}}

      {:error, reason} ->
        Logger.debug("HyParView.Transport.TCP: inbound spawn failed: #{inspect(reason)}")
        :gen_tcp.close(socket)
        {:noreply, state}
    end
  end

  def handle_info({:connection_established, %Peer{id: peer_id}, conn_pid}, state) do
    state = %{state | connections: Map.put(state.connections, peer_id, conn_pid)}
    {:noreply, state}
  end

  def handle_info({:connection_closed, peer_id}, state) do
    state = remove_connection(state, peer_id)
    {:noreply, state}
  end

  def handle_info({:DOWN, _ref, :process, dead_pid, _reason}, state) do
    state = remove_pid(state, dead_pid)
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:noreply, state}

  @impl GenServer
  def handle_call({:send_message, target, message}, _from, state) do
    case ensure_connection(state, target) do
      {:ok, conn_pid, state} ->
        result =
          try do
            Connection.send_message(conn_pid, message)
          catch
            :exit, _ -> {:error, :connection_dead}
          end

        {:reply, result, state}

      {:error, _reason} = err ->
        {:reply, err, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    :gen_tcp.close(state.listener)

    for {_, pid} <- state.connections, Process.alive?(pid) do
      :gen_statem.stop(pid, :normal, 100)
    end

    :ok
  end

  # ── Helpers ─────────────────────────────────────────────────────────

  defp ensure_connection(state, %Peer{id: id} = target) do
    case Map.get(state.connections, id) do
      pid when is_pid(pid) ->
        if Process.alive?(pid),
          do: {:ok, pid, state},
          else: open_outbound(remove_connection(state, id), target)

      nil ->
        open_outbound(state, target)
    end
  end

  defp open_outbound(state, target) do
    case Connection.start_link_outbound(
           local_peer: state.local_peer,
           remote_peer: target,
           events: state.events,
           transport: self()
         ) do
      {:ok, conn_pid} ->
        ref = Process.monitor(conn_pid)

        state =
          state
          |> put_in([:connections, target.id], conn_pid)
          |> put_in([:pids_to_id, ref], conn_pid)

        {:ok, conn_pid, state}

      {:error, _} = err ->
        err
    end
  end

  defp remove_connection(state, peer_id) do
    %{state | connections: Map.delete(state.connections, peer_id)}
  end

  defp remove_pid(state, dead_pid) do
    %{
      state
      | connections:
          state.connections
          |> Enum.reject(fn {_, pid} -> pid == dead_pid end)
          |> Map.new(),
        pids_to_id:
          state.pids_to_id
          |> Enum.reject(fn {_, pid} -> pid == dead_pid end)
          |> Map.new()
    }
  end

  defp parse_ip(ip) when is_tuple(ip), do: ip

  defp parse_ip(ip) when is_binary(ip) do
    {:ok, parsed} = :inet.parse_address(String.to_charlist(ip))
    parsed
  end
end
