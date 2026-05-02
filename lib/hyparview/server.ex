defmodule HyParView.Server do
  @moduledoc """
  GenServer wrapping `HyParView.State` with timers, transport, and
  subscriber notifications.

  This is the typical entry point for applications. Power users that
  want to drive the protocol from their own event loop can use
  `HyParView.State` directly and skip this module.

  ## Lifecycle

    1. `start_link/1` validates options, builds a fresh `State`, opens
       the configured transport, and schedules the first shuffle tick.
    2. After init, `handle_continue/2` runs the joiner-side handshake
       against each `:contacts` peer in order (the first one that's
       reachable is enough — subsequent JOIN traffic propagates the
       cluster's membership to us).
    3. Inbound transport messages arrive as `{:transport_message,
       from_peer, msg}` Erlang messages and are dispatched to
       `State.handle_message/2`.
    4. The shuffle tick re-schedules itself every `:shuffle_interval` ms.
    5. Subscribers (registered via `subscribe/2`) receive
       `{:hyparview, {:peer_up | :peer_down, peer}}` messages on view
       changes. Subscribers are monitored and pruned automatically.

  ## Telemetry

  All view changes emit `:telemetry` events under the configured
  prefix (default `[:hyparview]`):

    * `[:hyparview, :peer, :added]` — `metadata: %{peer, view: :active}`
    * `[:hyparview, :peer, :removed]` — same metadata
    * `[:hyparview, :send]` — every outbound message; metadata includes
      the message struct module
  """

  use GenServer

  require Logger

  alias HyParView.{Config, Peer, State}

  @typedoc "Options accepted by `start_link/1`."
  @type start_option ::
          {:peer, Peer.t()}
          | {:contacts, [Peer.t()]}
          | {:config, keyword() | Config.t()}
          | {:transport, module()}
          | {:transport_opts, keyword()}
          | {:rng_seed, term()}
          | {:name, GenServer.name()}
          | {:telemetry_prefix, [atom()]}

  @typedoc "Server state held by the GenServer."
  @type server_state :: %{
          required(:state) => State.t(),
          required(:transport) => module(),
          required(:transport_state) => term(),
          required(:subscribers) => %{reference() => pid()},
          required(:telemetry_prefix) => [atom()]
        }

  # ── Public API ───────────────────────────────────────────────────────

  @doc """
  Start a HyParView server.

  ## Required options

    * `:peer` — `%HyParView.Peer{}` for the local node.
    * `:transport` — a module implementing `HyParView.Transport`.

  ## Optional

    * `:contacts` — list of `%Peer{}` to JOIN against, tried in order.
    * `:config` — keyword list passed to `HyParView.Config.new/1`, or a
      pre-built `%Config{}`.
    * `:transport_opts` — passed to the transport (currently unused by
      the test transport).
    * `:rng_seed` — `:rand` seed for deterministic tests.
    * `:name` — registered name for the server.
    * `:telemetry_prefix` — overrides the default `[:hyparview]` prefix.
  """
  @spec start_link([start_option()]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name)

    if name do
      GenServer.start_link(__MODULE__, opts, name: name)
    else
      GenServer.start_link(__MODULE__, opts)
    end
  end

  @doc "Return the current active view as a list of peers."
  @spec active_view(GenServer.server()) :: [Peer.t()]
  def active_view(server), do: GenServer.call(server, :active_view)

  @doc "Return the current passive view as a list of peers."
  @spec passive_view(GenServer.server()) :: [Peer.t()]
  def passive_view(server), do: GenServer.call(server, :passive_view)

  @doc """
  Subscribe `pid` (defaults to caller) to view-change notifications.

  Subscribers receive `{:hyparview, {:peer_up | :peer_down, %Peer{}}}`
  on every change. The server monitors the subscriber and removes it
  on exit.

  ## Options

    * `:replay` — when `true`, the server immediately sends a
      `{:hyparview, {:peer_up, peer}}` for every peer currently in the
      active view, before any future events. Useful for late subscribers
      that need a complete picture without an additional `active_view/1`
      call. Defaults to `false`.
  """
  @spec subscribe(GenServer.server(), pid(), keyword()) :: :ok
  def subscribe(server, pid \\ self(), opts \\ []) do
    GenServer.call(server, {:subscribe, pid, opts})
  end

  @doc "Stop the server gracefully."
  @spec stop(GenServer.server()) :: :ok
  def stop(server), do: GenServer.stop(server)

  @doc """
  Signal that the connection to `peer` has been lost (failure detector).

  Triggers reactive recovery: the peer is removed from the active view,
  and a NEIGHBOR request is sent to a random passive peer to refill.
  Used by the TCP transport's connection process; tests can call this
  directly to simulate a node failure.
  """
  @spec connection_lost(GenServer.server(), Peer.t()) :: :ok
  def connection_lost(server, %Peer{} = peer) do
    GenServer.cast(server, {:connection_lost, peer})
  end

  # ── GenServer callbacks ─────────────────────────────────────────────

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    peer = Keyword.fetch!(opts, :peer)
    transport = Keyword.fetch!(opts, :transport)
    transport_opts = Keyword.get(opts, :transport_opts, [])
    contacts = Keyword.get(opts, :contacts, [])
    rng_seed = Keyword.get(opts, :rng_seed)

    config =
      case Keyword.get(opts, :config, []) do
        %Config{} = c -> c
        list when is_list(list) -> Config.new(list)
      end

    telemetry_prefix =
      Keyword.get(
        opts,
        :telemetry_prefix,
        Application.get_env(:hyparview, :telemetry_prefix, [:hyparview])
      )

    state_opts = if rng_seed, do: [rng_seed: rng_seed], else: []
    state = State.new(peer, config, state_opts)

    Process.set_label({HyParView.Server, peer.id})

    me = self()
    events = build_event_callback(me)

    case transport.listen(peer, events) do
      {:ok, transport_state} ->
        Process.send_after(self(), :shuffle_tick, config.shuffle_interval)

        internal = %{
          state: state,
          transport: transport,
          transport_state: transport_state,
          subscribers: %{},
          telemetry_prefix: telemetry_prefix
        }

        _ = transport_opts
        {:ok, internal, {:continue, {:join, contacts}}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl GenServer
  def handle_continue({:join, contacts}, internal) do
    {:noreply, drive_initial_join(internal, contacts)}
  end

  @impl GenServer
  def handle_info({:transport_message, _from_peer, msg}, internal) do
    {state, actions} = State.handle_message(internal.state, msg)
    {:noreply, apply_actions(%{internal | state: state}, actions)}
  end

  def handle_info({:transport_peer_lost, peer}, internal) do
    {state, actions} = State.connection_lost(internal.state, peer)
    {:noreply, apply_actions(%{internal | state: state}, actions)}
  end

  def handle_info(:shuffle_tick, internal) do
    {state, actions} = State.tick_shuffle(internal.state)
    internal = apply_actions(%{internal | state: state}, actions)
    Process.send_after(self(), :shuffle_tick, state.config.shuffle_interval)
    {:noreply, internal}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, internal) do
    {:noreply, %{internal | subscribers: Map.delete(internal.subscribers, ref)}}
  end

  # Trapped exit signals from linked processes. `:normal` is ignored;
  # `:shutdown` (typically from a supervisor) translates to a normal stop
  # so the standard "GenServer crashed" log isn't emitted. Other reasons
  # propagate so genuine crashes are surfaced.
  def handle_info({:EXIT, _pid, :normal}, internal), do: {:noreply, internal}
  def handle_info({:EXIT, _pid, :shutdown}, internal), do: {:stop, :normal, internal}
  def handle_info({:EXIT, _pid, reason}, internal), do: {:stop, reason, internal}

  @impl GenServer
  def handle_cast({:connection_lost, peer}, internal) do
    {state, actions} = State.connection_lost(internal.state, peer)
    {:noreply, apply_actions(%{internal | state: state}, actions)}
  end

  @impl GenServer
  def handle_call(:active_view, _from, internal) do
    {:reply, State.active_peers(internal.state), internal}
  end

  def handle_call(:passive_view, _from, internal) do
    {:reply, State.passive_peers(internal.state), internal}
  end

  def handle_call({:subscribe, pid, opts}, _from, internal) do
    ref = Process.monitor(pid)

    if Keyword.get(opts, :replay, false) do
      for peer <- State.active_peers(internal.state) do
        send(pid, {:hyparview, {:peer_up, peer}})
      end
    end

    {:reply, :ok, %{internal | subscribers: Map.put(internal.subscribers, ref, pid)}}
  end

  @impl GenServer
  def terminate(_reason, internal) do
    internal.transport.close(internal.transport_state)
    :ok
  end

  # ── Internals ───────────────────────────────────────────────────────

  # Build the callback handed to `transport.listen/2`. Translates each
  # transport event into an internal Erlang message destined for `me`
  # (this server's pid).
  @spec build_event_callback(pid()) :: HyParView.Transport.event_callback()
  defp build_event_callback(me) do
    fn
      {:message, from_peer, msg} ->
        send(me, {:transport_message, from_peer, msg})
        :ok

      {:peer_lost, peer} ->
        send(me, {:transport_peer_lost, peer})
        :ok
    end
  end

  defp drive_initial_join(internal, []), do: internal

  defp drive_initial_join(internal, [contact | _rest]) do
    {state, actions} = State.initiate_join(internal.state, contact)
    apply_actions(%{internal | state: state}, actions)
  end

  defp apply_actions(internal, actions) do
    Enum.reduce(actions, internal, &apply_action(&2, &1))
  end

  defp apply_action(internal, {:send, target, msg}) do
    case internal.transport.send_message(internal.transport_state, target, msg) do
      :ok ->
        emit_telemetry(internal, [:send], %{}, %{
          target: target,
          message: msg.__struct__
        })

        internal

      {:error, reason} ->
        Logger.debug(fn ->
          "HyParView: send to #{inspect(target.id)} failed (#{inspect(reason)})"
        end)

        emit_telemetry(internal, [:send_error], %{}, %{
          target: target,
          message: msg.__struct__,
          reason: reason
        })

        internal
    end
  end

  defp apply_action(internal, {:notify_up, peer}) do
    notify_subscribers(internal, {:peer_up, peer})
    emit_telemetry(internal, [:peer, :added], %{count: 1}, %{peer: peer, view: :active})
    internal
  end

  defp apply_action(internal, {:notify_down, peer}) do
    notify_subscribers(internal, {:peer_down, peer})
    emit_telemetry(internal, [:peer, :removed], %{count: 1}, %{peer: peer, view: :active})
    internal
  end

  defp notify_subscribers(internal, event) do
    for {_ref, pid} <- internal.subscribers do
      send(pid, {:hyparview, event})
    end
  end

  defp emit_telemetry(internal, event, measurements, metadata) do
    :telemetry.execute(internal.telemetry_prefix ++ event, measurements, metadata)
  end
end
