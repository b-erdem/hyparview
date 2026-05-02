defmodule HyParView.Connection do
  @moduledoc """
  Per-peer TCP connection process for `HyParView.Transport.TCP`.

  A `:gen_statem` with three states:

      :outbound_connecting  -- (outbound only) gen_tcp.connect is in flight
      :handshaking          -- exchanging Hello frames so each side learns the
                               other's `Peer`
      :connected            -- normal HyParView message flow

  Send calls in the first two states are *postponed* (gen_statem feature) and
  replayed once we transition to `:connected`, so the very first message on a
  fresh connection (typically a JOIN) doesn't get dropped.

  ## Wire format

  All frames are length-prefixed by `gen_tcp`'s `packet: 4`. Within each frame:

    * Hello frame:   `<<0xCAFEBABE::32, 1::8, term-encoded peer::binary>>`
    * Protocol frame: `<<0x48504956::32, version::8, term-encoded msg::binary>>`
      (see `HyParView.Protocol`).

  Both use `:erlang.term_to_binary(_, minor_version: 2)` for stability and
  decode with `:safe` to forbid atom creation from the wire.
  """

  @behaviour :gen_statem

  alias HyParView.{Peer, Protocol}

  @hello_magic 0xCAFEBABE
  @hello_version 1

  @typedoc "Internal data carried across states."
  @type data :: %__MODULE__{
          local_peer: Peer.t(),
          deliver: HyParView.Transport.deliver(),
          transport: pid(),
          mode: :outbound | :inbound,
          remote_peer: Peer.t() | nil,
          socket: :inet.socket() | nil
        }

  @enforce_keys [:local_peer, :deliver, :transport, :mode]
  defstruct [:local_peer, :deliver, :transport, :mode, :remote_peer, :socket]

  # ── Public API ───────────────────────────────────────────────────────

  @doc """
  Start an outbound connection to `:remote_peer`'s address. The Connection
  performs the handshake and notifies `:transport` once `:connected`.
  """
  @spec start_link_outbound(keyword()) :: :gen_statem.start_ret()
  def start_link_outbound(opts) do
    :gen_statem.start_link(__MODULE__, [{:mode, :outbound} | opts], [])
  end

  @doc """
  Start an inbound connection holding `:socket` (already accepted). The
  caller must transfer the socket via `:gen_tcp.controlling_process/2`.
  """
  @spec start_link_inbound(keyword()) :: :gen_statem.start_ret()
  def start_link_inbound(opts) do
    :gen_statem.start_link(__MODULE__, [{:mode, :inbound} | opts], [])
  end

  @doc """
  Send a HyParView protocol message over the connection. Postponed until
  the connection finishes its handshake.
  """
  @spec send_message(pid(), HyParView.Messages.t()) :: :ok | {:error, term()}
  def send_message(pid, message), do: :gen_statem.call(pid, {:send, message})

  # ── gen_statem callbacks ────────────────────────────────────────────

  @impl :gen_statem
  def callback_mode, do: :state_functions

  @impl :gen_statem
  def init(opts) do
    data = struct!(__MODULE__, Map.new(opts))

    case data.mode do
      :outbound ->
        remote = Keyword.fetch!(opts, :remote_peer)
        data = %{data | remote_peer: remote}
        {:ok, :outbound_connecting, data, [{:next_event, :internal, :connect}]}

      :inbound ->
        socket = Keyword.fetch!(opts, :socket)
        :ok = :inet.setopts(socket, active: :once)
        {:ok, :handshaking, %{data | socket: socket}}
    end
  end

  @impl :gen_statem
  def terminate(_reason, _state, data) do
    if data.socket, do: :gen_tcp.close(data.socket)
    :ok
  end

  # ── State: :outbound_connecting ─────────────────────────────────────

  def outbound_connecting(:internal, :connect, data) do
    {ip, port} = data.remote_peer.address

    opts = [:binary, packet: 4, active: false, send_timeout: 5_000]

    case :gen_tcp.connect(parse_ip(ip), port, opts, 5_000) do
      {:ok, socket} ->
        case :gen_tcp.send(socket, encode_hello(data.local_peer)) do
          :ok ->
            :ok = :inet.setopts(socket, active: :once)
            {:next_state, :handshaking, %{data | socket: socket}}

          {:error, reason} ->
            :gen_tcp.close(socket)
            {:stop, {:shutdown, {:hello_send_failed, reason}}}
        end

      {:error, reason} ->
        {:stop, {:shutdown, {:connect_failed, reason}}}
    end
  end

  def outbound_connecting({:call, _from}, {:send, _msg}, _data) do
    {:keep_state_and_data, [{:postpone, true}]}
  end

  # ── State: :handshaking ─────────────────────────────────────────────

  def handshaking(:info, {:tcp, sock, bytes}, %__MODULE__{socket: sock} = data) do
    case decode_hello(bytes) do
      {:ok, remote} ->
        case maybe_send_hello(data, sock) do
          :ok ->
            :ok = :inet.setopts(sock, active: :once)
            send(data.transport, {:connection_established, remote, self()})
            {:next_state, :connected, %{data | remote_peer: remote}}

          {:error, reason} ->
            {:stop, {:shutdown, {:hello_send_failed, reason}}}
        end

      {:error, reason} ->
        {:stop, {:shutdown, {:bad_hello, reason}}}
    end
  end

  def handshaking(:info, {:tcp_closed, sock}, %__MODULE__{socket: sock}) do
    {:stop, {:shutdown, :tcp_closed_during_handshake}}
  end

  def handshaking(:info, {:tcp_error, sock, reason}, %__MODULE__{socket: sock}) do
    {:stop, {:shutdown, {:tcp_error, reason}}}
  end

  def handshaking({:call, _from}, {:send, _msg}, _data) do
    {:keep_state_and_data, [{:postpone, true}]}
  end

  defp maybe_send_hello(%__MODULE__{mode: :outbound}, _sock), do: :ok

  defp maybe_send_hello(%__MODULE__{mode: :inbound, local_peer: peer}, sock) do
    :gen_tcp.send(sock, encode_hello(peer))
  end

  # ── State: :connected ───────────────────────────────────────────────

  def connected(:info, {:tcp, sock, bytes}, %__MODULE__{socket: sock} = data) do
    case Protocol.decode(bytes) do
      {:ok, msg} ->
        data.deliver.(data.remote_peer, msg)

      {:error, _reason} ->
        :ok
    end

    :ok = :inet.setopts(sock, active: :once)
    {:keep_state, data}
  end

  def connected(:info, {:tcp_closed, sock}, %__MODULE__{socket: sock} = data) do
    notify_closed(data)
    {:stop, :normal}
  end

  def connected(:info, {:tcp_error, sock, _reason}, %__MODULE__{socket: sock} = data) do
    notify_closed(data)
    {:stop, :normal}
  end

  def connected({:call, from}, {:send, msg}, %__MODULE__{} = data) do
    bytes = Protocol.encode(msg)

    case :gen_tcp.send(data.socket, bytes) do
      :ok ->
        {:keep_state_and_data, [{:reply, from, :ok}]}

      {:error, reason} = err ->
        notify_closed(data)
        {:stop_and_reply, :normal, [{:reply, from, err}], data, [{:reason, reason}]}
    end
  end

  defp notify_closed(%__MODULE__{remote_peer: %Peer{id: id}, transport: tp}) do
    send(tp, {:connection_closed, id})
  end

  defp notify_closed(_data), do: :ok

  # ── Wire format helpers ─────────────────────────────────────────────

  defp encode_hello(%Peer{} = peer) do
    bin = :erlang.term_to_binary(peer, minor_version: 2)
    <<@hello_magic::32, @hello_version::8, bin::binary>>
  end

  defp decode_hello(<<@hello_magic::32, @hello_version::8, bin::binary>>) do
    case :erlang.binary_to_term(bin, [:safe]) do
      %Peer{} = peer -> {:ok, peer}
      _ -> {:error, :not_peer}
    end
  rescue
    ArgumentError -> {:error, :bad_term}
  end

  defp decode_hello(<<@hello_magic::32, version::8, _::binary>>),
    do: {:error, {:unsupported_hello_version, version}}

  defp decode_hello(_), do: {:error, :bad_format}

  defp parse_ip(ip) when is_tuple(ip), do: ip
  defp parse_ip(ip) when is_list(ip), do: ip

  defp parse_ip(ip) when is_binary(ip) do
    {:ok, parsed} = :inet.parse_address(String.to_charlist(ip))
    parsed
  end
end
