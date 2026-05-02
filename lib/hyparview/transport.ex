defmodule HyParView.Transport do
  @moduledoc """
  Behaviour for the network transport that delivers HyParView messages
  between nodes.

  HyParView is transport-agnostic: the protocol logic lives in the pure
  `HyParView.State` machine, and `HyParView.Server` drives it. Transports
  plug in by implementing this behaviour.

  Implementations shipped with the library:

    * `HyParView.Transport.Test` — in-process, no network. Used by tests
      and integration suites.
    * `HyParView.Transport.TCP` — `gen_tcp`-based implementation suitable
      for real deployments. (Milestone 7.)

  Custom transports can wrap these to add TLS, alternate framing, or
  routing — there is intentionally no policy here.

  ## Lifecycle

    1. The `HyParView.Server` starts and calls `c:listen/2` to bind to
       its local address. The transport spawns whatever it needs to
       accept inbound connections.
    2. When the server needs to send a message to a peer, it calls
       `c:send_message/3`.
    3. When the transport receives an inbound message destined for the
       server, it delivers it via the callback registered at `listen/2`.

  Implementations are responsible for serialization (see
  `HyParView.Protocol`).
  """

  alias HyParView.{Messages, Peer}

  @typedoc """
  Opaque state held by the transport implementation. Returned from
  `c:listen/2` and threaded through all subsequent calls.
  """
  @type state :: term()

  @typedoc """
  A function the transport calls when an inbound message is received,
  framed as `{from_peer, message}`. The server receives this as a
  process message.
  """
  @type deliver :: (Peer.t(), Messages.t() -> :ok)

  @doc """
  Open a listening endpoint at `local_peer.address`. `deliver` is the
  callback the transport must invoke for each inbound message.

  Returns the transport's opaque state on success.
  """
  @callback listen(local_peer :: Peer.t(), deliver()) :: {:ok, state()} | {:error, term()}

  @doc """
  Send `message` to `target` peer over the transport.

  May open a new connection if needed. Errors are reported as
  `{:error, reason}` but message-level confirmation is *not* required —
  HyParView relies on TCP-style end-to-end delivery semantics within an
  established active-view connection, but the transport itself is not
  obligated to confirm individual messages.
  """
  @callback send_message(state(), target :: Peer.t(), message :: Messages.t()) ::
              :ok | {:error, term()}

  @doc """
  Close the transport, releasing any sockets/processes owned by it.
  """
  @callback close(state()) :: :ok
end
