defmodule HyParView do
  @moduledoc """
  HyParView — a hybrid partial-view membership protocol for reliable
  gossip-based broadcast.

  This module is the public entry point. It exposes thin wrappers around
  `HyParView.Server`, the GenServer that owns membership state and timers.
  Power users who want to drive the protocol from their own event loop can
  use `HyParView.State` directly — it is a pure functional core with no
  processes, no IO, and no time.

  ## Quick start

      {:ok, _pid} = HyParView.start_link(
        peer: HyParView.Peer.new("node-a", {"127.0.0.1", 4000}),
        contacts: [HyParView.Peer.new("node-b", {"127.0.0.1", 4001})],
        transport: HyParView.Transport.Test,
        config: [active_view_size: 5, passive_view_size: 30]
      )

      HyParView.active_view(pid)        # => [%HyParView.Peer{}, ...]
      HyParView.subscribe(pid)
      receive do
        {:hyparview, {:peer_up, peer}} -> IO.inspect(peer)
      end

  ## References

    * [HyParView paper (DSN 2007)](https://www.dpss.inesc-id.pt/~ler/reports/dsn07-leitao.pdf)
    * [`HyParView.State`](`HyParView.State`) — pure protocol core
    * [`HyParView.Server`](`HyParView.Server`) — GenServer wrapping the core
    * [`HyParView.Transport`](`HyParView.Transport`) — pluggable transport behaviour
  """

  alias HyParView.{Peer, Server}

  @doc "Delegates to `HyParView.Server.start_link/1`."
  @spec start_link([Server.start_option()]) :: GenServer.on_start()
  defdelegate start_link(opts), to: Server

  @doc "Return the current active view."
  @spec active_view(GenServer.server()) :: [Peer.t()]
  defdelegate active_view(server), to: Server

  @doc "Return the current passive view."
  @spec passive_view(GenServer.server()) :: [Peer.t()]
  defdelegate passive_view(server), to: Server

  @doc "Subscribe to view-change notifications. See `HyParView.Server.subscribe/2`."
  @spec subscribe(GenServer.server(), pid()) :: :ok
  defdelegate subscribe(server, pid \\ self()), to: Server

  @doc "Stop the server gracefully."
  @spec stop(GenServer.server()) :: :ok
  defdelegate stop(server), to: Server

  @doc "Signal that a connection to `peer` has been lost."
  @spec connection_lost(GenServer.server(), Peer.t()) :: :ok
  defdelegate connection_lost(server, peer), to: Server
end
