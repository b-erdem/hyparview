# HyParView

[![Hex.pm](https://img.shields.io/hexpm/v/hyparview.svg)](https://hex.pm/packages/hyparview)
[![Documentation](https://img.shields.io/badge/docs-hexdocs-blue.svg)](https://hexdocs.pm/hyparview)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

A clean, BEAM-native Elixir implementation of the **HyParView** hybrid
partial-view membership protocol from Leitão, Pereira, and Rodrigues
([DSN 2007](https://www.dpss.inesc-id.pt/~ler/reports/dsn07-leitao.pdf)).

## Why this exists

If your Elixir cluster is approaching the point where libcluster's full-mesh
distribution starts to crack (≈50–100 nodes), HyParView is the membership
primitive that Riak Core and Partisan are built on.

Each node maintains:

- A small **active view** (typically `log(N) + 1` peers, e.g. 5 for ~30 nodes)
  that is symmetric, TCP-connected, and used for messaging.
- A larger **passive view** (typically `K · (log(N) + 1)`, default `K=6`) that
  is gossiped lazily and held in reserve to repair the active view on failure.

The result: bounded per-node connection cost, fast failure detection (TCP),
and high reliability under massive node churn — the paper demonstrates >90%
delivery with 95% of nodes failed.

## Installation

Add to `mix.exs`:

```elixir
def deps do
  [{:hyparview, "~> 0.1"}]
end
```

## Quick start

### Built-in test transport (in-process, ideal for tests)

```elixir
{:ok, contact_pid} = HyParView.start_link(
  peer: HyParView.Peer.new("contact", make_ref()),
  transport: HyParView.Transport.Test
)

{:ok, joiner_pid} = HyParView.start_link(
  peer: HyParView.Peer.new("joiner", make_ref()),
  contacts: [HyParView.Peer.new("contact", make_ref())],  # same address
  transport: HyParView.Transport.Test
)

HyParView.active_view(contact_pid)
# => [%HyParView.Peer{id: "joiner", ...}]
```

### Real TCP transport

```elixir
contact = HyParView.Peer.new("node-a", {{127, 0, 0, 1}, 4000})
joiner  = HyParView.Peer.new("node-b", {{127, 0, 0, 1}, 4001})

{:ok, _} = HyParView.start_link(
  peer: contact,
  transport: HyParView.Transport.TCP
)

{:ok, joiner_pid} = HyParView.start_link(
  peer: joiner,
  contacts: [contact],
  transport: HyParView.Transport.TCP
)

HyParView.subscribe(joiner_pid)
receive do
  {:hyparview, {:peer_up, peer}} -> IO.puts("up: #{peer.id}")
end
```

### Power-user mode: pure protocol core

For applications that want to drive the protocol from their own event loop
(no GenServer, no transport) — `HyParView.State` is a pure functional
state machine:

```elixir
state = HyParView.State.new(local_peer, HyParView.Config.new())
{state, actions} = HyParView.State.handle_message(state, message)
{state, actions} = HyParView.State.tick_shuffle(state)
{state, actions} = HyParView.State.connection_lost(state, peer)

# `actions` is a list of one or more:
#   {:notify_up, peer}
#   {:notify_down, peer}
#   {:send, peer, message}
```

This is the canonical core; `HyParView.Server` is a thin wrapper around it
with timers, transport, and subscriber notifications.

## Comparison

| | libcluster | Horde | HyParView |
|---|---|---|---|
| Concern | Discovery + `Node.connect` | Distributed registry/supervisor | Membership |
| Topology | Full mesh | Full mesh (uses libcluster) | **Partial mesh** (active view) |
| Scale ceiling | ~50–100 nodes | ~50–100 nodes | Hundreds to thousands |
| Failure detection | BEAM net_kernel | Inherited | TCP, fast |
| Use this when | You want a small cluster | You want a registry/supervisor | You want membership at scale |

A future companion package, `libcluster_hyparview`, will combine HyParView for
membership with `Node.connect` calls scoped to the active view — giving Elixir
users partial-mesh BEAM distribution.

## Architecture

```
                   ┌─────────────────────────────┐
                   │  HyParView.State (pure)     │
                   │  views, transitions, actions│
                   └──────────────┬──────────────┘
                                  │ %{peer, message}
                                  │ %{action, ...}
                   ┌──────────────┴──────────────┐
                   │  HyParView.Server (GenServer)│
                   │  timers, subscribers,        │
                   │  telemetry, transport plumb. │
                   └──────────────┬──────────────┘
                                  │
                   ┌──────────────┴──────────────┐
                   │  HyParView.Transport         │
                   │  (behaviour)                 │
                   └────┬───────────────┬─────────┘
                        │               │
              ┌─────────┴───┐  ┌────────┴─────────┐
              │ Transport.  │  │ Transport.TCP    │
              │ Test        │  │ + Connection      │
              │ (in-proc)   │  │ (gen_statem/peer) │
              └─────────────┘  └───────────────────┘
```

## Configuration

```elixir
HyParView.start_link(
  peer: %Peer{...},
  transport: HyParView.Transport.TCP,
  contacts: [%Peer{...}, ...],
  config: [
    active_view_size: 5,           # paper default
    passive_view_size: 30,         # paper default
    arwl: 6,                       # active random-walk length
    prwl: 3,                       # passive random-walk length
    shuffle_active_count: 3,       # ka — active samples per shuffle
    shuffle_passive_count: 4,      # kp — passive samples per shuffle
    shuffle_interval: 30_000,      # ms between shuffle ticks
    shuffle_ttl: 6                 # walk depth for SHUFFLE
  ]
)
```

## Telemetry

All view changes emit events under the configured prefix (default
`[:hyparview]`):

```elixir
:telemetry.attach_many(
  "hyparview-handler",
  Enum.map(HyParView.Telemetry.event_paths(), &([:hyparview | &1])),
  fn event, measurements, metadata, _ ->
    Logger.info("#{inspect(event)}: #{inspect(metadata)}")
  end,
  nil
)
```

See `HyParView.Telemetry` for the full event catalog.

## Custom transports

The `HyParView.Transport` behaviour has three callbacks. A minimal
custom transport (e.g., over Erlang distribution) is around 30 lines.
See `HyParView.Transport.Test` for the reference implementation.

## What this library is, and isn't

| Question | Answer |
|---|---|
| Membership? | Yes — JOIN, FORWARD_JOIN, NEIGHBOR, DISCONNECT, SHUFFLE. |
| Broadcast (Plumtree)? | **No.** A separate library — membership and broadcast are different concerns. |
| Distributed registry / supervisor? | **No.** That's [Horde](https://hex.pm/packages/horde). |
| Replacement for libcluster? | **Adjacent.** See [Comparison](#comparison) above. |
| TLS / auth? | Out of scope. Wrap the transport behaviour with your own. |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). All commits require DCO sign-off.

## License

Apache 2.0. See [LICENSE](LICENSE).

## Citation

> João Leitão, José Pereira, Luís Rodrigues. *HyParView: a membership protocol
> for reliable gossip-based broadcast.* In Proc. of the 37th Annual IEEE/IFIP
> International Conference on Dependable Systems and Networks (DSN '07),
> Edinburgh, UK, June 2007.
