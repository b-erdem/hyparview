# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Race: late `NEIGHBOR_REPLY` re-adding a dead peer to the active
  view** (#1). When `connection_lost/2` evicted a peer between a
  NEIGHBOR being sent and its reply arriving, the
  `handle_neighbor_reply/2` accepted-branch added the now-dead peer
  back to the active view unconditionally, leaving every subsequent
  send to that peer broken until the failure detector fired again.

  `State` now keeps a small `recently_lost` deny-list of peer ids
  evicted via `connection_lost/2`. The reply handler rejects only
  replies whose sender is in that set; everything else is accepted
  exactly as before, so the protocol's other paths (FORWARD_JOIN
  TTL=0 add-active-with-handshake, NEIGHBOR-after-eviction
  re-symmetrisation) still re-sync views via reply traffic. The
  set is cleared on each `tick_shuffle/1` — well past any plausible
  in-flight-reply window — and individual entries are cleared
  whenever the same peer is later re-added to active legitimately.

  Found and reproduced by [Lockstep](https://hex.pm/packages/lockstep)
  POS-strategy schedule exploration (iteration 1 / seed 1). The
  initial fix used an allow-list (`repair_target`) that was too
  strict and silently dropped legitimate replies in unrelated
  multi-cycle scenarios; switched to a deny-list (`recently_lost`)
  that targets only the H1 race.

### Changed (breaking)

- `HyParView.Transport` callback signature: `listen/2` now takes a 1-arity
  `event_callback` instead of a 2-arity `deliver` callback. Events are
  `{:message, peer, msg}` and `{:peer_lost, peer}`.

### Added

- `HyParView.Transport.TCP` now automatically signals `{:peer_lost, peer}`
  when a TCP connection closes. The Server translates this into
  `HyParView.State.connection_lost/2`, so reactive recovery (NEIGHBOR to a
  passive peer) fires without applications having to call
  `HyParView.connection_lost/2` themselves.
- `TCP_NODELAY` (Nagle's algorithm disabled) is now set on every TCP
  socket the Transport opens — listening, outbound `:gen_tcp.connect`,
  and (explicitly, since Erlang doesn't propagate TCP-level options
  from a listening socket to accepted sockets) on each accepted
  socket via `Connection.start_receiving/1`. HyParView's protocol
  messages are all small (JOIN, FORWARD_JOIN, NEIGHBOR, SHUFFLE);
  without `nodelay`, every frame can wait up to 40ms for Nagle to
  coalesce, dominating handshake and tail latency under multi-peer
  load. This finding came out of profiling the
  `hyparview_pubsub_bench` companion project.
- `HyParView.subscribe/3` accepts a `:replay` option. When `replay: true`,
  the server immediately sends `{:hyparview, {:peer_up, peer}}` for every
  peer currently in the active view, before any future events. Useful for
  late subscribers (e.g. integrations like `libcluster_hyparview` that
  subscribe after the server has already JOINed).
- `HyParView.Telemetry.Metrics` — pre-built `Telemetry.Metrics`
  definitions for the events emitted by `HyParView.Server`. Drop them
  into a `telemetry_metrics_prometheus` / `telemetry_metrics_statsd` /
  any other reporter for instant observability. Optional dep on
  `:telemetry_metrics ~> 1.0`.

### Tested

- `HyParView.PartitionTest` — partition + heal tests (5 cases including
  one StreamData property): bounds and disjointness invariants hold under
  partition; the partition filter actually drops cross-half messages;
  shuffle traffic resumes after heal; partition-then-heal cycles preserve
  invariants across all seeds.
- `HyParView.Test.Cluster` simulator extended with `partition/3`,
  `heal/1`, and `detect_lost/2` for partition-style integration tests.
- `HyParView.ConnectionTest` — targeted error-path coverage for
  `Connection`'s `decode_hello/1` (rejects empty/garbage frames,
  unknown Hello version, term that isn't a `%Peer{}`, unsafe terms),
  `outbound_connecting`'s connect-failure handling, and `parse_ip/1`
  for the binary-IP form (covering both `Connection` and
  `Transport.TCP` listen paths).
- `mix.exs` adds `test_coverage: [summary: [threshold: 85],
  ignore_modules: [~r/^HyParView\.Test\..*$/]]` so test-support
  helpers don't dilute the coverage percentage. Library-only
  coverage is now **88.86%** (up from 86.02%).

## [0.1.0] — 2026-05-02

Initial release.

### Added

- `HyParView.Peer` — opaque peer identity (`id` + `address`).
- `HyParView.Messages` — seven protocol message structs from DSN 2007 paper.
- `HyParView.Protocol` — versioned wire format `<<HPIV::32, version::8, payload>>`
  with `:safe`-decoded `:erlang.term_to_binary/2` payloads.
- `HyParView.Config` — paper-default configuration with validation.
- `HyParView.State` — pure functional protocol core. All seven message
  handlers (`handle_message/2`), shuffle tick (`tick_shuffle/1`),
  connection-lost event (`connection_lost/2`), JOIN initiation
  (`initiate_join/2`).
- `HyParView.Transport` — pluggable transport behaviour.
- `HyParView.Transport.Test` — in-process transport using a `Registry`.
- `HyParView.Transport.TCP` — `:gen_tcp`-backed transport with per-peer
  `:gen_statem` connection processes (`HyParView.Connection`).
- `HyParView.Server` — `GenServer` wrapping `State` with timers, transport,
  subscribers, telemetry.
- `HyParView` — public API delegating to `HyParView.Server`.
- `HyParView.Telemetry` — telemetry event catalog and prefix helpers.

### Tested

- 7 doctests, 14 properties, 93 tests covering:
    - 7 numbered view invariants (`StateTest`)
    - Wire-format round-trip (`ProtocolTest`)
    - JOIN + dual-TTL FORWARD_JOIN (`JoinTest`)
    - DISCONNECT + NEIGHBOR + reactive recovery (`RecoveryTest`)
    - SHUFFLE + SHUFFLE_REPLY (`ShuffleTest`)
    - Active-view symmetry property (`SymmetryTest`)
    - Chaos + stress + determinism properties (`ChaosTest`)
    - Multi-process integration via `Transport.Test` (`IntegrationTest`)
    - Real-network integration via `Transport.TCP` (`TCPTest`)
    - Edge cases: lone node, concurrent joins, larger cluster, subscriber
      down events, server restart (`ComprehensiveTest`)
    - Telemetry events under default and custom prefixes (`TelemetryTest`)

[Unreleased]: https://github.com/b-erdem/hyparview/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/b-erdem/hyparview/releases/tag/v0.1.0
