# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- `HyParView.subscribe/3` accepts a `:replay` option. When `replay: true`,
  the server immediately sends `{:hyparview, {:peer_up, peer}}` for every
  peer currently in the active view, before any future events. Useful for
  late subscribers (e.g. integrations like `libcluster_hyparview` that
  subscribe after the server has already JOINed).

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
