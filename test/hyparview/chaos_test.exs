defmodule HyParView.ChaosTest do
  @moduledoc """
  Chaos and stress tests using the in-memory `Cluster` simulator.

  Goals:

    * Ensure protocol invariants survive random failure injection.
    * Ensure determinism: identical seeds produce identical outcomes.
    * Stress the state machine with many operations and verify bounds.

  These tests are deterministic (seed-driven) so failures are reproducible.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.{Peer, State}
  alias HyParView.Test.Cluster

  defp build_cluster(n, config_opts, seed) do
    peers = for i <- 1..n, do: Peer.new("p#{i}", :addr)
    [contact | rest] = peers
    cluster = Cluster.new(peers, config_opts, base_seed: seed)

    cluster =
      Enum.reduce(rest, cluster, fn joiner, c ->
        c = Cluster.join(c, joiner.id, contact.id)
        {_status, c} = Cluster.run_to_quiescence(c)
        c
      end)

    {peers, cluster}
  end

  defp invariants_hold!(cluster, max_active, max_passive) do
    for {_id, state} <- cluster.nodes do
      assert State.active_size(state) <= max_active,
             "active size #{State.active_size(state)} > #{max_active}"

      assert State.passive_size(state) <= max_passive,
             "passive size #{State.passive_size(state)} > #{max_passive}"

      active_ids = state |> State.active_peers() |> MapSet.new(& &1.id)
      passive_ids = state |> State.passive_peers() |> MapSet.new(& &1.id)
      assert MapSet.disjoint?(active_ids, passive_ids), "view overlap detected"

      refute State.in_active?(state, state.self), "self in active view"
      refute State.in_passive?(state, state.self), "self in passive view"
    end
  end

  defp inject_connection_loss(cluster, [_ | _] = active_ids, victim_picker) do
    Enum.reduce(active_ids, cluster, fn node_id, acc ->
      drop_one_active(acc, node_id, victim_picker)
    end)
  end

  defp drop_one_active(cluster, node_id, victim_picker) do
    state = Map.fetch!(cluster.nodes, node_id)

    case State.active_peers(state) do
      [] -> cluster
      peers -> apply_loss(cluster, node_id, state, victim_picker.(peers))
    end
  end

  defp apply_loss(cluster, node_id, state, target) do
    {state, actions} = State.connection_lost(state, target)
    cluster = %{cluster | nodes: Map.put(cluster.nodes, node_id, state)}
    enqueue_sends(cluster, actions)
  end

  defp enqueue_sends(cluster, actions) do
    Enum.reduce(actions, cluster, fn
      {:send, %Peer{id: id}, msg}, c -> %{c | queue: :queue.in({id, msg}, c.queue)}
      _, c -> c
    end)
  end

  describe "stress: many random join sequences" do
    property "view bounds hold for clusters of 5-12 nodes under any seed" do
      check all(
              n <- StreamData.integer(5..12),
              seed_a <- StreamData.integer(1..10_000),
              seed_b <- StreamData.integer(1..10_000),
              seed_c <- StreamData.integer(1..10_000),
              max_runs: 25
            ) do
        config = [
          active_view_size: 4,
          passive_view_size: 12,
          arwl: 4,
          prwl: 2,
          shuffle_active_count: 2,
          shuffle_passive_count: 3,
          shuffle_ttl: 4
        ]

        {_peers, cluster} = build_cluster(n, config, {seed_a, seed_b, seed_c})
        invariants_hold!(cluster, 4, 12)
      end
    end
  end

  describe "chaos: random connection failures" do
    test "10-node cluster survives random failure injection over multiple rounds" do
      config = [
        active_view_size: 4,
        passive_view_size: 12,
        arwl: 4,
        prwl: 2,
        shuffle_active_count: 2,
        shuffle_passive_count: 3,
        shuffle_ttl: 4
      ]

      {_peers, cluster} = build_cluster(10, config, {7, 11, 13})
      invariants_hold!(cluster, 4, 12)

      # Run 10 chaos rounds: each round, every node randomly drops one
      # active peer (simulating a failed link).
      rng_state = :rand.seed_s(:default, {99, 42, 17})

      {final_cluster, _rng} =
        Enum.reduce(1..10, {cluster, rng_state}, fn _, {c, r} ->
          all_ids = Map.keys(c.nodes)

          # Pick random victims with the seeded RNG (separate from cluster RNG).
          {picker, r2} = make_picker(r)

          c = inject_connection_loss(c, all_ids, picker)
          {_status, c} = Cluster.run_to_quiescence(c, 10_000)
          invariants_hold!(c, 4, 12)
          {c, r2}
        end)

      invariants_hold!(final_cluster, 4, 12)
    end

    defp make_picker(rng) do
      {seed, rng2} = :rand.uniform_s(1_000_000, rng)

      picker = fn peers ->
        # Deterministic-ish: hash by seed
        Enum.at(peers, rem(seed, length(peers)))
      end

      {picker, rng2}
    end
  end

  describe "determinism" do
    property "identical seeds + identical join sequence produce identical clusters" do
      check all(
              n <- StreamData.integer(4..8),
              seed_a <- StreamData.integer(1..1000),
              seed_b <- StreamData.integer(1..1000)
            ) do
        seed = {seed_a, seed_b, seed_a * seed_b}

        config = [
          active_view_size: 3,
          passive_view_size: 8,
          arwl: 3,
          prwl: 2
        ]

        {_, c1} = build_cluster(n, config, seed)
        {_, c2} = build_cluster(n, config, seed)

        for id <- Map.keys(c1.nodes) do
          s1 = Map.fetch!(c1.nodes, id)
          s2 = Map.fetch!(c2.nodes, id)

          assert s1.active == s2.active, "non-determinism in active view of #{id}"
          assert s1.passive == s2.passive, "non-determinism in passive view of #{id}"
        end
      end
    end
  end

  describe "edge cases" do
    test "cluster of size 2 (just joiner + contact) maintains invariants" do
      {_, cluster} = build_cluster(2, [active_view_size: 4, passive_view_size: 4], {1, 2, 3})
      invariants_hold!(cluster, 4, 4)
    end

    test "cluster with active_view_size=1 still functions" do
      {_, cluster} = build_cluster(5, [active_view_size: 1, passive_view_size: 8], {1, 2, 3})
      invariants_hold!(cluster, 1, 8)

      # Every node has exactly 0 or 1 active peer.
      for {_, state} <- cluster.nodes do
        assert State.active_size(state) in [0, 1]
      end
    end

    test "cluster with passive_view_size=0 still functions" do
      {_, cluster} = build_cluster(5, [active_view_size: 3, passive_view_size: 0], {1, 2, 3})
      invariants_hold!(cluster, 3, 0)

      for {_, state} <- cluster.nodes do
        assert State.passive_size(state) == 0
      end
    end
  end
end
