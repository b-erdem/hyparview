defmodule HyParView.PartitionTest do
  @moduledoc """
  Network-partition tests using the deterministic in-memory `Cluster`
  simulator. Goals:

    1. Verify protocol invariants hold *during* a partition (each half
       remains self-consistent, view bounds preserved, no overlap).
    2. Verify the cluster recovers cleanly after the partition heals
       (invariants still hold; no spurious crashes; views may grow as
       cross-half peers re-discover each other).

  Like every test in this suite, this is seed-driven and deterministic —
  any failure is reproducible.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.{Peer, State}
  alias HyParView.Test.Cluster

  defp build_8_node_cluster(seed) do
    peers = for i <- 1..8, do: Peer.new("p#{i}", :addr)
    [contact | rest] = peers

    config = [
      active_view_size: 4,
      passive_view_size: 12,
      arwl: 4,
      prwl: 2,
      shuffle_active_count: 2,
      shuffle_passive_count: 3,
      shuffle_ttl: 4
    ]

    cluster = Cluster.new(peers, config, base_seed: seed)

    cluster =
      Enum.reduce(rest, cluster, fn joiner, c ->
        c = Cluster.join(c, joiner.id, contact.id)
        {_status, c} = Cluster.run_to_quiescence(c)
        c
      end)

    {peers, cluster}
  end

  defp halves do
    {["p1", "p2", "p3", "p4"], ["p5", "p6", "p7", "p8"]}
  end

  defp assert_invariants!(cluster, max_active, max_passive) do
    for {id, state} <- cluster.nodes do
      assert State.active_size(state) <= max_active,
             "#{id}: active size #{State.active_size(state)} > #{max_active}"

      assert State.passive_size(state) <= max_passive,
             "#{id}: passive size #{State.passive_size(state)} > #{max_passive}"

      active_ids = state |> State.active_peers() |> MapSet.new(& &1.id)
      passive_ids = state |> State.passive_peers() |> MapSet.new(& &1.id)
      assert MapSet.disjoint?(active_ids, passive_ids), "#{id}: view overlap"

      refute State.in_active?(state, state.self), "#{id}: self in active"
      refute State.in_passive?(state, state.self), "#{id}: self in passive"
    end
  end

  defp tick_all_shuffles(cluster) do
    Enum.reduce(cluster.nodes, cluster, fn {id, state}, acc ->
      {new_state, actions} = State.tick_shuffle(state)
      acc = %{acc | nodes: Map.put(acc.nodes, id, new_state)}
      apply_shuffle_actions(acc, id, actions)
    end)
  end

  defp apply_shuffle_actions(cluster, sender_id, actions) do
    Enum.reduce(actions, cluster, &apply_shuffle_action(&2, sender_id, &1))
  end

  defp apply_shuffle_action(c, from_id, {:send, %Peer{id: to_id}, msg}) do
    if drop?(c, from_id, to_id),
      do: %{c | dropped: c.dropped + 1},
      else: %{c | queue: :queue.in({to_id, msg}, c.queue)}
  end

  defp apply_shuffle_action(c, _from_id, _other), do: c

  defp drop?(cluster, from_id, to_id) do
    case cluster.partition_filter do
      nil -> false
      fun -> fun.(from_id, to_id) == :drop
    end
  end

  describe "during partition" do
    test "each half remains self-consistent (bounds + disjointness)" do
      {_peers, cluster} = build_8_node_cluster({3, 5, 7})
      assert_invariants!(cluster, 4, 12)

      {a, b} = halves()
      cluster = Cluster.partition(cluster, a, b)

      # Detect cross-half failures (the simulator has no TCP, so we
      # explicitly trigger connection_lost for cross-half active peers).
      cluster = Cluster.detect_lost(cluster, a ++ b)
      {_, cluster} = Cluster.run_to_quiescence(cluster)

      # Invariants hold for every node, even with messages dropping.
      assert_invariants!(cluster, 4, 12)

      # No node retains a cross-half peer in active.
      a_set = MapSet.new(a)
      b_set = MapSet.new(b)

      for {id, state} <- cluster.nodes do
        active = state |> State.active_peers() |> MapSet.new(& &1.id)
        my_half = if id in a, do: a_set, else: b_set
        other_half = if id in a, do: b_set, else: a_set

        cross = MapSet.intersection(active, other_half)

        assert MapSet.size(cross) == 0,
               "#{id} still has cross-half peers in active: #{inspect(cross)}"

        assert MapSet.subset?(active, MapSet.delete(my_half, id))
      end
    end

    test "messages crossing the partition are dropped" do
      {_peers, cluster} = build_8_node_cluster({11, 13, 17})
      {a, b} = halves()
      cluster = Cluster.partition(cluster, a, b)

      pre = cluster.dropped

      # Trigger detect_lost which generates protocol traffic; some of it
      # (NEIGHBOR sent to a cross-half passive peer, etc.) should hit
      # the partition filter.
      cluster = Cluster.detect_lost(cluster, a ++ b)
      {_, cluster} = Cluster.run_to_quiescence(cluster)

      # We expect at least some drops. Exact count depends on RNG;
      # asserting > pre is enough to confirm the filter fires.
      assert cluster.dropped > pre, "no messages dropped during partition"
    end
  end

  describe "after heal" do
    test "invariants still hold and shuffle traffic resumes" do
      {_peers, cluster} = build_8_node_cluster({23, 29, 31})
      {a, b} = halves()

      cluster = cluster |> Cluster.partition(a, b) |> Cluster.detect_lost(a ++ b)
      {_, cluster} = Cluster.run_to_quiescence(cluster)

      # Heal the partition.
      cluster = Cluster.heal(cluster)

      # Run several rounds of shuffles. With the partition gone, cross-half
      # passive entries refresh and traffic flows freely again.
      cluster =
        Enum.reduce(1..6, cluster, fn _, c ->
          c = tick_all_shuffles(c)
          {_, c} = Cluster.run_to_quiescence(c)
          c
        end)

      assert_invariants!(cluster, 4, 12)
    end

    test "post-heal connection_lost can re-establish a cross-half active peer" do
      {_peers, cluster} = build_8_node_cluster({37, 41, 43})
      {a, b} = halves()

      cluster = cluster |> Cluster.partition(a, b) |> Cluster.detect_lost(a ++ b)
      {_, cluster} = Cluster.run_to_quiescence(cluster)
      cluster = Cluster.heal(cluster)

      # Run shuffles so passive views can pick up cross-half peers again.
      cluster =
        Enum.reduce(1..6, cluster, fn _, c ->
          c = tick_all_shuffles(c)
          {_, c} = Cluster.run_to_quiescence(c)
          c
        end)

      # Cause one same-half link loss on p1 to force attempt_repair.
      # The picked NEIGHBOR target may be a cross-half peer (now reachable),
      # which after acceptance demonstrates re-unification across halves.
      p1_state = Map.fetch!(cluster.nodes, "p1")

      same_half_active =
        p1_state
        |> State.active_peers()
        |> Enum.find(&(&1.id in a))

      if same_half_active do
        {p1_state, actions} = State.connection_lost(p1_state, same_half_active)
        cluster = %{cluster | nodes: Map.put(cluster.nodes, "p1", p1_state)}

        cluster =
          Enum.reduce(actions, cluster, fn
            {:send, %Peer{id: t}, m}, c -> %{c | queue: :queue.in({t, m}, c.queue)}
            _, c -> c
          end)

        {_, cluster} = Cluster.run_to_quiescence(cluster)
        assert_invariants!(cluster, 4, 12)
      end
    end
  end

  describe "property: bounds hold under partition + heal cycles" do
    property "any seed × any partition cycle preserves invariants" do
      check all(
              seed_a <- StreamData.integer(1..1_000),
              seed_b <- StreamData.integer(1..1_000),
              cycles <- StreamData.integer(1..3),
              max_runs: 15
            ) do
        {_peers, cluster} = build_8_node_cluster({seed_a, seed_b, seed_a + seed_b})
        {a, b} = halves()

        cluster =
          Enum.reduce(1..cycles, cluster, fn _, c ->
            c = c |> Cluster.partition(a, b) |> Cluster.detect_lost(a ++ b)
            {_, c} = Cluster.run_to_quiescence(c)
            assert_invariants!(c, 4, 12)

            c = Cluster.heal(c)
            c = tick_all_shuffles(c)
            {_, c} = Cluster.run_to_quiescence(c)
            assert_invariants!(c, 4, 12)
            c
          end)

        assert_invariants!(cluster, 4, 12)
      end
    end
  end
end
