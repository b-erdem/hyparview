defmodule HyParView.StateTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.{Config, Peer, State}
  alias HyParView.Test.Generators

  doctest State

  # ── Test helpers ──────────────────────────────────────────────────────

  defp self_peer, do: Peer.new("self", :self_addr)

  defp fresh_state(opts \\ []) do
    config_opts = Keyword.get(opts, :config, [])
    state_opts = Keyword.get(opts, :state, rng_seed: {1, 2, 3})
    State.new(self_peer(), Config.new(config_opts), state_opts)
  end

  defp fill_active(state, peers) do
    Enum.reduce(peers, state, fn peer, s ->
      {s, _} = State.add_to_active(s, peer)
      s
    end)
  end

  # Generate a peer that is never `self` (so tests don't need to filter)
  defp other_peer do
    Generators.peer()
    |> StreamData.filter(fn %Peer{id: id} -> id != "self" end)
  end

  defp distinct_others(opts) do
    other_peer()
    |> StreamData.list_of(opts)
    |> StreamData.map(&Enum.uniq_by(&1, fn %Peer{id: id} -> id end))
  end

  # ── Construction ──────────────────────────────────────────────────────

  describe "new/3" do
    test "starts with empty views" do
      state = fresh_state()
      assert State.active_size(state) == 0
      assert State.passive_size(state) == 0
      assert State.active_peers(state) == []
      assert State.passive_peers(state) == []
    end

    test "self?/2 is true only for the local peer" do
      state = fresh_state()
      assert State.self?(state, self_peer())
      refute State.self?(state, Peer.new("other", :addr))
    end
  end

  # ── add_to_active ─────────────────────────────────────────────────────

  describe "add_to_active/2" do
    test "adds a peer below capacity, emits notify_up" do
      state = fresh_state()
      peer = Peer.new("p1", :addr)

      assert {state, [{:notify_up, ^peer}]} = State.add_to_active(state, peer)
      assert State.in_active?(state, peer)
      assert State.active_size(state) == 1
    end

    test "is a no-op for self" do
      state = fresh_state()
      assert {^state, []} = State.add_to_active(state, self_peer())
    end

    test "is a no-op when already in active view" do
      state = fresh_state()
      peer = Peer.new("p1", :addr)
      {state, _} = State.add_to_active(state, peer)
      assert {^state, []} = State.add_to_active(state, peer)
    end

    test "moves a passive peer up to active (no overlap invariant)" do
      state = fresh_state()
      peer = Peer.new("p1", :addr)
      {state, _} = State.add_to_passive(state, peer)
      assert State.in_passive?(state, peer)

      {state, [{:notify_up, ^peer}]} = State.add_to_active(state, peer)

      assert State.in_active?(state, peer)
      refute State.in_passive?(state, peer)
    end

    test "evicts a random active peer when full, demoting it to passive" do
      state = fresh_state(config: [active_view_size: 3])
      ps = for i <- 1..3, do: Peer.new("p#{i}", :addr)
      state = fill_active(state, ps)
      assert State.active_size(state) == 3

      extra = Peer.new("extra", :addr)
      {state, actions} = State.add_to_active(state, extra)

      notify_downs = for {:notify_down, p} <- actions, do: p
      notify_ups = for {:notify_up, p} <- actions, do: p

      assert [evicted] = notify_downs
      assert [^extra] = notify_ups

      assert State.in_active?(state, extra)
      refute State.in_active?(state, evicted)
      assert State.in_passive?(state, evicted)
      assert State.active_size(state) == 3
    end
  end

  # ── add_to_passive ────────────────────────────────────────────────────

  describe "add_to_passive/2" do
    test "is a no-op for self" do
      state = fresh_state()
      assert {^state, []} = State.add_to_passive(state, self_peer())
    end

    test "is a no-op when already in active view" do
      state = fresh_state()
      peer = Peer.new("p1", :addr)
      {state, _} = State.add_to_active(state, peer)
      assert {^state, []} = State.add_to_passive(state, peer)
      assert State.in_active?(state, peer)
      refute State.in_passive?(state, peer)
    end

    test "drops a random passive peer silently when full" do
      state = fresh_state(config: [passive_view_size: 2])
      a = Peer.new("a", :addr)
      b = Peer.new("b", :addr)
      c = Peer.new("c", :addr)
      {state, _} = State.add_to_passive(state, a)
      {state, _} = State.add_to_passive(state, b)
      assert State.passive_size(state) == 2

      {state, []} = State.add_to_passive(state, c)
      assert State.passive_size(state) == 2
      assert State.in_passive?(state, c)
    end
  end

  # ── remove ────────────────────────────────────────────────────────────

  describe "remove_from_active/2" do
    test "emits notify_down when present" do
      state = fresh_state()
      peer = Peer.new("p1", :addr)
      {state, _} = State.add_to_active(state, peer)

      {state, [{:notify_down, ^peer}]} = State.remove_from_active(state, peer)
      refute State.in_active?(state, peer)
    end

    test "is a no-op when peer not present" do
      state = fresh_state()
      assert {^state, []} = State.remove_from_active(state, Peer.new("ghost", :addr))
    end
  end

  # ── Properties (the seven invariants) ─────────────────────────────────

  @max_active 5
  @max_passive 30

  defp run_op_sequence(state, ops) do
    Enum.reduce(ops, state, fn
      {:add_active, p}, s -> elem(State.add_to_active(s, p), 0)
      {:add_passive, p}, s -> elem(State.add_to_passive(s, p), 0)
      {:remove_active, p}, s -> elem(State.remove_from_active(s, p), 0)
      {:remove_passive, p}, s -> elem(State.remove_from_passive(s, p), 0)
    end)
  end

  defp op_gen(peers) do
    StreamData.bind(StreamData.member_of(peers), fn p ->
      StreamData.bind(
        StreamData.member_of([:add_active, :add_passive, :remove_active, :remove_passive]),
        fn op -> StreamData.constant({op, p}) end
      )
    end)
  end

  property "[1] active view size never exceeds active_view_size" do
    check all(
            peers <- distinct_others(min_length: 1, max_length: 30),
            ops <- StreamData.list_of(op_gen(peers), max_length: 100)
          ) do
      state =
        fresh_state(config: [active_view_size: @max_active, passive_view_size: @max_passive])

      state = run_op_sequence(state, ops)
      assert State.active_size(state) <= @max_active
    end
  end

  property "[2] passive view size never exceeds passive_view_size" do
    check all(
            peers <- distinct_others(min_length: 1, max_length: 30),
            ops <- StreamData.list_of(op_gen(peers), max_length: 100)
          ) do
      state =
        fresh_state(config: [active_view_size: @max_active, passive_view_size: @max_passive])

      state = run_op_sequence(state, ops)
      assert State.passive_size(state) <= @max_passive
    end
  end

  property "[3] active and passive views are always disjoint" do
    check all(
            peers <- distinct_others(min_length: 1, max_length: 30),
            ops <- StreamData.list_of(op_gen(peers), max_length: 100)
          ) do
      state = fresh_state()
      state = run_op_sequence(state, ops)

      active_ids = state |> State.active_peers() |> Enum.map(& &1.id) |> MapSet.new()
      passive_ids = state |> State.passive_peers() |> Enum.map(& &1.id) |> MapSet.new()

      assert MapSet.disjoint?(active_ids, passive_ids)
    end
  end

  property "[4] self is never in any view" do
    check all(
            peers <- distinct_others(min_length: 1, max_length: 30),
            ops <- StreamData.list_of(op_gen(peers), max_length: 100)
          ) do
      state = fresh_state()
      state = run_op_sequence(state, ops)
      refute State.in_active?(state, self_peer())
      refute State.in_passive?(state, self_peer())
    end
  end

  property "[5] eviction from full active view always demotes to passive" do
    check all(peers <- distinct_others(min_length: 6, max_length: 20)) do
      state = fresh_state(config: [active_view_size: 3, passive_view_size: 30])
      [extra | first_three] = Enum.take(peers, 4)
      state = fill_active(state, first_three)
      assert State.active_size(state) == 3

      {new_state, actions} = State.add_to_active(state, extra)
      notify_downs = for {:notify_down, p} <- actions, do: p

      # We just evicted into a passive view that was empty, so the demoted
      # peer must be present in passive.
      assert [evicted] = notify_downs
      assert State.in_passive?(new_state, evicted)
    end
  end

  property "[6] same RNG seed produces identical state under identical operations" do
    check all(
            peers <- distinct_others(min_length: 5, max_length: 30),
            ops <- StreamData.list_of(op_gen(peers), max_length: 50)
          ) do
      a =
        fresh_state(
          config: [active_view_size: 3, passive_view_size: 5],
          state: [rng_seed: {42, 43, 44}]
        )

      b =
        fresh_state(
          config: [active_view_size: 3, passive_view_size: 5],
          state: [rng_seed: {42, 43, 44}]
        )

      a_final = run_op_sequence(a, ops)
      b_final = run_op_sequence(b, ops)

      assert a_final.active == b_final.active
      assert a_final.passive == b_final.passive
    end
  end

  property "[7] adding then removing a peer from active leaves it absent" do
    check all(peer <- other_peer()) do
      state = fresh_state()
      {state, _} = State.add_to_active(state, peer)
      {state, _} = State.remove_from_active(state, peer)
      refute State.in_active?(state, peer)
    end
  end
end
