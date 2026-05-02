defmodule HyParView.ShuffleTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.{Config, Peer, State}
  alias HyParView.Messages.{Shuffle, ShuffleReply}
  alias HyParView.Test.Cluster

  defp self_peer, do: Peer.new("self", :self_addr)

  defp fresh_state(opts \\ []) do
    config_opts = Keyword.get(opts, :config, [])
    state_opts = Keyword.get(opts, :state, rng_seed: {1, 2, 3})
    State.new(self_peer(), Config.new(config_opts), state_opts)
  end

  defp fill_active(state, peers) do
    Enum.reduce(peers, state, fn p, s ->
      {s, _} = State.add_to_active(s, p)
      s
    end)
  end

  defp fill_passive(state, peers) do
    Enum.reduce(peers, state, fn p, s ->
      {s, _} = State.add_to_passive(s, p)
      s
    end)
  end

  # ── tick_shuffle ──────────────────────────────────────────────────────

  describe "tick_shuffle/1" do
    test "is a no-op when active view is empty" do
      state = fresh_state()
      assert {^state, []} = State.tick_shuffle(state)
    end

    test "sends a Shuffle to a random active peer with self as origin" do
      state = fresh_state(config: [active_view_size: 5, shuffle_ttl: 6])
      neighbors = for i <- 1..3, do: Peer.new("n#{i}", :addr)
      state = fill_active(state, neighbors)

      {_state, actions} = State.tick_shuffle(state)

      shuffles = for {:send, target, %Shuffle{} = m} <- actions, do: {target, m}
      assert [{target, msg}] = shuffles
      assert target in neighbors
      assert msg.origin == self_peer()
      assert msg.sender == self_peer()
      assert msg.ttl == 6
      assert self_peer() in msg.sample
    end

    test "sample includes up to ka active and kp passive peers" do
      state =
        fresh_state(
          config: [
            active_view_size: 10,
            passive_view_size: 30,
            shuffle_active_count: 2,
            shuffle_passive_count: 3
          ]
        )

      active = for i <- 1..5, do: Peer.new("a#{i}", :addr)
      passive = for i <- 1..6, do: Peer.new("p#{i}", :addr)
      state = state |> fill_active(active) |> fill_passive(passive)

      {_state, [{:send, _, %Shuffle{sample: sample}}]} = State.tick_shuffle(state)

      # sample size: 1 (self) + 2 (active) + 3 (passive) = 6
      assert length(sample) == 6
      assert self_peer() in sample
    end
  end

  # ── handle_shuffle ────────────────────────────────────────────────────

  describe "handle_message/2 — Shuffle" do
    test "TTL expires (=1 → 0): merges sample, sends ShuffleReply to origin" do
      state = fresh_state(config: [active_view_size: 5, passive_view_size: 30])
      sender = Peer.new("sender", :addr)
      state = fill_active(state, [sender])

      origin = Peer.new("origin", :addr)
      sample = [origin, Peer.new("s1", :addr), Peer.new("s2", :addr)]

      msg = %Shuffle{origin: origin, sample: sample, ttl: 1, sender: sender}
      {state, actions} = State.handle_message(state, msg)

      # Sample (excluding self if present) is merged
      assert State.in_passive?(state, origin)

      replies = for {:send, ^origin, %ShuffleReply{sample: rs}} <- actions, do: rs
      assert [_reply_sample] = replies
    end

    test "active size <= 1: terminates and replies even with TTL > 0" do
      state = fresh_state(config: [active_view_size: 5, passive_view_size: 30])
      sender = Peer.new("sender", :addr)
      state = fill_active(state, [sender])

      origin = Peer.new("origin", :addr)
      msg = %Shuffle{origin: origin, sample: [origin], ttl: 5, sender: sender}
      {_state, actions} = State.handle_message(state, msg)

      replies = for {:send, ^origin, %ShuffleReply{}} <- actions, do: :ok
      assert [_] = replies
    end

    test "TTL > 0 with multiple active: forwards to a peer != sender, sender becomes self" do
      state = fresh_state(config: [active_view_size: 5])
      sender = Peer.new("sender", :addr)
      others = for i <- 1..3, do: Peer.new("o#{i}", :addr)
      state = fill_active(state, [sender | others])

      origin = Peer.new("origin", :addr)
      msg = %Shuffle{origin: origin, sample: [origin], ttl: 4, sender: sender}
      {_state, actions} = State.handle_message(state, msg)

      forwards =
        for {:send, %Peer{id: id}, %Shuffle{ttl: ttl, sender: s, origin: o}} <- actions,
            do: {id, ttl, s, o}

      assert [{forwarded_id, 3, _new_sender, ^origin}] = forwards
      assert forwarded_id in ["o1", "o2", "o3"]
    end
  end

  # ── handle_shuffle_reply ──────────────────────────────────────────────

  describe "handle_message/2 — ShuffleReply" do
    test "merges sample into passive, no actions" do
      state = fresh_state(config: [passive_view_size: 30])
      new = Peer.new("new", :addr)
      msg = %ShuffleReply{sample: [new]}

      {state, actions} = State.handle_message(state, msg)

      assert State.in_passive?(state, new)
      assert actions == []
    end

    test "respects view bounds and overlap invariants" do
      state = fresh_state(config: [passive_view_size: 30])
      already_active = Peer.new("a", :addr)
      state = fill_active(state, [already_active])

      msg = %ShuffleReply{sample: [self_peer(), already_active, Peer.new("new", :addr)]}
      {state, _} = State.handle_message(state, msg)

      # self never in passive; already-active not in passive; new is in passive
      refute State.in_passive?(state, self_peer())
      refute State.in_passive?(state, already_active)
      assert State.in_passive?(state, Peer.new("new", :addr))
    end
  end

  # ── Integration: shuffle in a cluster ────────────────────────────────

  describe "cluster behaviour: shuffle preserves bounds" do
    property "after T shuffle ticks per node, view sizes remain bounded" do
      check all(
              n <- StreamData.integer(4..8),
              ticks <- StreamData.integer(1..3)
            ) do
        peers = for i <- 1..n, do: Peer.new("p#{i}", :addr)
        [contact | rest] = peers

        cluster =
          Cluster.new(
            peers,
            [
              active_view_size: 4,
              passive_view_size: 10,
              arwl: 4,
              prwl: 2,
              shuffle_active_count: 2,
              shuffle_passive_count: 3,
              shuffle_ttl: 4
            ],
            base_seed: {n, ticks * 7, 13}
          )

        # Bring the cluster up via sequential JOINs.
        cluster =
          Enum.reduce(rest, cluster, fn joiner, c ->
            c = Cluster.join(c, joiner.id, contact.id)
            {_status, c} = Cluster.run_to_quiescence(c)
            c
          end)

        # Run T shuffle ticks per node, draining the queue between ticks.
        cluster =
          Enum.reduce(1..ticks, cluster, fn _, c ->
            c =
              Enum.reduce(c.nodes, c, fn {id, _}, acc ->
                state = Map.fetch!(acc.nodes, id)
                {new_state, actions} = State.tick_shuffle(state)
                acc = %{acc | nodes: Map.put(acc.nodes, id, new_state)}

                Enum.reduce(actions, acc, fn
                  {:send, %Peer{id: target}, msg}, c2 ->
                    %{c2 | queue: :queue.in({target, msg}, c2.queue)}

                  _, c2 ->
                    c2
                end)
              end)

            {_status, c} = Cluster.run_to_quiescence(c)
            c
          end)

        for {_id, state} <- cluster.nodes do
          assert State.active_size(state) <= 4
          assert State.passive_size(state) <= 10

          active_ids = state |> State.active_peers() |> MapSet.new(& &1.id)
          passive_ids = state |> State.passive_peers() |> MapSet.new(& &1.id)
          assert MapSet.disjoint?(active_ids, passive_ids)
        end
      end
    end
  end
end
