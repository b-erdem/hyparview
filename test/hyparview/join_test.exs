defmodule HyParView.JoinTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.{Config, Peer, State}
  alias HyParView.Messages.{Disconnect, ForwardJoin, Join}
  alias HyParView.Test.Cluster

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

  # ── JOIN handler ──────────────────────────────────────────────────────

  describe "handle_message/2 — Join" do
    test "adds joiner to active view of an empty contact" do
      contact = fresh_state()
      joiner = Peer.new("joiner", :addr)

      {contact, actions} = State.handle_message(contact, %Join{new_peer: joiner})

      assert State.in_active?(contact, joiner)
      ups = for {:notify_up, p} <- actions, do: p
      assert joiner in ups
      # No FORWARD_JOIN: there are no other active peers to forward to.
      refute Enum.any?(actions, &match?({:send, _, %ForwardJoin{}}, &1))
    end

    test "fans out FORWARD_JOIN to all other active peers" do
      contact = fresh_state(config: [active_view_size: 5, arwl: 6])
      neighbors = for i <- 1..3, do: Peer.new("n#{i}", :addr)
      contact = fill_active(contact, neighbors)

      joiner = Peer.new("joiner", :addr)
      {contact, actions} = State.handle_message(contact, %Join{new_peer: joiner})

      forward_targets =
        for {:send, %Peer{id: id}, %ForwardJoin{new_peer: ^joiner, ttl: 6}} <- actions,
            do: id

      assert Enum.sort(forward_targets) == ["n1", "n2", "n3"]
      assert State.in_active?(contact, joiner)
    end

    test "evicts when active is full, sending Disconnect to evicted peer" do
      contact = fresh_state(config: [active_view_size: 2, arwl: 6])
      a = Peer.new("a", :addr)
      b = Peer.new("b", :addr)
      contact = fill_active(contact, [a, b])

      joiner = Peer.new("joiner", :addr)
      {contact, actions} = State.handle_message(contact, %Join{new_peer: joiner})

      disconnects = for {:send, target, %Disconnect{}} <- actions, do: target
      assert [evicted] = disconnects
      assert evicted in [a, b]

      # The evicted peer is now in passive
      assert State.in_passive?(contact, evicted)
      assert State.in_active?(contact, joiner)
      assert State.active_size(contact) == 2
    end

    test "rejects self-join (no-op)" do
      state = fresh_state()
      self = self_peer()
      {state2, actions} = State.handle_message(state, %Join{new_peer: self})

      refute State.in_active?(state2, self)
      refute State.in_passive?(state2, self)
      assert actions == []
    end
  end

  # ── FORWARD_JOIN handler ──────────────────────────────────────────────

  describe "handle_message/2 — ForwardJoin" do
    test "TTL = 0: adds to active and stops forwarding" do
      state = fresh_state(config: [active_view_size: 5])
      sender = Peer.new("sender", :addr)
      {state, _} = State.add_to_active(state, sender)

      new_peer = Peer.new("new", :addr)

      {state, actions} =
        State.handle_message(state, %ForwardJoin{new_peer: new_peer, ttl: 0, sender: sender})

      assert State.in_active?(state, new_peer)
      refute Enum.any?(actions, &match?({:send, _, %ForwardJoin{}}, &1))
    end

    test "active size <= 1: adds to active even if TTL > 0" do
      state = fresh_state(config: [active_view_size: 5])
      sender = Peer.new("sender", :addr)
      {state, _} = State.add_to_active(state, sender)

      assert State.active_size(state) == 1

      new_peer = Peer.new("new", :addr)

      {state, actions} =
        State.handle_message(state, %ForwardJoin{new_peer: new_peer, ttl: 6, sender: sender})

      assert State.in_active?(state, new_peer)
      refute Enum.any?(actions, &match?({:send, _, %ForwardJoin{}}, &1))
    end

    test "TTL > 0 with multiple active: forwards to a peer != sender" do
      state = fresh_state(config: [active_view_size: 5])
      sender = Peer.new("sender", :addr)
      others = for i <- 1..3, do: Peer.new("o#{i}", :addr)
      state = fill_active(state, [sender | others])

      new_peer = Peer.new("new", :addr)

      {_state, actions} =
        State.handle_message(state, %ForwardJoin{new_peer: new_peer, ttl: 4, sender: sender})

      forwards =
        for {:send, %Peer{id: id}, %ForwardJoin{new_peer: ^new_peer, ttl: 3}} <- actions, do: id

      assert [target_id] = forwards
      assert target_id != "sender"
      assert target_id in ["o1", "o2", "o3"]
    end

    test "TTL == PRWL: adds to passive AND keeps forwarding" do
      state = fresh_state(config: [active_view_size: 5, arwl: 6, prwl: 3])
      sender = Peer.new("sender", :addr)
      others = for i <- 1..3, do: Peer.new("o#{i}", :addr)
      state = fill_active(state, [sender | others])

      new_peer = Peer.new("new", :addr)

      {state, actions} =
        State.handle_message(state, %ForwardJoin{new_peer: new_peer, ttl: 3, sender: sender})

      # Both effects must hold:
      assert State.in_passive?(state, new_peer)

      forwards = for {:send, _, %ForwardJoin{ttl: 2}} <- actions, do: :ok
      assert [_] = forwards
    end

    test "rejects new_peer that is self" do
      state = fresh_state(config: [active_view_size: 5])
      sender = Peer.new("sender", :addr)
      others = for i <- 1..3, do: Peer.new("o#{i}", :addr)
      state = fill_active(state, [sender | others])

      msg = %ForwardJoin{new_peer: self_peer(), ttl: 4, sender: sender}
      {state, _actions} = State.handle_message(state, msg)

      refute State.in_active?(state, self_peer())
      refute State.in_passive?(state, self_peer())
    end
  end

  # ── Multi-node integration ────────────────────────────────────────────

  describe "cluster behaviour: sequential joins via one contact" do
    test "5-node cluster bounds hold and active views are populated" do
      peers = for i <- 1..5, do: Peer.new("p#{i}", :addr)
      [contact | rest] = peers

      cluster =
        Cluster.new(
          peers,
          [active_view_size: 4, passive_view_size: 10, arwl: 4, prwl: 2],
          base_seed: {7, 11, 13}
        )

      cluster =
        Enum.reduce(rest, cluster, fn joiner, c ->
          c = Cluster.join(c, joiner.id, contact.id)
          {_status, c} = Cluster.run_to_quiescence(c)
          c
        end)

      # Bounds hold for every node
      for {_id, state} <- cluster.nodes do
        assert State.active_size(state) <= 4
        assert State.passive_size(state) <= 10
      end

      # Every joining node has a non-empty active view at the end
      for joiner <- rest do
        joiner_state = Cluster.get_state(cluster, joiner.id)
        assert State.active_size(joiner_state) >= 1, "#{joiner.id} has no active peers"
      end
    end

    property "view bounds hold for any 4-10 node sequential join sequence" do
      check all(n <- StreamData.integer(4..10)) do
        peers = for i <- 1..n, do: Peer.new("p#{i}", :addr)
        [contact | rest] = peers

        cluster =
          Cluster.new(
            peers,
            [active_view_size: 4, passive_view_size: 10, arwl: 4, prwl: 2],
            base_seed: {n, n * 2, n * 3}
          )

        cluster =
          Enum.reduce(rest, cluster, fn joiner, c ->
            c = Cluster.join(c, joiner.id, contact.id)
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
