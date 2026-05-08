defmodule HyParView.RecoveryTest do
  use ExUnit.Case, async: true

  alias HyParView.{Config, Peer, State}
  alias HyParView.Messages.{Disconnect, Neighbor, NeighborReply}

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

  # ── Disconnect ────────────────────────────────────────────────────────

  describe "handle_message/2 — Disconnect" do
    test "moves sender from active to passive and attempts repair" do
      state = fresh_state(config: [active_view_size: 5])
      a = Peer.new("a", :addr)
      b = Peer.new("b", :addr)
      state = state |> fill_active([a, b]) |> fill_passive([Peer.new("c", :addr)])

      {state, actions} = State.handle_message(state, %Disconnect{peer: a})

      refute State.in_active?(state, a)
      assert State.in_passive?(state, a)

      # Repair: NEIGHBOR sent to some passive peer with priority :low (active not empty)
      neighbor_sends = for {:send, target, %Neighbor{priority: prio}} <- actions, do: {target, prio}
      assert [{_target, :low}] = neighbor_sends
    end

    test "uses :high priority when active becomes empty" do
      state = fresh_state(config: [active_view_size: 5])
      a = Peer.new("a", :addr)
      state = state |> fill_active([a]) |> fill_passive([Peer.new("c", :addr)])

      {_state, actions} = State.handle_message(state, %Disconnect{peer: a})

      neighbor_sends = for {:send, _, %Neighbor{priority: prio}} <- actions, do: prio
      assert [:high] = neighbor_sends
    end

    test "no-op if sender not in active view" do
      state = fresh_state()
      ghost = Peer.new("ghost", :addr)
      assert {^state, []} = State.handle_message(state, %Disconnect{peer: ghost})
    end

    test "no repair attempt when passive view is empty" do
      state = fresh_state()
      a = Peer.new("a", :addr)
      state = fill_active(state, [a])

      {state, actions} = State.handle_message(state, %Disconnect{peer: a})

      refute State.in_active?(state, a)
      refute Enum.any?(actions, &match?({:send, _, %Neighbor{}}, &1))
    end
  end

  # ── connection_lost ──────────────────────────────────────────────────

  describe "connection_lost/2" do
    test "removes peer from active without demoting to passive (failure path)" do
      state = fresh_state(config: [active_view_size: 5])
      a = Peer.new("a", :addr)
      state = fill_active(state, [a])

      {state, _actions} = State.connection_lost(state, a)

      refute State.in_active?(state, a)
      # Per paper §4.5: failed (non-graceful) peers do not go to passive
      refute State.in_passive?(state, a)
    end

    test "no-op when peer not in active" do
      state = fresh_state()
      assert {^state, []} = State.connection_lost(state, Peer.new("ghost", :addr))
    end

    test "triggers a repair attempt when passive has candidates" do
      state = fresh_state()
      a = Peer.new("a", :addr)
      candidate = Peer.new("candidate", :addr)
      state = state |> fill_active([a]) |> fill_passive([candidate])

      {_state, actions} = State.connection_lost(state, a)

      assert [{:send, ^candidate, %Neighbor{priority: :high}}] =
               for({:send, _, %Neighbor{}} = a <- actions, do: a)
    end
  end

  # ── Neighbor (request) ────────────────────────────────────────────────

  describe "handle_message/2 — Neighbor (request)" do
    test "high priority is always accepted, evicting if needed" do
      state = fresh_state(config: [active_view_size: 2])
      state = fill_active(state, [Peer.new("a", :addr), Peer.new("b", :addr)])

      requester = Peer.new("requester", :addr)
      msg = %Neighbor{peer: requester, priority: :high}
      {state, actions} = State.handle_message(state, msg)

      assert State.in_active?(state, requester)

      replies = for {:send, ^requester, %NeighborReply{accepted?: ok}} <- actions, do: ok
      assert [true] = replies
    end

    test "low priority is rejected when active is full" do
      state = fresh_state(config: [active_view_size: 2])
      state = fill_active(state, [Peer.new("a", :addr), Peer.new("b", :addr)])

      requester = Peer.new("requester", :addr)
      msg = %Neighbor{peer: requester, priority: :low}
      {state, actions} = State.handle_message(state, msg)

      refute State.in_active?(state, requester)
      assert [{:send, ^requester, %NeighborReply{accepted?: false}}] = actions
    end

    test "low priority is accepted when active has free space" do
      state = fresh_state(config: [active_view_size: 5])
      state = fill_active(state, [Peer.new("a", :addr)])

      requester = Peer.new("requester", :addr)
      msg = %Neighbor{peer: requester, priority: :low}
      {state, actions} = State.handle_message(state, msg)

      assert State.in_active?(state, requester)
      replies = for {:send, ^requester, %NeighborReply{accepted?: true}} <- actions, do: :ok
      assert [_] = replies
    end

    test "rejects self-request" do
      state = fresh_state()
      msg = %Neighbor{peer: self_peer(), priority: :high}
      assert {^state, []} = State.handle_message(state, msg)
    end
  end

  # ── NeighborReply ────────────────────────────────────────────────────

  describe "handle_message/2 — NeighborReply" do
    test "accepted: replier moves from passive to active, pending_repair clears" do
      state = fresh_state()
      replier = Peer.new("replier", :addr)
      state = fill_passive(state, [replier])
      # `repair_target` must match the replier — otherwise the reply
      # is treated as stale and dropped (issue #1).
      state = %{
        state
        | pending_repair: MapSet.new([replier.id]),
          repair_target: replier
      }

      msg = %NeighborReply{peer: replier, accepted?: true}
      {state, _actions} = State.handle_message(state, msg)

      assert State.in_active?(state, replier)
      refute State.in_passive?(state, replier)
      assert state.pending_repair == nil
      assert state.repair_target == nil
    end

    test "accepted: stale reply (no in-flight target) is dropped, active view untouched" do
      # Regression for issue #1: a NEIGHBOR_REPLY arriving when no
      # NEIGHBOR is in flight (e.g., because the original target was
      # declared lost via `connection_lost/2` between send and reply)
      # must NOT add the replier to the active view.
      state = fresh_state()
      stranger = Peer.new("stranger", :addr)

      # Note: no `repair_target` set, no `pending_repair`.
      msg = %NeighborReply{peer: stranger, accepted?: true}
      {state, actions} = State.handle_message(state, msg)

      refute State.in_active?(state, stranger)
      assert actions == []
    end

    test "rejected: tries another passive peer, accumulates tried set" do
      state = fresh_state()
      a = Peer.new("a", :addr)
      b = Peer.new("b", :addr)
      state = fill_passive(state, [a, b])
      state = %{state | pending_repair: MapSet.new([a.id]), repair_target: a}

      msg = %NeighborReply{peer: a, accepted?: false}
      {state, actions} = State.handle_message(state, msg)

      # Pending repair should now include both
      assert state.pending_repair == MapSet.new([a.id, b.id])
      # And the new repair target should be `b`
      assert state.repair_target == b

      # And we should have sent NEIGHBOR to b
      assert [{:send, ^b, %Neighbor{}}] = for({:send, _, %Neighbor{}} = act <- actions, do: act)
    end

    test "rejected with no remaining passive candidates: gives up" do
      state = fresh_state()
      a = Peer.new("a", :addr)
      state = fill_passive(state, [a])
      state = %{state | pending_repair: MapSet.new([a.id]), repair_target: a}

      msg = %NeighborReply{peer: a, accepted?: false}
      {state, actions} = State.handle_message(state, msg)

      assert state.pending_repair == nil
      assert state.repair_target == nil
      refute Enum.any?(actions, &match?({:send, _, %Neighbor{}}, &1))
    end
  end
end
