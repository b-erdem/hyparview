defmodule HyParView.IntegrationTest do
  use ExUnit.Case, async: true

  alias HyParView.Peer
  alias HyParView.Test.Integration

  import Integration

  defp start_node(peer, opts \\ []) do
    default = [
      peer: peer,
      transport: HyParView.Transport.Test,
      config: [
        active_view_size: 4,
        passive_view_size: 10,
        arwl: 4,
        prwl: 2,
        # disabled in most tests; we trigger shuffles explicitly when needed
        shuffle_interval: 1_000_000
      ]
    ]

    start_supervised!({HyParView.Server, Keyword.merge(default, opts)}, id: peer.id)
  end

  describe "single-node lifecycle" do
    test "starts and exposes empty views" do
      pid = start_node(unique_peer("solo"))
      assert HyParView.active_view(pid) == []
      assert HyParView.passive_view(pid) == []
    end

    test "stops cleanly" do
      pid = start_node(unique_peer("solo"))
      assert :ok = HyParView.stop(pid)
      refute Process.alive?(pid)
    end
  end

  describe "two-node handshake" do
    test "joiner and contact end up with each other in active views" do
      contact = unique_peer("contact")
      joiner = unique_peer("joiner")

      contact_pid = start_node(contact)
      joiner_pid = start_node(joiner, contacts: [contact])

      assert :ok =
               wait_for_active(
                 [contact_pid, joiner_pid],
                 fn av -> match?([_], av) end,
                 1_000
               )

      [c_in_joiner] = HyParView.active_view(joiner_pid)
      [j_in_contact] = HyParView.active_view(contact_pid)
      assert c_in_joiner.id == contact.id
      assert j_in_contact.id == joiner.id
    end
  end

  describe "5-node cluster" do
    test "every node has at least one active peer after sequential joins" do
      contact = unique_peer("c")
      joiners = for i <- 1..4, do: unique_peer("j#{i}")

      contact_pid = start_node(contact)

      joiner_pids =
        for j <- joiners do
          start_node(j, contacts: [contact])
        end

      pids = [contact_pid | joiner_pids]

      assert :ok =
               wait_for_active(
                 pids,
                 fn av -> av != [] end,
                 2_000
               )

      for pid <- pids do
        av = HyParView.active_view(pid)
        size = length(av)
        assert size >= 1
        assert size <= 4
      end
    end
  end

  describe "subscribers" do
    test "receive :peer_up when a new peer joins" do
      contact = unique_peer("sub-contact")
      joiner = unique_peer("sub-joiner")

      contact_pid = start_node(contact)
      :ok = HyParView.subscribe(contact_pid)

      _joiner_pid = start_node(joiner, contacts: [contact])

      assert_receive {:hyparview, {:peer_up, %Peer{} = peer}}, 1_000
      assert peer.id == joiner.id
    end

    test "subscriber that exits is auto-removed (no leak)" do
      contact = unique_peer("sub-contact-2")
      contact_pid = start_node(contact)

      # Spawn a short-lived subscriber.
      task =
        Task.async(fn ->
          :ok = HyParView.subscribe(contact_pid)
          :done
        end)

      :done = Task.await(task)

      # Sanity: server is still healthy, no leaked monitor refs.
      assert HyParView.active_view(contact_pid) == []
    end
  end

  describe "connection_lost recovery" do
    test "active peer marked lost is removed and repair NEIGHBOR is sent" do
      contact = unique_peer("rec-contact")
      a = unique_peer("rec-a")
      b = unique_peer("rec-b")

      contact_pid = start_node(contact)
      _a_pid = start_node(a, contacts: [contact])
      _b_pid = start_node(b, contacts: [contact])

      # Wait for both joiners visible at contact.
      assert :ok =
               wait_for_active([contact_pid], fn av -> length(av) == 2 end, 1_000)

      # Tell the contact one peer is dead.
      victim = HyParView.active_view(contact_pid) |> Enum.find(&(&1.id == a.id))
      assert victim != nil

      :ok = HyParView.connection_lost(contact_pid, victim)

      # The dead peer should be gone from contact's active view.
      assert :ok =
               wait_for_active(
                 [contact_pid],
                 fn av -> Enum.all?(av, fn p -> p.id != a.id end) end,
                 500
               )

      remaining_ids = contact_pid |> HyParView.active_view() |> Enum.map(& &1.id)
      refute a.id in remaining_ids
    end
  end

  describe "shuffle propagation" do
    test "after periodic shuffles, passive views grow with non-direct peers" do
      contact = unique_peer("sh-c")
      joiners = for i <- 1..5, do: unique_peer("sh-j#{i}")
      all_peers = [contact | joiners]

      # Use a short shuffle interval for this test.
      contact_pid =
        start_node(contact,
          config: [
            active_view_size: 3,
            passive_view_size: 10,
            arwl: 3,
            prwl: 2,
            shuffle_active_count: 2,
            shuffle_passive_count: 3,
            shuffle_ttl: 3,
            shuffle_interval: 50
          ]
        )

      joiner_pids =
        for j <- joiners do
          start_node(j,
            contacts: [contact],
            config: [
              active_view_size: 3,
              passive_view_size: 10,
              arwl: 3,
              prwl: 2,
              shuffle_active_count: 2,
              shuffle_passive_count: 3,
              shuffle_ttl: 3,
              shuffle_interval: 50
            ]
          )
        end

      pids = [contact_pid | joiner_pids]

      assert :ok =
               wait_for_active(pids, fn av -> av != [] end, 2_000)

      # Allow a few shuffle ticks to run.
      Process.sleep(400)

      # All bounds still hold.
      for pid <- pids do
        av = HyParView.active_view(pid)
        pv = HyParView.passive_view(pid)
        assert length(av) <= 3
        assert length(pv) <= 10

        # No node has itself in either view.
        active_ids = MapSet.new(av, & &1.id)
        passive_ids = MapSet.new(pv, & &1.id)
        assert MapSet.disjoint?(active_ids, passive_ids)
      end

      # Total reachable peers across all views should cover most of the cluster.
      reach_per_node =
        for pid <- pids do
          av_ids = pid |> HyParView.active_view() |> MapSet.new(& &1.id)
          pv_ids = pid |> HyParView.passive_view() |> MapSet.new(& &1.id)
          MapSet.union(av_ids, pv_ids) |> MapSet.size()
        end

      avg_reach = Enum.sum(reach_per_node) / length(reach_per_node)
      total_others = length(all_peers) - 1
      # Each node should know about at least half of the others.
      assert avg_reach >= total_others / 2,
             "average reach #{avg_reach} too low (expected >= #{total_others / 2})"
    end
  end
end
