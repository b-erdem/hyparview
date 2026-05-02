defmodule HyParView.ComprehensiveTest do
  @moduledoc """
  Edge-case and stress tests that fall outside the dedicated milestone test
  files. The focus is on robustness: malformed input, concurrent operations,
  large clusters, restart, subscriber lifecycle.
  """

  use ExUnit.Case, async: true

  alias HyParView.{Peer, Server}
  alias HyParView.Test.Integration

  import Integration

  defp start_node(peer, opts \\ []) do
    default = [
      peer: peer,
      transport: HyParView.Transport.Test,
      config: [
        active_view_size: 4,
        passive_view_size: 12,
        arwl: 4,
        prwl: 2,
        shuffle_interval: 1_000_000
      ]
    ]

    start_supervised!({Server, Keyword.merge(default, opts)}, id: peer.id)
  end

  describe "lone node" do
    test "starts with no contacts and serves empty views" do
      pid = start_node(unique_peer("solo"), contacts: [])
      assert HyParView.active_view(pid) == []
      assert HyParView.passive_view(pid) == []
      assert :ok = HyParView.stop(pid)
    end

    test "tolerates an unreachable contact (logs send_error, stays running)" do
      ghost = Peer.new("ghost", make_ref())
      pid = start_node(unique_peer("alive"), contacts: [ghost])

      # Server should still be alive after failing to reach the ghost.
      Process.sleep(50)
      assert Process.alive?(pid)
      # Ghost is in active view (we pre-emptively added it via initiate_join);
      # nothing in passive.
      assert HyParView.passive_view(pid) == []
    end
  end

  describe "concurrent joins" do
    test "two joiners arriving at the same contact in rapid succession both succeed" do
      contact = unique_peer("conj-c")
      a = unique_peer("conj-a")
      b = unique_peer("conj-b")

      contact_pid = start_node(contact)
      _ = start_node(a, contacts: [contact])
      _ = start_node(b, contacts: [contact])

      assert :ok =
               wait_for_active(
                 [contact_pid],
                 fn av -> length(av) == 2 end,
                 1_000
               )

      ids = contact_pid |> HyParView.active_view() |> MapSet.new(& &1.id)
      assert MapSet.new([a.id, b.id]) == ids
    end
  end

  describe "larger cluster" do
    test "12-node sequential join converges with bounded views" do
      contact = unique_peer("big-c")
      contact_pid = start_node(contact)

      joiners =
        for i <- 1..11 do
          j = unique_peer("big-j#{i}")
          start_node(j, contacts: [contact])
        end

      pids = [contact_pid | joiners]

      assert :ok =
               wait_for_active(pids, fn av -> av != [] end, 4_000)

      for pid <- pids do
        av_size = pid |> HyParView.active_view() |> length()
        pv_size = pid |> HyParView.passive_view() |> length()
        assert av_size >= 1
        assert av_size <= 4
        assert pv_size <= 12
      end
    end
  end

  describe "subscribers" do
    test "receive :peer_down on connection_lost" do
      contact = unique_peer("dn-c")
      a = unique_peer("dn-a")

      contact_pid = start_node(contact)
      _ = start_node(a, contacts: [contact])

      :ok = HyParView.subscribe(contact_pid)

      # Wait for `a` to be active at the contact.
      assert :ok = wait_for_active([contact_pid], fn av -> av != [] end, 1_000)

      victim =
        contact_pid
        |> HyParView.active_view()
        |> Enum.find(&(&1.id == a.id))

      assert victim != nil
      :ok = HyParView.connection_lost(contact_pid, victim)

      assert_receive {:hyparview, {:peer_down, %Peer{id: down_id}}}, 1_000
      assert down_id == a.id
    end

    test "multiple subscribers all get notified" do
      contact = unique_peer("multi-c")
      joiner = unique_peer("multi-j")

      contact_pid = start_node(contact)

      # Spawn two subscriber processes that just wait for an event and reply.
      test_pid = self()

      sub1 =
        spawn_link(fn ->
          :ok = HyParView.subscribe(contact_pid)
          send(test_pid, {:subscribed, :sub1})

          receive do
            {:hyparview, {:peer_up, _}} -> send(test_pid, {:got, :sub1})
          after
            1_000 -> send(test_pid, {:timeout, :sub1})
          end
        end)

      sub2 =
        spawn_link(fn ->
          :ok = HyParView.subscribe(contact_pid)
          send(test_pid, {:subscribed, :sub2})

          receive do
            {:hyparview, {:peer_up, _}} -> send(test_pid, {:got, :sub2})
          after
            1_000 -> send(test_pid, {:timeout, :sub2})
          end
        end)

      # Wait for both subscribers to register before triggering the join.
      assert_receive {:subscribed, :sub1}, 200
      assert_receive {:subscribed, :sub2}, 200

      _ = start_node(joiner, contacts: [contact])

      assert_receive {:got, :sub1}, 1_000
      assert_receive {:got, :sub2}, 1_000

      _ = sub1
      _ = sub2
    end
  end

  describe "view inspection" do
    test "active_view and passive_view are disjoint" do
      contact = unique_peer("inv-c")

      pids =
        for i <- 1..5 do
          if i == 1 do
            start_node(contact)
          else
            start_node(unique_peer("inv-j#{i}"), contacts: [contact])
          end
        end

      assert :ok = wait_for_active(pids, fn av -> av != [] end, 2_000)

      for pid <- pids do
        active_ids = pid |> HyParView.active_view() |> MapSet.new(& &1.id)
        passive_ids = pid |> HyParView.passive_view() |> MapSet.new(& &1.id)
        assert MapSet.disjoint?(active_ids, passive_ids)
      end
    end
  end

  describe "bad input" do
    test "connection_lost for an unknown peer is a silent no-op" do
      pid = start_node(unique_peer("bad-1"))
      :ok = HyParView.connection_lost(pid, Peer.new("ghost", make_ref()))
      Process.sleep(20)
      assert Process.alive?(pid)
      assert HyParView.active_view(pid) == []
    end
  end

  describe "stop semantics" do
    test "stop returns :ok and the server exits with reason :normal" do
      pid = start_node(unique_peer("stop-1"))
      ref = Process.monitor(pid)
      assert :ok = HyParView.stop(pid)
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 500
    end
  end
end
