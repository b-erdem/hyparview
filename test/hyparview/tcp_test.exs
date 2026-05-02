defmodule HyParView.TCPTest do
  @moduledoc """
  Real-network integration tests for `HyParView.Transport.TCP`.

  Each test allocates ephemeral ports on the loopback interface so they
  can run in parallel without conflicts.
  """

  use ExUnit.Case, async: true

  import ExUnit.CaptureLog
  import HyParView.Test.Integration, only: [wait_for_active: 3]

  alias HyParView.{Peer, Server}

  defp ephemeral_port do
    {:ok, sock} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, reuseaddr: true)
    {:ok, port} = :inet.port(sock)
    :gen_tcp.close(sock)
    port
  end

  defp tcp_peer(id_prefix) do
    Peer.new(
      "#{id_prefix}-#{System.unique_integer([:positive])}",
      {{127, 0, 0, 1}, ephemeral_port()}
    )
  end

  defp start_tcp_node(peer, opts \\ []) do
    default = [
      peer: peer,
      transport: HyParView.Transport.TCP,
      config: [active_view_size: 4, shuffle_interval: 1_000_000]
    ]

    # `:temporary` so that killing a node in failure-detection tests doesn't
    # trigger a supervisor-driven restart (which would re-JOIN and confuse
    # state assertions).
    start_supervised!(%{
      id: peer.id,
      start: {Server, :start_link, [Keyword.merge(default, opts)]},
      restart: :temporary
    })
  end

  describe "two-node TCP handshake" do
    test "joiner and contact see each other in active views" do
      contact = tcp_peer("tcp-contact")
      joiner = tcp_peer("tcp-joiner")

      contact_pid = start_tcp_node(contact)
      joiner_pid = start_tcp_node(joiner, contacts: [contact])

      assert :ok =
               wait_for_active(
                 [contact_pid, joiner_pid],
                 fn av -> match?([_], av) end,
                 2_000
               )

      [c_in_joiner] = HyParView.active_view(joiner_pid)
      [j_in_contact] = HyParView.active_view(contact_pid)

      assert c_in_joiner.id == contact.id
      assert j_in_contact.id == joiner.id
    end
  end

  describe "3-node TCP cluster" do
    test "every node has at least one active peer after sequential joins" do
      contact = tcp_peer("3tcp-c")
      a = tcp_peer("3tcp-a")
      b = tcp_peer("3tcp-b")

      contact_pid = start_tcp_node(contact)
      a_pid = start_tcp_node(a, contacts: [contact])
      b_pid = start_tcp_node(b, contacts: [contact])

      pids = [contact_pid, a_pid, b_pid]

      assert :ok = wait_for_active(pids, fn av -> av != [] end, 3_000)

      for pid <- pids do
        size = pid |> HyParView.active_view() |> length()
        assert size >= 1
        assert size <= 4
      end
    end
  end

  describe "TCP failure detection" do
    test "killing a node auto-triggers connection_lost on its peer" do
      contact = tcp_peer("fail-c")
      a = tcp_peer("fail-a")

      contact_pid = start_tcp_node(contact)
      a_pid = start_tcp_node(a, contacts: [contact])

      :ok = HyParView.subscribe(contact_pid)

      assert :ok = wait_for_active([contact_pid, a_pid], fn av -> av != [] end, 2_000)

      # Drain the :peer_up event from the join.
      receive do
        {:hyparview, {:peer_up, _}} -> :ok
      after
        200 -> :ok
      end

      # Kill `a`. Its outbound TCP closes; the contact's `Connection` reads
      # `{:tcp_closed, _}`, calls the events callback with `{:peer_lost, a}`,
      # which the Server translates into `State.connection_lost(a)` —
      # the protocol-level repair fires automatically. CaptureLog silences
      # the expected SIGKILL noise.
      capture_log(fn ->
        Process.exit(a_pid, :kill)
        Process.sleep(150)
      end)

      assert Process.alive?(contact_pid)

      # The :peer_down event must arrive at the subscriber.
      assert_receive {:hyparview, {:peer_down, %Peer{id: down_id}}}, 1_000
      assert down_id == a.id

      # And `a` must be gone from the active view.
      remaining = contact_pid |> HyParView.active_view() |> Enum.map(& &1.id)
      refute a.id in remaining
    end
  end
end
