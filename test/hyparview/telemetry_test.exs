defmodule HyParView.TelemetryTest do
  use ExUnit.Case, async: true

  alias HyParView.Test.Integration

  import Integration

  defp start_node(peer, opts \\ []) do
    default = [
      peer: peer,
      transport: HyParView.Transport.Test,
      config: [active_view_size: 4, shuffle_interval: 1_000_000]
    ]

    start_supervised!({HyParView.Server, Keyword.merge(default, opts)}, id: peer.id)
  end

  defp attach_handler(test_pid, prefix \\ [:hyparview]) do
    handler_id = "test-#{System.unique_integer([:positive])}"
    events = Enum.map(HyParView.Telemetry.event_paths(), &(prefix ++ &1))

    :telemetry.attach_many(
      handler_id,
      events,
      fn event, measurements, metadata, _config ->
        send(test_pid, {:telemetry, event, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
    handler_id
  end

  test "emits :peer, :added when a peer joins" do
    attach_handler(self())

    contact = unique_peer("tel-c")
    joiner = unique_peer("tel-j")

    contact_pid = start_node(contact)
    _joiner_pid = start_node(joiner, contacts: [contact])

    # Either side may report the peer addition first, depending on scheduling.
    # Both should fire eventually.
    assert_receive {:telemetry, [:hyparview, :peer, :added], %{count: 1},
                    %{peer: _, view: :active}},
                   1_000

    _ = contact_pid
  end

  test "emits :send for each outbound message" do
    attach_handler(self())

    contact = unique_peer("tel-send-c")
    joiner = unique_peer("tel-send-j")

    _ = start_node(contact)
    _ = start_node(joiner, contacts: [contact])

    # The joiner sends a Join, the contact replies via FORWARD_JOIN... we should
    # observe at least one :send event with a known message module.
    assert_receive {:telemetry, [:hyparview, :send], _measurements, metadata}, 1_000

    assert metadata.message in [
             HyParView.Messages.Join,
             HyParView.Messages.ForwardJoin,
             HyParView.Messages.Neighbor,
             HyParView.Messages.NeighborReply
           ]
  end

  test "supports custom telemetry prefix" do
    handler_id = attach_handler(self(), [:custom, :hp])

    contact = unique_peer("tel-pre-c")
    joiner = unique_peer("tel-pre-j")

    _ = start_node(contact, telemetry_prefix: [:custom, :hp])
    _ = start_node(joiner, contacts: [contact], telemetry_prefix: [:custom, :hp])

    assert_receive {:telemetry, [:custom, :hp, :peer, :added], _, _}, 1_000

    _ = handler_id
  end

  test "does not emit events under the wrong prefix" do
    attach_handler(self(), [:wrong])

    contact = unique_peer("tel-wrong-c")
    _ = start_node(contact)

    refute_receive {:telemetry, _, _, _}, 100
  end

  describe "Telemetry.event/2 + event_paths/0" do
    test "event_paths/0 returns all relative event paths" do
      paths = HyParView.Telemetry.event_paths()
      assert [:peer, :added] in paths
      assert [:peer, :removed] in paths
      assert [:send] in paths
      assert [:send_error] in paths
    end

    test "event/2 prepends prefix" do
      assert HyParView.Telemetry.event([:hp], [:foo, :bar]) == [:hp, :foo, :bar]
    end
  end
end
