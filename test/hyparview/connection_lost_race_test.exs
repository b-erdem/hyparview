defmodule HyParView.ConnectionLostRaceTest do
  @moduledoc """
  Regression test for issue #1 — late `NEIGHBOR_REPLY` after
  `connection_lost` re-adds a dead peer to the active view.

  The race shape (paper-classic for any membership protocol with
  asynchronous transport):

      step 1  A's transport: `{:connection_lost, B}` arrives
              -> A's `connection_lost(B)` removes B from active and
                 launches a fresh repair attempt to a passive peer.

      step 2  A's transport: `{:transport_message, B,
              %NeighborReply{peer: B, accepted?: true}}` arrives
              (this reply was queued before B died — or, in the
              adversarial case, injected by a buggy / malicious
              peer)
              -> previously: `handle_neighbor_reply/2` accepted
                 unconditionally and re-added B to the active view,
                 leaving A with a dead peer that wedges every
                 subsequent send to B.
              -> now: A's State must reject any NEIGHBOR_REPLY
                 that doesn't correspond to a NEIGHBOR currently
                 in flight.

  The bug surfaced in `b-erdem/lockstep`'s POS-strategy schedule
  exploration of HyParView's State module (iteration 1, seed 1).
  """

  use ExUnit.Case, async: true

  alias HyParView.{Messages.NeighborReply, Peer, Server}
  alias HyParView.Test.Integration

  defp start_node(peer, opts) do
    default = [
      peer: peer,
      transport: HyParView.Transport.Test,
      config: [
        active_view_size: 3,
        passive_view_size: 10,
        arwl: 4,
        prwl: 2,
        shuffle_interval: 1_000_000
      ]
    ]

    start_supervised!(
      {Server, Keyword.merge(default, opts)},
      id: peer.id,
      restart: :temporary
    )
  end

  test "stray NEIGHBOR_REPLY does not re-add a peer that connection_lost just evicted" do
    a = Peer.new("clr-A-#{System.unique_integer([:positive])}", {:address, "A"})
    b = Peer.new("clr-B-#{System.unique_integer([:positive])}", {:address, "B"})
    c = Peer.new("clr-C-#{System.unique_integer([:positive])}", {:address, "C"})

    a_pid = start_node(a, contacts: [])
    b_pid = start_node(b, contacts: [a])
    _c_pid = start_node(c, contacts: [a])

    # Wait for everyone to learn about each other.
    :ok =
      Integration.wait_until(
        2_000,
        fn ->
          length(HyParView.active_view(a_pid)) >= 2
        end
      )

    assert b in HyParView.active_view(a_pid),
           "B should be in A's active view before the race"

    # Step 1: declare B lost on A. A removes B from active view.
    HyParView.connection_lost(a_pid, b)

    :ok =
      Integration.wait_until(
        500,
        fn -> b not in HyParView.active_view(a_pid) end
      )

    # Step 2: inject the stray NEIGHBOR_REPLY claiming B accepted us
    # back. With the old (buggy) code, A would re-add B to its active
    # view here. With the fix, A rejects it because there's no
    # outstanding NEIGHBOR repair attempt targeting B.
    stray_reply = %NeighborReply{peer: b, accepted?: true}
    Kernel.send(a_pid, {:transport_message, b, stray_reply})

    # Give the GenServer a tick to process.
    Process.sleep(50)

    a_active = HyParView.active_view(a_pid)

    refute b in a_active,
           """
           B was re-added to A's active view by the stray NEIGHBOR_REPLY.
           Race regression: handle_neighbor_reply/2 must validate the
           reply against the current repair target (issue #1).

           Active view: #{inspect(a_active)}
           """

    HyParView.stop(b_pid)
  end
end
