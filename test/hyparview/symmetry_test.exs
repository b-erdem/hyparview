defmodule HyParView.SymmetryTest do
  @moduledoc """
  Symmetry properties of the HyParView active view.

  The paper guarantees: "if node q is in the active view of node p, then
  node p is also in the active view of node q" (§4.1). This is achieved
  *eventually* — JOIN handshakes are bidirectional but propagation through
  FORWARD_JOIN takes time.

  These tests verify that after a deterministic cluster simulation runs
  to quiescence, active views are symmetric for every directed pair.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.{Peer, State}
  alias HyParView.Test.Cluster

  defp build_and_join(n, opts) do
    peers = for i <- 1..n, do: Peer.new("p#{i}", :addr)
    [contact | rest] = peers

    cluster =
      Cluster.new(
        peers,
        Keyword.get(opts, :config, []),
        base_seed: Keyword.get(opts, :seed, {1, 2, 3})
      )

    cluster =
      Enum.reduce(rest, cluster, fn joiner, c ->
        c = Cluster.join(c, joiner.id, contact.id)
        {_status, c} = Cluster.run_to_quiescence(c)
        c
      end)

    {peers, cluster}
  end

  defp tick_all_shuffles(cluster) do
    Enum.reduce(cluster.nodes, cluster, fn {id, _}, acc ->
      state = Map.fetch!(acc.nodes, id)
      {new_state, actions} = State.tick_shuffle(state)
      acc = %{acc | nodes: Map.put(acc.nodes, id, new_state)}

      Enum.reduce(actions, acc, fn
        {:send, %Peer{id: target}, msg}, c ->
          %{c | queue: :queue.in({target, msg}, c.queue)}

        _, c ->
          c
      end)
    end)
  end

  defp asymmetric_pairs(cluster) do
    nodes = cluster.nodes

    for {id_a, state_a} <- nodes,
        peer_b <- State.active_peers(state_a),
        not is_nil(Map.get(nodes, peer_b.id)),
        state_b = Map.fetch!(nodes, peer_b.id),
        not (cluster.nodes[id_a] != nil and
               State.in_active?(state_b, %Peer{id: id_a, address: state_a.self.address})),
        do: {id_a, peer_b.id}
  end

  describe "active-view symmetry after sequential joins" do
    test "5-node cluster has symmetric active views immediately after joins" do
      {_peers, cluster} =
        build_and_join(5,
          config: [
            active_view_size: 4,
            passive_view_size: 10,
            arwl: 4,
            prwl: 2
          ]
        )

      # Symmetry: every directed edge in active views has a reverse edge.
      asymmetric = asymmetric_pairs(cluster)

      assert asymmetric == [],
             "expected symmetric active views, found asymmetric pairs: #{inspect(asymmetric)}"
    end

    property "8-node cluster: symmetry holds after several shuffle rounds" do
      check all(
              seed_a <- StreamData.integer(1..1000),
              seed_b <- StreamData.integer(1..1000)
            ) do
        {_peers, cluster} =
          build_and_join(8,
            config: [
              active_view_size: 4,
              passive_view_size: 12,
              arwl: 4,
              prwl: 2,
              shuffle_active_count: 2,
              shuffle_passive_count: 3,
              shuffle_ttl: 4
            ],
            seed: {seed_a, seed_b, seed_a + seed_b}
          )

        # Run 5 rounds of shuffles to allow further propagation.
        cluster =
          Enum.reduce(1..5, cluster, fn _, c ->
            c = tick_all_shuffles(c)
            {_status, c} = Cluster.run_to_quiescence(c)
            c
          end)

        asymmetric = asymmetric_pairs(cluster)
        assert asymmetric == []
      end
    end
  end
end
