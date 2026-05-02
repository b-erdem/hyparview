defmodule HyParView.Test.Cluster do
  @moduledoc """
  In-memory cluster simulator for testing the pure `HyParView.State` machine
  with multiple peers exchanging messages.

  Holds a map of `peer_id => State.t()` plus a FIFO message queue. Messages
  are delivered one at a time via `step/1`; `run_to_quiescence/2` runs until
  the queue is empty (or a step budget is exhausted).

  This simulator is for *unit* multi-node testing — no processes, no
  transport, no clocks. Real-network integration tests live in
  `HyParView.Transport.Test` (milestone 6) and the TCP transport
  (milestone 7).
  """

  alias HyParView.{Config, Peer, State}

  @enforce_keys [:nodes]
  defstruct nodes: %{}, queue: nil, step_count: 0, dropped: 0, partition_filter: nil

  @type peer_id :: Peer.id()
  @type queued :: {peer_id(), HyParView.Messages.t()}

  @typedoc """
  Filter applied at message-enqueue time. Returns `:drop` to discard a
  message, `:deliver` to enqueue it. `nil` means deliver everything.
  """
  @type partition_filter :: (peer_id(), peer_id() -> :drop | :deliver) | nil

  @type t :: %__MODULE__{
          nodes: %{peer_id() => State.t()},
          queue: :queue.queue(queued()),
          step_count: non_neg_integer(),
          dropped: non_neg_integer(),
          partition_filter: partition_filter()
        }

  @doc """
  Build a cluster from a list of `Peer.t()`. Each peer gets its own State
  with the same `Config` and a deterministic per-peer RNG seed (derived
  from the supplied `:base_seed`).
  """
  @spec new([Peer.t()], keyword(), keyword()) :: t()
  def new(peers, config_opts \\ [], opts \\ []) do
    base_seed = Keyword.get(opts, :base_seed, {1, 2, 3})
    config = Config.new(config_opts)

    nodes =
      peers
      |> Enum.with_index()
      |> Map.new(fn {peer, i} ->
        seed = perturb(base_seed, i)
        {peer.id, State.new(peer, config, rng_seed: seed)}
      end)

    %__MODULE__{nodes: nodes, queue: :queue.new()}
  end

  defp perturb({a, b, c}, i), do: {a + i, b + i * 7, c + i * 13}

  @doc """
  Drive a JOIN: the joiner adds the contact to its active view and sends
  a `Join` message. Both the joiner and the contact must already be in
  the cluster's nodes map.
  """
  @spec join(t(), peer_id(), peer_id()) :: t()
  def join(%__MODULE__{} = cluster, joiner_id, contact_id) do
    joiner_state = Map.fetch!(cluster.nodes, joiner_id)
    contact_state = Map.fetch!(cluster.nodes, contact_id)
    {joiner_state, actions} = State.initiate_join(joiner_state, contact_state.self)

    cluster = %{cluster | nodes: Map.put(cluster.nodes, joiner_id, joiner_state)}
    apply_actions(cluster, joiner_id, actions)
  end

  @doc """
  Install a partition filter that drops messages crossing between two
  groups of node ids. Within each group, messages still flow normally.

  Use `heal/1` to remove the filter.

  ## Example

      cluster = Cluster.partition(cluster, ["p1", "p2"], ["p3", "p4"])
      # Messages from p1/p2 to p3/p4 (and back) are dropped.
      cluster = Cluster.heal(cluster)
  """
  @spec partition(t(), [peer_id()], [peer_id()]) :: t()
  def partition(%__MODULE__{} = cluster, group_a, group_b) do
    a_set = MapSet.new(group_a)
    b_set = MapSet.new(group_b)

    filter = fn from_id, to_id ->
      cond do
        MapSet.member?(a_set, from_id) and MapSet.member?(b_set, to_id) -> :drop
        MapSet.member?(b_set, from_id) and MapSet.member?(a_set, to_id) -> :drop
        true -> :deliver
      end
    end

    %{cluster | partition_filter: filter}
  end

  @doc "Remove any active partition filter; messages flow freely again."
  @spec heal(t()) :: t()
  def heal(%__MODULE__{} = cluster), do: %{cluster | partition_filter: nil}

  @doc """
  Trigger `connection_lost` on every node for any active peer in `dead_set`.

  Use this after `partition/3` to simulate failure-detection of cross-half
  peers (which the simulator otherwise wouldn't detect, since it has no
  TCP). The resulting protocol-level repair runs as messages get delivered
  via subsequent `step/1` calls.
  """
  @spec detect_lost(t(), [peer_id()]) :: t()
  def detect_lost(%__MODULE__{} = cluster, dead_set) do
    dead = MapSet.new(dead_set)

    Enum.reduce(cluster.nodes, cluster, fn {node_id, state}, acc ->
      detect_lost_for_node(acc, node_id, State.active_peers(state), dead)
    end)
  end

  defp detect_lost_for_node(cluster, node_id, active_peers, dead) do
    Enum.reduce(active_peers, cluster, fn peer, acc ->
      if MapSet.member?(dead, peer.id),
        do: trigger_loss(acc, node_id, peer),
        else: acc
    end)
  end

  defp trigger_loss(cluster, node_id, peer) do
    current_state = Map.fetch!(cluster.nodes, node_id)
    {new_state, actions} = State.connection_lost(current_state, peer)
    cluster = %{cluster | nodes: Map.put(cluster.nodes, node_id, new_state)}
    apply_actions(cluster, node_id, actions)
  end

  @doc """
  Deliver one queued message. Returns:

    * `{:ok, cluster}` if a message was delivered to a known node;
    * `{:dropped, cluster}` if the recipient is not in the cluster
       (count tracked in `cluster.dropped`);
    * `:empty` if the queue was empty.
  """
  @spec step(t()) :: {:ok, t()} | {:dropped, t()} | :empty
  def step(%__MODULE__{queue: q} = cluster) do
    case :queue.out(q) do
      {:empty, _} ->
        :empty

      {{:value, {to_id, msg}}, q2} ->
        cluster = %{cluster | queue: q2, step_count: cluster.step_count + 1}

        case Map.fetch(cluster.nodes, to_id) do
          :error ->
            {:dropped, %{cluster | dropped: cluster.dropped + 1}}

          {:ok, state} ->
            {state, actions} = State.handle_message(state, msg)
            cluster = %{cluster | nodes: Map.put(cluster.nodes, to_id, state)}
            {:ok, apply_actions(cluster, to_id, actions)}
        end
    end
  end

  @doc """
  Run `step/1` repeatedly until the queue is empty or `max_steps` reached.
  Returns `{:done, cluster}` if drained or `{:max_steps, cluster}` on cap.
  """
  @spec run_to_quiescence(t(), non_neg_integer()) :: {:done | :max_steps, t()}
  def run_to_quiescence(cluster, max_steps \\ 5_000) do
    do_run(cluster, max_steps)
  end

  defp do_run(cluster, 0), do: {:max_steps, cluster}

  defp do_run(cluster, n) do
    case step(cluster) do
      :empty -> {:done, cluster}
      {:ok, c} -> do_run(c, n - 1)
      {:dropped, c} -> do_run(c, n - 1)
    end
  end

  defp apply_actions(cluster, sender_id, actions) do
    Enum.reduce(actions, cluster, fn
      {:send, %Peer{id: to_id}, msg}, c -> enqueue(c, sender_id, to_id, msg)
      {:notify_up, _peer}, c -> c
      {:notify_down, _peer}, c -> c
    end)
  end

  defp enqueue(%__MODULE__{} = cluster, from_id, to_id, msg) do
    drop? =
      case cluster.partition_filter do
        nil -> false
        fun -> fun.(from_id, to_id) == :drop
      end

    if drop? do
      %{cluster | dropped: cluster.dropped + 1}
    else
      %{cluster | queue: :queue.in({to_id, msg}, cluster.queue)}
    end
  end

  @doc "Active peers (as `Peer.t()` lists) for every node in the cluster."
  @spec active_views(t()) :: %{peer_id() => [Peer.t()]}
  def active_views(%__MODULE__{nodes: nodes}) do
    Map.new(nodes, fn {id, state} -> {id, State.active_peers(state)} end)
  end

  @doc "Passive peers for every node in the cluster."
  @spec passive_views(t()) :: %{peer_id() => [Peer.t()]}
  def passive_views(%__MODULE__{nodes: nodes}) do
    Map.new(nodes, fn {id, state} -> {id, State.passive_peers(state)} end)
  end

  @doc "Get the State for a specific peer id."
  @spec get_state(t(), peer_id()) :: State.t() | nil
  def get_state(%__MODULE__{nodes: nodes}, peer_id), do: Map.get(nodes, peer_id)
end
