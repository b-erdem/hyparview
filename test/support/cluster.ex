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
  defstruct nodes: %{}, queue: nil, step_count: 0, dropped: 0

  @type peer_id :: Peer.id()
  @type queued :: {peer_id(), HyParView.Messages.t()}

  @type t :: %__MODULE__{
          nodes: %{peer_id() => State.t()},
          queue: :queue.queue(queued()),
          step_count: non_neg_integer(),
          dropped: non_neg_integer()
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
    apply_actions(cluster, actions)
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
            {:ok, apply_actions(cluster, actions)}
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

  defp apply_actions(cluster, actions) do
    Enum.reduce(actions, cluster, fn
      {:send, %Peer{id: id}, msg}, c -> enqueue(c, id, msg)
      {:notify_up, _peer}, c -> c
      {:notify_down, _peer}, c -> c
    end)
  end

  defp enqueue(%__MODULE__{queue: q} = cluster, to_id, msg) do
    %{cluster | queue: :queue.in({to_id, msg}, q)}
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
