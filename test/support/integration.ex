defmodule HyParView.Test.Integration do
  @moduledoc """
  Helpers for multi-process integration tests using `HyParView.Transport.Test`.
  """

  alias HyParView.Peer

  @doc """
  Build a unique peer for use in tests. The address is `make_ref/0` so each
  invocation produces a peer with a globally-unique address (safe across
  parallel test runs sharing the test transport registry).
  """
  @spec unique_peer(String.t()) :: Peer.t()
  def unique_peer(id_prefix) do
    suffix = System.unique_integer([:positive])
    Peer.new("#{id_prefix}-#{suffix}", make_ref())
  end

  @doc """
  Poll `condition_fn` until it returns truthy or `timeout_ms` elapses.

  Returns `:ok` on success, `{:timeout, last_value}` otherwise. Polls every
  `interval` ms (default 10).
  """
  @spec wait_until(non_neg_integer(), (-> any()), pos_integer()) ::
          :ok | {:timeout, term()}
  def wait_until(timeout_ms, condition_fn, interval \\ 10)
  def wait_until(timeout, _fn, _) when timeout <= 0, do: {:timeout, nil}

  def wait_until(timeout, fun, interval) do
    case fun.() do
      result when result in [false, nil] ->
        Process.sleep(interval)
        wait_until(timeout - interval, fun, interval)

      _truthy ->
        :ok
    end
  end

  @doc """
  Poll all `pids` until the supplied predicate holds for every server's
  active view, or the timeout elapses. Returns `:ok` or `{:timeout, snapshot}`.
  """
  @spec wait_for_active(list(pid()), (list(Peer.t()) -> boolean()), non_neg_integer()) ::
          :ok | {:timeout, [list(Peer.t())]}
  def wait_for_active(pids, predicate, timeout_ms \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms

    do_wait(pids, predicate, deadline)
  end

  defp do_wait(pids, predicate, deadline) do
    snapshots = Enum.map(pids, &HyParView.active_view/1)

    cond do
      Enum.all?(snapshots, predicate) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        {:timeout, snapshots}

      true ->
        Process.sleep(10)
        do_wait(pids, predicate, deadline)
    end
  end
end
