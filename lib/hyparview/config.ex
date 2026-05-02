defmodule HyParView.Config do
  @moduledoc """
  Tunable parameters for a HyParView node.

  Defaults match the values used in the paper's evaluation (a 10,000-node
  PeerSim simulation, §5.1):

      | parameter             | default | paper symbol     |
      | --------------------- | ------- | ---------------- |
      | `:active_view_size`   |       5 | `log(N) + c`     |
      | `:passive_view_size`  |      30 | `K · (log(N)+c)` |
      | `:arwl`               |       6 | `ARWL`           |
      | `:prwl`               |       3 | `PRWL`           |
      | `:shuffle_active_count`  |    3 | `ka`             |
      | `:shuffle_passive_count` |    4 | `kp`             |
      | `:shuffle_interval`   |   30_000 ms | not specified |
      | `:shuffle_ttl`        |       6 | not specified, defaults to `ARWL` |

  The paper does not specify a value for the shuffle period or for the
  shuffle TTL; we default to 30 s and `ARWL` respectively, matching the
  Partisan reference implementation.
  """

  @typedoc "HyParView configuration."
  @type t :: %__MODULE__{
          active_view_size: pos_integer(),
          passive_view_size: non_neg_integer(),
          arwl: non_neg_integer(),
          prwl: non_neg_integer(),
          shuffle_active_count: non_neg_integer(),
          shuffle_passive_count: non_neg_integer(),
          shuffle_interval: pos_integer(),
          shuffle_ttl: non_neg_integer()
        }

  defstruct active_view_size: 5,
            passive_view_size: 30,
            arwl: 6,
            prwl: 3,
            shuffle_active_count: 3,
            shuffle_passive_count: 4,
            shuffle_interval: 30_000,
            shuffle_ttl: 6

  @doc """
  Build a config, overriding any defaults from the supplied options.

  Validates that `:prwl <= :arwl` and that all sizes are positive.

  ## Examples

      iex> HyParView.Config.new()
      %HyParView.Config{
        active_view_size: 5,
        passive_view_size: 30,
        arwl: 6,
        prwl: 3,
        shuffle_active_count: 3,
        shuffle_passive_count: 4,
        shuffle_interval: 30_000,
        shuffle_ttl: 6
      }

      iex> HyParView.Config.new(active_view_size: 4, arwl: 5, prwl: 2)
      %HyParView.Config{
        active_view_size: 4,
        passive_view_size: 30,
        arwl: 5,
        prwl: 2,
        shuffle_active_count: 3,
        shuffle_passive_count: 4,
        shuffle_interval: 30_000,
        shuffle_ttl: 6
      }
  """
  @spec new(keyword()) :: t()
  def new(opts \\ []) when is_list(opts) do
    config = struct!(__MODULE__, opts)
    validate!(config)
    config
  end

  @spec validate!(t()) :: :ok
  defp validate!(%__MODULE__{} = c) do
    if c.active_view_size < 1, do: raise(ArgumentError, ":active_view_size must be >= 1")
    if c.passive_view_size < 0, do: raise(ArgumentError, ":passive_view_size must be >= 0")

    if c.prwl > c.arwl,
      do: raise(ArgumentError, ":prwl must not exceed :arwl (PRWL <= ARWL invariant)")

    if c.shuffle_interval < 1, do: raise(ArgumentError, ":shuffle_interval must be >= 1ms")
    :ok
  end
end
