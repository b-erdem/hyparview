defmodule HyParView.Peer do
  @moduledoc """
  A peer in a HyParView overlay.

  A peer has two parts:

    * `:id` — opaque identity. Used for view membership checks and equality.
      The protocol compares peers exclusively by `id`.
    * `:address` — opaque transport address. Passed to the configured
      `HyParView.Transport` implementation when establishing a connection.

  Both fields are user-defined. They may be the same value if the user does
  not need to distinguish identity from address (for example, when the
  address itself uniquely identifies the peer).

  ## Choosing identifiers

  When peers are exchanged across nodes (which they are, via FORWARD_JOIN
  and SHUFFLE), the receiver decodes them with `:erlang.binary_to_term/2`
  in `:safe` mode. This rejects atoms the receiver has not previously seen.

  In practice that means **do not use freshly-minted atoms as `:id` or
  `:address`** for peers that may be sent to nodes that don't already
  know them — prefer binaries, integers, or tuples thereof. Atoms that are
  baked into the VM (such as module names or atoms used in the project's
  source code) are safe.
  """

  @enforce_keys [:id, :address]
  defstruct [:id, :address]

  @typedoc "Opaque peer identity used for membership comparisons."
  @type id :: term()

  @typedoc "Opaque transport address passed to `HyParView.Transport`."
  @type address :: term()

  @typedoc "A peer record."
  @type t :: %__MODULE__{id: id(), address: address()}

  @doc """
  Build a peer from `id` and `address`.

  ## Examples

      iex> HyParView.Peer.new("node-a", {"10.0.0.1", 4000})
      %HyParView.Peer{id: "node-a", address: {"10.0.0.1", 4000}}
  """
  @spec new(id(), address()) :: t()
  def new(id, address), do: %__MODULE__{id: id, address: address}

  @doc """
  Return `true` if two peers refer to the same node, compared by `:id`.

  ## Examples

      iex> a = HyParView.Peer.new("n1", {"10.0.0.1", 4000})
      iex> b = HyParView.Peer.new("n1", {"10.0.0.2", 4000})
      iex> HyParView.Peer.same?(a, b)
      true

      iex> a = HyParView.Peer.new("n1", :addr)
      iex> b = HyParView.Peer.new("n2", :addr)
      iex> HyParView.Peer.same?(a, b)
      false
  """
  @spec same?(t(), t()) :: boolean()
  def same?(%__MODULE__{id: id}, %__MODULE__{id: id}), do: true
  def same?(%__MODULE__{}, %__MODULE__{}), do: false
end
