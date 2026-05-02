defmodule HyParView.Protocol do
  @moduledoc """
  Wire format for HyParView messages.

  ## Layout

      <<magic::32, version::8, payload::binary>>

    * `magic` — `0x48504956` (`"HPIV"` in ASCII). Distinguishes our framing
      from arbitrary `:erlang.term_to_binary/1` output and helps detect
      misrouted bytes early.
    * `version` — protocol version, currently `1`. Bumping this is reserved
      for backwards-incompatible wire changes; receivers reject unsupported
      versions explicitly.
    * `payload` — `:erlang.term_to_binary/2` of the message struct, decoded
      with the `:safe` option to forbid creation of new atoms.

  Versioning is included from day 1 so that future format changes can be
  rolled out without silently corrupting running clusters.

  ## Safety

  Decoding uses `:erlang.binary_to_term(payload, [:safe])`, which rejects
  atoms the VM has not previously seen. This means peer identifiers should
  not be arbitrary atoms — see `HyParView.Peer` for guidance.
  """

  alias HyParView.Messages

  alias HyParView.Messages.{
    Disconnect,
    ForwardJoin,
    Join,
    Neighbor,
    NeighborReply,
    Shuffle,
    ShuffleReply
  }

  @magic 0x48504956
  @version 1

  @valid_modules [Join, ForwardJoin, Neighbor, NeighborReply, Disconnect, Shuffle, ShuffleReply]

  @typedoc "Encoded wire bytes."
  @type wire :: binary()

  @typedoc "Reasons decoding may fail."
  @type decode_error ::
          :bad_magic
          | {:unsupported_version, non_neg_integer()}
          | :unsafe_payload
          | :unknown_message_type

  @doc """
  Encode a message struct to its wire form.

  Raises `FunctionClauseError` if `message` is not one of the recognised
  message structs in `HyParView.Messages`.

  ## Examples

      iex> peer = HyParView.Peer.new("n1", :addr)
      iex> msg = %HyParView.Messages.Join{new_peer: peer}
      iex> bin = HyParView.Protocol.encode(msg)
      iex> HyParView.Protocol.decode(bin)
      {:ok, %HyParView.Messages.Join{new_peer: %HyParView.Peer{id: "n1", address: :addr}}}
  """
  @spec encode(Messages.t()) :: wire()
  def encode(%mod{} = message) when mod in @valid_modules do
    payload = :erlang.term_to_binary(message, minor_version: 2)
    <<@magic::32, @version::8, payload::binary>>
  end

  @doc """
  Decode wire bytes back into a message struct.

  Returns `{:ok, message}` on success, or `{:error, reason}` for any
  malformed, unsupported, or unsafe input.
  """
  @spec decode(binary()) :: {:ok, Messages.t()} | {:error, decode_error()}
  def decode(<<@magic::32, @version::8, payload::binary>>) do
    case safe_term_decode(payload) do
      {:ok, %mod{} = message} when mod in @valid_modules -> {:ok, message}
      {:ok, _other} -> {:error, :unknown_message_type}
      {:error, _} = err -> err
    end
  end

  def decode(<<@magic::32, version::8, _rest::binary>>),
    do: {:error, {:unsupported_version, version}}

  def decode(_), do: {:error, :bad_magic}

  @spec safe_term_decode(binary()) :: {:ok, term()} | {:error, :unsafe_payload}
  defp safe_term_decode(bin) do
    {:ok, :erlang.binary_to_term(bin, [:safe])}
  rescue
    ArgumentError -> {:error, :unsafe_payload}
  end
end
