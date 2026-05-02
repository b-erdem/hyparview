defmodule HyParView.Test.Generators do
  @moduledoc """
  StreamData generators for property-based tests.

  Generators here produce *protocol-valid* values: bounded TTLs, sane peer
  identifiers, and so on. Tests should compose these rather than building
  their own from scratch.
  """

  alias HyParView.Messages.{
    Disconnect,
    ForwardJoin,
    Join,
    Neighbor,
    NeighborReply,
    Shuffle,
    ShuffleReply
  }

  alias HyParView.Peer

  @doc "A peer with a binary id and `{ip-string, port}` address."
  @spec peer() :: StreamData.t(Peer.t())
  def peer do
    {StreamData.string(:alphanumeric, min_length: 1, max_length: 16),
     StreamData.integer(1024..65_535)}
    |> StreamData.tuple()
    |> StreamData.map(fn {id, port} -> Peer.new(id, {"10.0.0.1", port}) end)
  end

  @doc "A list of distinct peers (deduped by id)."
  @spec peer_list(keyword()) :: StreamData.t([Peer.t()])
  def peer_list(opts \\ []) do
    max = Keyword.get(opts, :max_length, 10)

    peer()
    |> StreamData.list_of(max_length: max)
    |> StreamData.map(&Enum.uniq_by(&1, fn %Peer{id: id} -> id end))
  end

  @spec join() :: StreamData.t(Join.t())
  def join, do: StreamData.map(peer(), &%Join{new_peer: &1})

  @spec forward_join() :: StreamData.t(ForwardJoin.t())
  def forward_join do
    {peer(), peer(), StreamData.integer(0..16)}
    |> StreamData.tuple()
    |> StreamData.map(fn {new_peer, sender, ttl} ->
      %ForwardJoin{new_peer: new_peer, ttl: ttl, sender: sender}
    end)
  end

  @spec neighbor() :: StreamData.t(Neighbor.t())
  def neighbor do
    {peer(), StreamData.member_of([:high, :low])}
    |> StreamData.tuple()
    |> StreamData.map(fn {p, priority} -> %Neighbor{peer: p, priority: priority} end)
  end

  @spec neighbor_reply() :: StreamData.t(NeighborReply.t())
  def neighbor_reply do
    {peer(), StreamData.boolean()}
    |> StreamData.tuple()
    |> StreamData.map(fn {p, accepted?} -> %NeighborReply{peer: p, accepted?: accepted?} end)
  end

  @spec disconnect() :: StreamData.t(Disconnect.t())
  def disconnect, do: StreamData.map(peer(), &%Disconnect{peer: &1})

  @spec shuffle() :: StreamData.t(Shuffle.t())
  def shuffle do
    {peer(), peer(), peer_list(max_length: 8), StreamData.integer(0..16)}
    |> StreamData.tuple()
    |> StreamData.map(fn {origin, sender, sample, ttl} ->
      %Shuffle{origin: origin, sender: sender, sample: sample, ttl: ttl}
    end)
  end

  @spec shuffle_reply() :: StreamData.t(ShuffleReply.t())
  def shuffle_reply do
    StreamData.map(peer_list(max_length: 8), &%ShuffleReply{sample: &1})
  end

  @doc "Any HyParView wire message."
  @spec any_message() :: StreamData.t(HyParView.Messages.t())
  def any_message do
    StreamData.one_of([
      join(),
      forward_join(),
      neighbor(),
      neighbor_reply(),
      disconnect(),
      shuffle(),
      shuffle_reply()
    ])
  end
end
