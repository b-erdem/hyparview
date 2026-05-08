defmodule HyParView.State do
  @moduledoc """
  Pure functional core of the HyParView protocol.

  This module owns all membership state (`active` and `passive` views) and
  exposes side-effect-free transition functions. No processes, no IO, no
  timers, no transport — only the protocol's invariants and view bookkeeping.

  Higher layers (`HyParView.Server`) drive the state machine, handling
  timers and transport. Power users who want to drive the protocol
  themselves can use this module directly.

  ## Actions

  Transition functions return `{state, actions}`. Each action is a directive
  for the caller to execute:

    * `{:notify_up, peer}` — `peer` was added to the active view.
    * `{:notify_down, peer}` — `peer` was removed from the active view.
    * `{:send, peer, message}` — send `message` to `peer` over the transport.

  ## Invariants

  Every public function preserves these invariants:

    1. `|active| <= config.active_view_size`
    2. `|passive| <= config.passive_view_size`
    3. `active` and `passive` are disjoint (no peer in both views)
    4. `self` is in neither view
    5. Eviction from the active view always demotes the evicted peer to
       the passive view (it is never silently dropped)

  These are checked by property tests in `test/hyparview/state_test.exs`.
  """

  alias HyParView.{Config, Messages, Peer}

  alias HyParView.Messages.{
    Disconnect,
    ForwardJoin,
    Join,
    Neighbor,
    NeighborReply,
    Shuffle,
    ShuffleReply
  }

  @typedoc "Internal RNG state from `:rand`."
  @opaque rng :: :rand.state()

  @typedoc """
  Tracks an in-flight repair attempt: the set of passive peer ids that
  have been chosen (and possibly rejected) NEIGHBOR targets in the
  current repair cycle, so retries don't re-pick them. `nil` means no
  repair is in progress.
  """
  @type pending_repair :: MapSet.t(Peer.id()) | nil

  @typedoc """
  Set of peer ids that were recently declared lost via
  `connection_lost/2` and that we therefore must NOT re-add via a
  late `NEIGHBOR_REPLY`. This is a deny-list, not an allow-list:
  most replies are accepted, only those from a *just-evicted* peer
  are rejected.

  Why a deny-list rather than an allow-list:

  An allow-list (e.g. "only accept replies from peers we're currently
  awaiting") would also drop legitimate replies that the protocol
  relies on for *non-repair* paths — most importantly the
  add-active-with-handshake path during FORWARD_JOIN at TTL=0,
  whose reply is what restores symmetry when an eviction-during-
  handshake race has briefly desynchronised the two views.

  Lifecycle:

    * `connection_lost/2` adds the lost peer's id.
    * The set is cleared on `tick_shuffle/1` — the H1 race window is
      narrow (the gap between connection_lost and the in-flight
      reply landing); by the time the next shuffle round fires
      whatever replies were going to arrive have arrived.
    * `add_to_active/2` removes a peer's id when they're added back
      legitimately via JOIN / FORWARD_JOIN / Neighbor request — so a
      peer that was lost and then properly re-joined isn't blocked
      from later NeighborReply traffic.
  """
  @type recently_lost :: MapSet.t(Peer.id())

  @typedoc "An action the caller must execute."
  @type action ::
          {:notify_up, Peer.t()}
          | {:notify_down, Peer.t()}
          | {:send, Peer.t(), Messages.t()}

  @typedoc "A list of actions returned from a transition function."
  @type actions :: [action()]

  @typedoc "Internal protocol state for one node."
  @type t :: %__MODULE__{
          self: Peer.t(),
          config: Config.t(),
          active: %{Peer.id() => Peer.t()},
          passive: %{Peer.id() => Peer.t()},
          rng: rng(),
          pending_repair: pending_repair(),
          recently_lost: recently_lost()
        }

  @enforce_keys [:self, :config, :active, :passive, :rng]
  defstruct [
    :self,
    :config,
    :active,
    :passive,
    :rng,
    pending_repair: nil,
    recently_lost: %MapSet{}
  ]

  @doc """
  Build a new state for the local node `self_peer`.

  Options:

    * `:rng_seed` — a `:rand.seed/1` algorithm seed (a 3-tuple of integers,
      or any value `:rand.seed_s/2` accepts). Used to make tests fully
      deterministic. Defaults to a fresh seed via `:rand.seed_s(:default)`.

  ## Examples

      iex> alias HyParView.{Config, Peer, State}
      iex> state = State.new(Peer.new("self", :addr), Config.new())
      iex> State.active_size(state)
      0
      iex> State.passive_size(state)
      0
  """
  @spec new(Peer.t(), Config.t(), keyword()) :: t()
  def new(%Peer{} = self_peer, %Config{} = config, opts \\ []) do
    rng =
      case Keyword.get(opts, :rng_seed) do
        nil -> :rand.seed_s(:default)
        seed -> :rand.seed_s(:default, seed)
      end

    %__MODULE__{
      self: self_peer,
      config: config,
      active: %{},
      passive: %{},
      rng: rng,
      pending_repair: nil,
      recently_lost: MapSet.new()
    }
  end

  # ── Inspectors ─────────────────────────────────────────────────────────

  @doc "List of peers in the active view, in undefined order."
  @spec active_peers(t()) :: [Peer.t()]
  def active_peers(%__MODULE__{active: a}), do: Map.values(a)

  @doc "List of peers in the passive view, in undefined order."
  @spec passive_peers(t()) :: [Peer.t()]
  def passive_peers(%__MODULE__{passive: p}), do: Map.values(p)

  @doc "Cardinality of the active view."
  @spec active_size(t()) :: non_neg_integer()
  def active_size(%__MODULE__{active: a}), do: map_size(a)

  @doc "Cardinality of the passive view."
  @spec passive_size(t()) :: non_neg_integer()
  def passive_size(%__MODULE__{passive: p}), do: map_size(p)

  @doc "Whether `peer` is currently in the active view (compared by id)."
  @spec in_active?(t(), Peer.t()) :: boolean()
  def in_active?(%__MODULE__{active: a}, %Peer{id: id}), do: Map.has_key?(a, id)

  @doc "Whether `peer` is currently in the passive view (compared by id)."
  @spec in_passive?(t(), Peer.t()) :: boolean()
  def in_passive?(%__MODULE__{passive: p}, %Peer{id: id}), do: Map.has_key?(p, id)

  @doc "Whether `peer` is the local node (compared by id)."
  @spec self?(t(), Peer.t()) :: boolean()
  def self?(%__MODULE__{self: %Peer{id: id}}, %Peer{id: id}), do: true
  def self?(%__MODULE__{}, %Peer{}), do: false

  # ── Transitions ───────────────────────────────────────────────────────

  @doc """
  Add `peer` to the active view.

  No-op if `peer` is the local node or is already in the active view. If
  the active view is full, evicts a random peer (sending it to the passive
  view) before adding `peer`.

  Returns `{state, actions}`.
  """
  @spec add_to_active(t(), Peer.t()) :: {t(), actions()}
  def add_to_active(%__MODULE__{} = state, %Peer{} = peer) do
    cond do
      self?(state, peer) ->
        {state, []}

      in_active?(state, peer) ->
        {state, []}

      true ->
        state = remove_from_passive_silent(state, peer)
        {state, evict_actions} = maybe_evict_active(state)
        active = Map.put(state.active, peer.id, peer)
        # If this peer was previously in `recently_lost` (e.g.
        # connection was lost briefly, then re-established via JOIN
        # / FORWARD_JOIN / NEIGHBOR), legitimate re-adds clear the
        # entry so future replies from them aren't blocked.
        recently_lost = MapSet.delete(state.recently_lost, peer.id)
        state = %{state | active: active, recently_lost: recently_lost}
        {state, evict_actions ++ [{:notify_up, peer}]}
    end
  end

  @doc """
  Add `peer` to the passive view.

  No-op if `peer` is the local node, is already in the passive view, or is
  in the active view (the disjoint-views invariant). If the passive view is
  full, evicts a random peer (silently — no notification, since passive
  peers are not connected).
  """
  @spec add_to_passive(t(), Peer.t()) :: {t(), actions()}
  def add_to_passive(%__MODULE__{} = state, %Peer{} = peer) do
    cond do
      self?(state, peer) -> {state, []}
      in_active?(state, peer) -> {state, []}
      in_passive?(state, peer) -> {state, []}
      true -> {do_add_to_passive(state, peer), []}
    end
  end

  @doc """
  Remove `peer` from the active view.

  Emits `{:notify_down, peer}` if `peer` was actually present.
  """
  @spec remove_from_active(t(), Peer.t()) :: {t(), actions()}
  def remove_from_active(%__MODULE__{active: a} = state, %Peer{id: id} = peer) do
    case Map.pop(a, id) do
      {nil, _} -> {state, []}
      {removed, new_active} -> {%{state | active: new_active}, [{:notify_down, removed || peer}]}
    end
  end

  @doc """
  Remove `peer` from the passive view (silent — no notification).
  """
  @spec remove_from_passive(t(), Peer.t()) :: {t(), actions()}
  def remove_from_passive(%__MODULE__{} = state, %Peer{} = peer) do
    {remove_from_passive_silent(state, peer), []}
  end

  # ── External events (initiate_join, connection_lost) ─────────────────

  @doc """
  Initiate a JOIN to the cluster via `contact`.

  Adds `contact` to the local active view (the joiner-side TCP connection
  establishment described in paper §4.2) and emits a `Join` message
  carrying the local node's identity.
  """
  @spec initiate_join(t(), Peer.t()) :: {t(), actions()}
  def initiate_join(%__MODULE__{} = state, %Peer{} = contact) do
    {state, add_actions} = add_to_active(state, contact)
    msg = %Join{new_peer: state.self}
    {state, add_actions ++ [{:send, contact, msg}]}
  end

  # ── Message handling ──────────────────────────────────────────────────

  @doc """
  Process an incoming protocol message.

  The single dispatch entry point for `HyParView.Messages` types. Returns
  `{state, actions}` where `actions` is a list of directives the caller
  must execute (sends, notifications).

  Handles all seven message types defined in the paper:
  `Join`, `ForwardJoin`, `Neighbor`, `NeighborReply`, `Disconnect`,
  `Shuffle`, `ShuffleReply`.
  """
  @spec handle_message(t(), Messages.t()) :: {t(), actions()}
  def handle_message(%__MODULE__{} = state, %Join{new_peer: new_peer}) do
    handle_join(state, new_peer)
  end

  def handle_message(%__MODULE__{} = state, %ForwardJoin{} = msg) do
    handle_forward_join(state, msg)
  end

  def handle_message(%__MODULE__{} = state, %Neighbor{} = msg) do
    handle_neighbor_request(state, msg)
  end

  def handle_message(%__MODULE__{} = state, %NeighborReply{} = msg) do
    handle_neighbor_reply(state, msg)
  end

  def handle_message(%__MODULE__{} = state, %Disconnect{peer: sender}) do
    handle_disconnect(state, sender)
  end

  def handle_message(%__MODULE__{} = state, %Shuffle{} = msg) do
    handle_shuffle(state, msg)
  end

  def handle_message(%__MODULE__{} = state, %ShuffleReply{sample: sample}) do
    {merge_into_passive(state, sample), []}
  end

  @doc """
  Periodic shuffle tick — initiate a SHUFFLE exchange (paper §4.4).

  Builds an exchange list of `[self | ka active samples ++ kp passive samples]`
  and sends it to a random active peer with TTL `shuffle_ttl`. The receiver
  forwards the request along an active-view random walk until TTL=0 or
  active size <=1, at which point the terminal node replies with a same-
  size sample of its own passive view.

  No-op if the active view is empty.
  """
  @spec tick_shuffle(t()) :: {t(), actions()}
  def tick_shuffle(%__MODULE__{} = state) do
    # Clear the recently-lost set on every shuffle round: the H1
    # in-flight-reply window is much shorter than the shuffle
    # interval, so by the time a shuffle fires whatever stale
    # `NEIGHBOR_REPLY` was going to arrive has either arrived or
    # been lost to network. Bounding `recently_lost`'s lifetime here
    # keeps the deny-list from growing unboundedly across long
    # uptime.
    state = %{state | recently_lost: MapSet.new()}

    case map_size(state.active) do
      0 ->
        {state, []}

      _ ->
        {sample, state} = build_shuffle_sample(state)
        {target, state} = pick_random_active(state)

        msg = %Shuffle{
          origin: state.self,
          sender: state.self,
          sample: sample,
          ttl: state.config.shuffle_ttl
        }

        {state, [{:send, target, msg}]}
    end
  end

  @doc """
  Signal that the TCP connection to `peer` has been lost (failure detector).

  Per the paper §4.5, a peer removed from the active view due to *failure*
  (TCP break, blocking) is **not** demoted to the passive view — only
  gracefully-disconnected (via `Disconnect`) or evicted peers go to passive.

  This function removes `peer` from the active view, then attempts repair
  by sending a `Neighbor` request to a random passive peer.
  """
  @spec connection_lost(t(), Peer.t()) :: {t(), actions()}
  def connection_lost(%__MODULE__{} = state, %Peer{} = peer) do
    if in_active?(state, peer) do
      # Mark the peer as recently lost so a stale, in-flight
      # `NEIGHBOR_REPLY` from them doesn't re-add a dead peer (H1
      # race in #1). The marker is cleared on the next
      # `tick_shuffle/1`, well past any plausible in-flight-reply
      # window.
      state = %{state | recently_lost: MapSet.put(state.recently_lost, peer.id)}
      {state, [{:notify_down, _} = down]} = remove_from_active(state, peer)
      {state, repair_actions} = attempt_repair(state)
      {state, [down | repair_actions]}
    else
      {state, []}
    end
  end

  # JOIN — paper §4.2 (Algorithm 1, lines 347-352):
  # The contact node adds the joiner to its active view (evicting if full),
  # then sends FORWARD_JOIN(newNode, ARWL, self) to every other active peer.
  @spec handle_join(t(), Peer.t()) :: {t(), actions()}
  defp handle_join(state, new_peer) do
    {state, add_actions} = add_to_active(state, new_peer)

    forward_msg = %ForwardJoin{
      new_peer: new_peer,
      ttl: state.config.arwl,
      sender: state.self
    }

    forward_actions =
      state.active
      |> Map.values()
      |> Enum.reject(fn %Peer{id: id} -> id == new_peer.id end)
      |> Enum.map(fn target -> {:send, target, forward_msg} end)

    {state, add_actions ++ forward_actions}
  end

  # FORWARD_JOIN — paper §4.2 (Algorithm 1, lines 354-361 and prose §4.2):
  #
  # On receipt of `ForwardJoin(newPeer, ttl, sender)`:
  #
  #   i.   if `ttl == 0` OR `|active| <= 1` (only the sender is present):
  #          add newPeer to the active view and stop forwarding.
  #          Note: pseudocode says `|active| == 0`; prose says "equal to one";
  #          we follow the prose (matches Partisan; with size 1 we can't pick
  #          a forward target other than the sender, so termination here is
  #          the only sensible behaviour).
  #
  #   ii.  if `ttl == prwl`: insert newPeer into the passive view (and
  #          continue with steps iii-iv).
  #
  #   iii. decrement ttl.
  #
  #   iv.  if newPeer was not just added to active, forward FORWARD_JOIN
  #          to a random active peer != sender.
  @spec handle_forward_join(t(), ForwardJoin.t()) :: {t(), actions()}
  defp handle_forward_join(state, %ForwardJoin{
         new_peer: new_peer,
         ttl: ttl,
         sender: sender
       }) do
    if ttl == 0 or active_size(state) <= 1 do
      add_active_with_handshake(state, new_peer)
    else
      {state, passive_actions} =
        if ttl == state.config.prwl,
          do: add_to_passive(state, new_peer),
          else: {state, []}

      case pick_forward_target(state, sender) do
        {nil, state} ->
          {state, passive_actions}

        {target, state} ->
          forward = %ForwardJoin{new_peer: new_peer, ttl: ttl - 1, sender: state.self}
          {state, passive_actions ++ [{:send, target, forward}]}
      end
    end
  end

  # Add `peer` to active and, if a new addition occurred, send a high-priority
  # NEIGHBOR so the peer reciprocally adds us. Bridges the protocol's reliance
  # on TCP-establishment symmetry that we have to model explicitly without
  # a real socket layer (paper §4.1: "links in the overlay are symmetric").
  @spec add_active_with_handshake(t(), Peer.t()) :: {t(), actions()}
  defp add_active_with_handshake(state, peer) do
    {state, add_actions} = add_to_active(state, peer)

    handshake_actions =
      if peer_was_added?(add_actions, peer) do
        [{:send, peer, %Neighbor{peer: state.self, priority: :high}}]
      else
        []
      end

    {state, add_actions ++ handshake_actions}
  end

  @spec peer_was_added?(actions(), Peer.t()) :: boolean()
  defp peer_was_added?(actions, %Peer{id: id}) do
    Enum.any?(actions, fn
      {:notify_up, %Peer{id: ^id}} -> true
      _ -> false
    end)
  end

  # Pick a random active peer that is not `excluded`. Returns `{nil, state}`
  # if no such peer exists.
  @spec pick_forward_target(t(), Peer.t()) :: {Peer.t() | nil, t()}
  defp pick_forward_target(state, %Peer{id: excluded_id}) do
    candidates =
      state.active
      |> Map.values()
      |> Enum.reject(fn %Peer{id: id} -> id == excluded_id end)

    case candidates do
      [] ->
        {nil, state}

      list ->
        {n, rng} = :rand.uniform_s(length(list), state.rng)
        {Enum.at(list, n - 1), %{state | rng: rng}}
    end
  end

  # NEIGHBOR (request) — paper §4.3.
  # High-priority requests must be accepted (evicting if needed); low-priority
  # requests are accepted only if the active view has free space.
  @spec handle_neighbor_request(t(), Neighbor.t()) :: {t(), actions()}
  defp handle_neighbor_request(state, %Neighbor{peer: requester, priority: priority}) do
    cond do
      self?(state, requester) ->
        {state, []}

      in_active?(state, requester) ->
        # Already in active — confirm with an accept.
        reply = %NeighborReply{peer: state.self, accepted?: true}
        {state, [{:send, requester, reply}]}

      priority == :high ->
        accept_neighbor(state, requester)

      active_size(state) >= state.config.active_view_size ->
        reply = %NeighborReply{peer: state.self, accepted?: false}
        {state, [{:send, requester, reply}]}

      true ->
        accept_neighbor(state, requester)
    end
  end

  @spec accept_neighbor(t(), Peer.t()) :: {t(), actions()}
  defp accept_neighbor(state, requester) do
    {state, add_actions} = add_to_active(state, requester)
    reply = %NeighborReply{peer: state.self, accepted?: true}
    {state, add_actions ++ [{:send, requester, reply}]}
  end

  # NEIGHBOR_REPLY — accepted: clear pending repair and add the replier to
  # active view. Rejected: track the rejecter and try another passive peer.
  #
  # Both branches reject replies from peers in `state.recently_lost`
  # — i.e. peers that were just declared lost via
  # `connection_lost/2`. A late reply from such a peer would
  # otherwise re-add a dead peer to the active view (the H1 race in
  # #1).
  #
  # Replies from any *other* peer are accepted unconditionally. This
  # preserves the symmetry-restoring behaviour the protocol relies on
  # for non-repair NEIGHBOR exchanges (e.g. the
  # add-active-with-handshake path during FORWARD_JOIN at TTL=0,
  # whose reply re-syncs an eviction-during-handshake race).
  @spec handle_neighbor_reply(t(), NeighborReply.t()) :: {t(), actions()}
  defp handle_neighbor_reply(state, %NeighborReply{peer: replier, accepted?: true}) do
    if MapSet.member?(state.recently_lost, replier.id) do
      {state, []}
    else
      state = %{state | pending_repair: nil}
      {state, add_actions} = add_to_active(state, replier)
      {state, add_actions}
    end
  end

  defp handle_neighbor_reply(state, %NeighborReply{peer: rejecter, accepted?: false}) do
    if MapSet.member?(state.recently_lost, rejecter.id) do
      {state, []}
    else
      previous = state.pending_repair || MapSet.new()
      tried = MapSet.put(previous, rejecter.id)
      state = %{state | pending_repair: tried}
      attempt_repair_excluding(state, tried)
    end
  end

  # DISCONNECT — paper §4.2 Algorithm 1 + §4.3 (failure-detection repair):
  # remove sender from active, attempt repair *before* demoting, then
  # add sender to passive. This ordering ensures the just-disconnected
  # peer isn't picked as a NEIGHBOR target in the same step.
  @spec handle_disconnect(t(), Peer.t()) :: {t(), actions()}
  defp handle_disconnect(state, sender) do
    if in_active?(state, sender) do
      {state, [{:notify_down, _} = down]} = remove_from_active(state, sender)
      {state, repair_actions} = attempt_repair(state)
      {state, _} = add_to_passive(state, sender)
      {state, [down | repair_actions]}
    else
      {state, []}
    end
  end

  # SHUFFLE — paper §4.4. Decrement TTL; if walk hasn't terminated, forward
  # to a random active peer != sender. Otherwise, merge the received sample
  # into our passive view and reply with a same-size sample of our passive.
  @spec handle_shuffle(t(), Shuffle.t()) :: {t(), actions()}
  defp handle_shuffle(state, %Shuffle{
         origin: origin,
         sample: sample,
         ttl: ttl,
         sender: sender
       }) do
    decremented = ttl - 1

    if decremented > 0 and active_size(state) > 1 do
      case pick_forward_target(state, sender) do
        {nil, state} ->
          terminate_shuffle(state, origin, sample)

        {target, state} ->
          forward = %Shuffle{
            origin: origin,
            sample: sample,
            ttl: decremented,
            sender: state.self
          }

          {state, [{:send, target, forward}]}
      end
    else
      terminate_shuffle(state, origin, sample)
    end
  end

  @spec terminate_shuffle(t(), Peer.t(), [Peer.t()]) :: {t(), actions()}
  defp terminate_shuffle(state, origin, received_sample) do
    desired_size = length(received_sample)
    {reply_sample, state} = sample_from_map(state, state.passive, desired_size)
    state = merge_into_passive(state, received_sample)
    reply = %ShuffleReply{sample: reply_sample}
    {state, [{:send, origin, reply}]}
  end

  @spec build_shuffle_sample(t()) :: {[Peer.t()], t()}
  defp build_shuffle_sample(state) do
    {active_sample, state} =
      sample_from_map(state, state.active, state.config.shuffle_active_count)

    {passive_sample, state} =
      sample_from_map(state, state.passive, state.config.shuffle_passive_count)

    {[state.self | active_sample ++ passive_sample], state}
  end

  # Pick up to `n` random distinct values from `peer_map`. Returns the
  # sampled list and the updated state (with advanced RNG).
  @spec sample_from_map(t(), %{Peer.id() => Peer.t()}, non_neg_integer()) ::
          {[Peer.t()], t()}
  defp sample_from_map(state, peer_map, n) when n >= 0 do
    values = Map.values(peer_map)

    if length(values) <= n do
      {values, state}
    else
      {shuffled, rng} = shuffle_with_rng(values, state.rng)
      {Enum.take(shuffled, n), %{state | rng: rng}}
    end
  end

  # Fisher-Yates shuffle using injected RNG (so tests are deterministic).
  @spec shuffle_with_rng([term()], rng()) :: {[term()], rng()}
  defp shuffle_with_rng(list, rng) do
    {keyed, rng} =
      Enum.map_reduce(list, rng, fn item, r ->
        {k, r2} = :rand.uniform_s(1_000_000_000, r)
        {{k, item}, r2}
      end)

    shuffled = keyed |> Enum.sort() |> Enum.map(&elem(&1, 1))
    {shuffled, rng}
  end

  # Merge a list of peers into the passive view, respecting all invariants
  # (skip self, skip active members, skip existing passive members; evict
  # randomly when full — TODO: prefer evicting peers we sent to the same
  # peer, per paper §4.4).
  @spec merge_into_passive(t(), [Peer.t()]) :: t()
  defp merge_into_passive(state, peers) do
    Enum.reduce(peers, state, fn peer, s ->
      {s, _} = add_to_passive(s, peer)
      s
    end)
  end

  @spec pick_random_active(t()) :: {Peer.t(), t()}
  defp pick_random_active(state) do
    {peer, rng} = pick_random_value(state.active, state.rng)
    {peer, %{state | rng: rng}}
  end

  # Initiate a fresh repair attempt (resets the tried set).
  @spec attempt_repair(t()) :: {t(), actions()}
  defp attempt_repair(state) do
    state = %{state | pending_repair: nil}
    attempt_repair_excluding(state, MapSet.new())
  end

  # Pick a passive peer not in `excludes`, send a NEIGHBOR. Priority is
  # `:high` if active is empty, `:low` otherwise (paper §4.3).
  @spec attempt_repair_excluding(t(), MapSet.t(Peer.id())) :: {t(), actions()}
  defp attempt_repair_excluding(state, excludes) do
    candidates =
      state.passive
      |> Map.values()
      |> Enum.reject(fn %Peer{id: id} -> MapSet.member?(excludes, id) end)

    case candidates do
      [] ->
        {%{state | pending_repair: nil}, []}

      list ->
        {n, rng} = :rand.uniform_s(length(list), state.rng)
        target = Enum.at(list, n - 1)
        tried = MapSet.put(excludes, target.id)
        state = %{state | rng: rng, pending_repair: tried}
        priority = if active_size(state) == 0, do: :high, else: :low
        msg = %Neighbor{peer: state.self, priority: priority}
        {state, [{:send, target, msg}]}
    end
  end

  # ── Internal helpers ──────────────────────────────────────────────────

  @spec do_add_to_passive(t(), Peer.t()) :: t()
  defp do_add_to_passive(%__MODULE__{} = state, %Peer{} = peer) do
    cond do
      state.config.passive_view_size == 0 ->
        # Bound is zero; the passive view simply doesn't exist on this node.
        state

      passive_size(state) >= state.config.passive_view_size ->
        state = drop_random_passive(state)
        %{state | passive: Map.put(state.passive, peer.id, peer)}

      true ->
        %{state | passive: Map.put(state.passive, peer.id, peer)}
    end
  end

  @spec remove_from_passive_silent(t(), Peer.t()) :: t()
  defp remove_from_passive_silent(%__MODULE__{} = state, %Peer{id: id}) do
    %{state | passive: Map.delete(state.passive, id)}
  end

  @spec maybe_evict_active(t()) :: {t(), actions()}
  defp maybe_evict_active(%__MODULE__{} = state) do
    if active_size(state) >= state.config.active_view_size do
      evict_random_active(state)
    else
      {state, []}
    end
  end

  @spec evict_random_active(t()) :: {t(), actions()}
  defp evict_random_active(%__MODULE__{} = state) do
    {evicted, rng} = pick_random_value(state.active, state.rng)
    active = Map.delete(state.active, evicted.id)
    state = %{state | active: active, rng: rng}
    state = do_add_to_passive(state, evicted)
    disconnect = %Disconnect{peer: state.self}
    {state, [{:send, evicted, disconnect}, {:notify_down, evicted}]}
  end

  @spec drop_random_passive(t()) :: t()
  defp drop_random_passive(%__MODULE__{} = state) do
    {dropped, rng} = pick_random_value(state.passive, state.rng)
    %{state | passive: Map.delete(state.passive, dropped.id), rng: rng}
  end

  @spec pick_random_value(%{Peer.id() => Peer.t()}, rng()) :: {Peer.t(), rng()}
  defp pick_random_value(map, rng) when map_size(map) > 0 do
    values = Map.values(map)
    {n, rng} = :rand.uniform_s(length(values), rng)
    {Enum.at(values, n - 1), rng}
  end
end
