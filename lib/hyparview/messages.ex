defmodule HyParView.Messages do
  @moduledoc """
  Wire messages defined in the HyParView paper (DSN 2007, Algorithm 1).

  Each message is a struct in this module's namespace so handlers can
  pattern-match cleanly. `HyParView.Protocol` handles binary encoding and
  decoding.

  The seven message types are:

    * `Join` — a new node asks its contact to admit it.
    * `ForwardJoin` — propagated through active views with a TTL, seeding
      both active and passive views along its random walk.
    * `Neighbor` — request to be promoted into the recipient's active view.
    * `NeighborReply` — explicit accept/reject for a `Neighbor` request.
    * `Disconnect` — graceful drop from the sender's active view.
    * `Shuffle` — periodic gossip of view samples for passive maintenance.
    * `ShuffleReply` — terminal node's response to a `Shuffle`.

  The paper does not specify an explicit `NeighborReply` message; we
  introduce one (matching the Partisan reference implementation) so the
  decision is conveyed without depending on TCP-level signaling.
  """

  alias HyParView.Peer

  defmodule Join do
    @moduledoc """
    JOIN — the new node sends this to its contact node to enter the overlay.
    Paper §4.2, Algorithm 1.
    """

    @enforce_keys [:new_peer]
    defstruct [:new_peer]

    @typedoc "A JOIN request carrying the new peer's identity."
    @type t :: %__MODULE__{new_peer: Peer.t()}
  end

  defmodule ForwardJoin do
    @moduledoc """
    FORWARD_JOIN — propagated through the active view by the contact node
    after a JOIN. Carries `ttl`, decremented at each hop.

    On receipt:

      * if `ttl == 0` or the recipient's active view has only the sender,
        the new peer is added to the recipient's active view (paper §4.2);
      * if `ttl == PRWL`, the new peer is added to the recipient's passive
        view *and* the request is still forwarded.

    Paper §4.2, Algorithm 1.
    """

    @enforce_keys [:new_peer, :ttl, :sender]
    defstruct [:new_peer, :ttl, :sender]

    @type t :: %__MODULE__{
            new_peer: Peer.t(),
            ttl: non_neg_integer(),
            sender: Peer.t()
          }
  end

  defmodule Neighbor do
    @moduledoc """
    NEIGHBOR — asks the recipient to add the sender to its active view.

    `:high` priority is sent when the sender's active view is empty: the
    recipient must accept (evicting if needed). `:low` priority may be
    rejected if the recipient's active view is full. Paper §4.3.
    """

    @typedoc "Priority of a NEIGHBOR request."
    @type priority :: :high | :low

    @enforce_keys [:peer, :priority]
    defstruct [:peer, :priority]

    @type t :: %__MODULE__{peer: Peer.t(), priority: priority()}
  end

  defmodule NeighborReply do
    @moduledoc """
    NEIGHBOR_REPLY — explicit accept/reject for a NEIGHBOR request.

    Not named in the paper as a distinct message type; we model it
    explicitly so acceptance is signaled without relying on TCP semantics
    (matches the Partisan reference implementation).
    """

    @enforce_keys [:peer, :accepted?]
    defstruct [:peer, :accepted?]

    @type t :: %__MODULE__{peer: Peer.t(), accepted?: boolean()}
  end

  defmodule Disconnect do
    @moduledoc """
    DISCONNECT — sent by the dropper to the dropped peer when an active-view
    slot is reclaimed. The recipient removes the sender from its active view
    and adds it to its passive view. Paper §4.2, Algorithm 1.
    """

    @enforce_keys [:peer]
    defstruct [:peer]

    @type t :: %__MODULE__{peer: Peer.t()}
  end

  defmodule Shuffle do
    @moduledoc """
    SHUFFLE — periodic gossip exchange to refresh passive views.

    The initiator (`origin`) builds a sample list containing its own id, a
    random subset of its active view (size `ka`), and a random subset of its
    passive view (size `kp`). The message is forwarded along an active-view
    random walk; the terminal node merges the sample into its passive view
    and replies with `ShuffleReply`. Paper §4.4.
    """

    @enforce_keys [:origin, :sample, :ttl, :sender]
    defstruct [:origin, :sample, :ttl, :sender]

    @type t :: %__MODULE__{
            origin: Peer.t(),
            sample: [Peer.t()],
            ttl: non_neg_integer(),
            sender: Peer.t()
          }
  end

  defmodule ShuffleReply do
    @moduledoc """
    SHUFFLE_REPLY — sent by the terminal node of a SHUFFLE walk back to the
    `origin`, containing a same-size sample from the responder's passive
    view. Paper §4.4.
    """

    @enforce_keys [:sample]
    defstruct [:sample]

    @type t :: %__MODULE__{sample: [Peer.t()]}
  end

  @typedoc "Any HyParView wire message."
  @type t ::
          Join.t()
          | ForwardJoin.t()
          | Neighbor.t()
          | NeighborReply.t()
          | Disconnect.t()
          | Shuffle.t()
          | ShuffleReply.t()
end
