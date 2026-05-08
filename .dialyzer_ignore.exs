# Known false positives.
#
# `call_without_opaque` on `MapSet.member?/2` and `MapSet.put/2` —
# dialyzer expands `MapSet.t(Peer.id())` into the internal struct
# shape; usage is correct (all values come from MapSet.new/put).
#
# `contract_with_opaque` on `State.new/3` and `tick_shuffle/1` —
# the State struct contains `MapSet.t/0` fields (`pending_repair`,
# `recently_lost`); when dialyzer flattens the struct against the
# `t :: %__MODULE__{...}` spec it sees the MapSet's internal map
# representation and complains. Same root cause as the
# `call_without_opaque` ignores above; not a real type error.
[
  {"lib/hyparview/state.ex", :call_without_opaque},
  {"lib/hyparview/state.ex", :contract_with_opaque}
]
