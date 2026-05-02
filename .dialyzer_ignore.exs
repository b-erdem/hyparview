# Known false positives.
#
# `call_without_opaque` on `MapSet.member?/2` and `MapSet.put/2` —
# dialyzer expands `MapSet.t(Peer.id())` into the internal struct
# shape; usage is correct (all values come from MapSet.new/put).
[
  {"lib/hyparview/state.ex", :call_without_opaque}
]
