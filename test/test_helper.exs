ExUnit.start()

# Shared in-process transport registry, used by Transport.Test and any
# integration tests that drive real GenServer-backed nodes.
{:ok, _} = HyParView.Transport.Test.start_link()
