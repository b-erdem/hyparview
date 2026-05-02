defmodule HyParViewTest do
  use ExUnit.Case, async: true

  doctest HyParView

  test "module loads" do
    assert Code.ensure_loaded?(HyParView)
  end
end
