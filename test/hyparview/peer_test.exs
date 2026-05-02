defmodule HyParView.PeerTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.Peer

  doctest Peer

  describe "new/2" do
    property "round-trips id and address" do
      check all(
              id <- StreamData.binary(min_length: 1),
              addr <- StreamData.term()
            ) do
        peer = Peer.new(id, addr)
        assert %Peer{id: ^id, address: ^addr} = peer
      end
    end
  end

  describe "same?/2" do
    test "returns true when ids match, regardless of address" do
      a = Peer.new("n1", :addr_a)
      b = Peer.new("n1", :addr_b)
      assert Peer.same?(a, b)
    end

    test "returns false when ids differ" do
      a = Peer.new("n1", :addr)
      b = Peer.new("n2", :addr)
      refute Peer.same?(a, b)
    end
  end

  describe "@enforce_keys" do
    test "raises when id is missing" do
      assert_raise ArgumentError, fn ->
        struct!(Peer, address: :addr)
      end
    end

    test "raises when address is missing" do
      assert_raise ArgumentError, fn ->
        struct!(Peer, id: "n1")
      end
    end
  end
end
