defmodule HyParView.ProtocolTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias HyParView.Messages.Join
  alias HyParView.Peer
  alias HyParView.Protocol
  alias HyParView.Test.Generators

  doctest Protocol

  describe "encode/1 + decode/1 round-trip" do
    property "every well-formed message round-trips" do
      check all(msg <- Generators.any_message()) do
        encoded = Protocol.encode(msg)
        assert is_binary(encoded)
        assert {:ok, ^msg} = Protocol.decode(encoded)
      end
    end

    test "concrete Join example" do
      peer = Peer.new("alice", {"127.0.0.1", 4000})
      msg = %Join{new_peer: peer}

      assert {:ok, ^msg} = msg |> Protocol.encode() |> Protocol.decode()
    end
  end

  describe "decode/1 error paths" do
    test "rejects bad magic" do
      assert {:error, :bad_magic} = Protocol.decode(<<0::32, 1::8, "anything"::binary>>)
    end

    test "rejects empty input" do
      assert {:error, :bad_magic} = Protocol.decode(<<>>)
    end

    test "rejects unsupported version" do
      assert {:error, {:unsupported_version, 99}} =
               Protocol.decode(<<0x48504956::32, 99::8, "x"::binary>>)
    end

    test "rejects malformed payload" do
      assert {:error, :unsafe_payload} =
               Protocol.decode(<<0x48504956::32, 1::8, "not a term-to-binary blob">>)
    end

    test "rejects encoded but non-message terms (e.g. plain map)" do
      framed =
        <<0x48504956::32, 1::8,
          :erlang.term_to_binary(%{not: :a_message}, minor_version: 2)::binary>>

      assert {:error, :unknown_message_type} = Protocol.decode(framed)
    end
  end
end
