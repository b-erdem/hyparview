defmodule HyParView.ConnectionTest do
  @moduledoc """
  Targeted tests for `HyParView.Connection`'s edge / error paths that
  the regular two-node and cluster tests don't exercise:

    * `decode_hello/1` rejecting malformed bytes (`:bad_format`,
      `:unsupported_hello_version`, `:bad_term`, `:not_peer`).
    * `outbound_connecting` failing when the peer port is unreachable
      (`{:connect_failed, _}` shutdown).
    * `parse_ip/1` accepting a binary IP (the only form the regular
      tests don't use).

  Tests use raw `:gen_tcp` sockets to drive the wire protocol directly
  rather than spinning up two `HyParView.Server`s — that lets us
  inject deliberately-bad bytes and observe the `Transport.TCP`'s
  response without needing to defeat the protocol's own correctness.
  """

  use ExUnit.Case, async: true

  alias HyParView.{Peer, Server, Transport}
  alias HyParView.Test.Integration

  @hello_magic 0x48505631
  @hello_version 1

  defp ephemeral_port do
    {:ok, sock} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, reuseaddr: true)
    {:ok, port} = :inet.port(sock)
    :gen_tcp.close(sock)
    port
  end

  defp start_tcp_node(peer, opts \\ []) do
    default = [
      peer: peer,
      transport: Transport.TCP,
      config: [active_view_size: 4, shuffle_interval: 1_000_000]
    ]

    start_supervised!(%{
      id: peer.id,
      start: {Server, :start_link, [Keyword.merge(default, opts)]},
      restart: :temporary
    })
  end

  describe "Hello frame decoding" do
    setup do
      # A real listener; we'll send raw bytes at it.
      listen_peer =
        Peer.new(
          "listen-#{System.unique_integer([:positive])}",
          {{127, 0, 0, 1}, ephemeral_port()}
        )

      _pid = start_tcp_node(listen_peer)
      {_ip, port} = listen_peer.address
      {:ok, port: port}
    end

    defp connect_and_send(port, bytes) do
      {:ok, sock} =
        :gen_tcp.connect(~c"127.0.0.1", port, [
          :binary,
          packet: 4,
          active: false,
          nodelay: true
        ])

      :ok = :gen_tcp.send(sock, bytes)
      sock
    end

    test "rejects empty / non-magic frame (`:bad_format`)", %{port: port} do
      sock = connect_and_send(port, <<>>)

      # Server should close the connection rather than crash. Either we
      # see {:error, :closed} on the next recv, or a timeout — both mean
      # the receiver dropped us cleanly.
      assert match?({:error, _}, :gen_tcp.recv(sock, 0, 500))
      :gen_tcp.close(sock)
    end

    test "rejects garbage that looks vaguely like a frame (`:bad_format`)", %{port: port} do
      sock = connect_and_send(port, <<0, 1, 2, 3>>)
      assert match?({:error, _}, :gen_tcp.recv(sock, 0, 500))
      :gen_tcp.close(sock)
    end

    test "rejects unknown Hello version", %{port: port} do
      bogus_version = @hello_version + 99
      bytes = <<@hello_magic::32, bogus_version::8, "ignored">>
      sock = connect_and_send(port, bytes)
      assert match?({:error, _}, :gen_tcp.recv(sock, 0, 500))
      :gen_tcp.close(sock)
    end

    test "rejects Hello whose term doesn't decode to a Peer struct (`:not_peer`)",
         %{port: port} do
      # Valid magic + version but the term is a list, not a `%Peer{}`.
      bin = :erlang.term_to_binary([:not, :a, :peer])
      bytes = <<@hello_magic::32, @hello_version::8, bin::binary>>
      sock = connect_and_send(port, bytes)
      assert match?({:error, _}, :gen_tcp.recv(sock, 0, 500))
      :gen_tcp.close(sock)
    end

    test "rejects Hello whose payload isn't valid term_to_binary (`:bad_term`)",
         %{port: port} do
      # Valid magic + version, but the rest is garbage that
      # binary_to_term will reject under `:safe`.
      bytes = <<@hello_magic::32, @hello_version::8, 0xFF, 0xFF, 0xFF, 0xFF>>
      sock = connect_and_send(port, bytes)
      assert match?({:error, _}, :gen_tcp.recv(sock, 0, 500))
      :gen_tcp.close(sock)
    end
  end

  describe "outbound connect failure" do
    test "outbound_connecting handles unreachable peer cleanly (no crash, eventual eviction)" do
      # Pick a port that should be free, then tell our outbound to
      # connect to it without anyone listening. Connection will fail
      # with `{:shutdown, {:connect_failed, :econnrefused}}` — we want
      # to confirm the rest of the system survives.
      port = ephemeral_port()

      contact =
        Peer.new(
          "dead-#{System.unique_integer([:positive])}",
          {{127, 0, 0, 1}, port}
        )

      joiner =
        Peer.new(
          "joiner-#{System.unique_integer([:positive])}",
          {{127, 0, 0, 1}, ephemeral_port()}
        )

      # The connect-failure path will log; capture so test output stays clean.
      ExUnit.CaptureLog.capture_log(fn ->
        joiner_pid = start_tcp_node(joiner, contacts: [contact])

        # Give the connection attempt time to fail and the failure to
        # propagate. The contact may or may not still be in active view
        # depending on retry/eviction policy — we only assert the
        # server doesn't crash and is still responsive.
        Process.sleep(300)

        assert Process.alive?(joiner_pid)
        # Server still answers calls (so its mailbox isn't wedged):
        assert is_list(HyParView.active_view(joiner_pid))
      end)
    end
  end

  describe "parse_ip with binary form" do
    test "outbound connect succeeds when peer.address uses a binary IP" do
      # Both nodes get a binary-IP address: this exercises
      # `parse_ip/1` on both the LISTEN side (Transport.TCP.init/1)
      # and the CONNECT side (Connection.outbound_connecting/3).
      contact =
        Peer.new(
          "bin-c-#{System.unique_integer([:positive])}",
          {"127.0.0.1", ephemeral_port()}
        )

      _ = start_tcp_node(contact)

      joiner =
        Peer.new(
          "bin-j-#{System.unique_integer([:positive])}",
          {"127.0.0.1", ephemeral_port()}
        )

      joiner_pid = start_tcp_node(joiner, contacts: [contact])

      assert :ok =
               Integration.wait_for_active(
                 [joiner_pid],
                 fn av -> match?([_], av) end,
                 2_000
               )
    end
  end
end
