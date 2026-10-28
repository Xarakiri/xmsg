# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.AdmissionTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{ClientIp, WsClient}
  alias Xmsg.V1.{ClientFrame, Connect, Ping, ServerFrame, Welcome}

  setup do
    previous = Application.get_all_env(:xmsg_gateway)

    on_exit(fn ->
      for key <- [
            :admission_rate,
            :max_connections,
            :preauth_per_ip,
            :proxy_hops,
            :frame_rate,
            :frame_burst
          ] do
        case Keyword.fetch(previous, key) do
          {:ok, value} -> Application.put_env(:xmsg_gateway, key, value)
          :error -> Application.delete_env(:xmsg_gateway, key)
        end
      end
    end)

    %{port: XmsgGateway.Application.port()}
  end

  defp set(key, value), do: Application.put_env(:xmsg_gateway, key, value)

  defp connect_as(c, user_id) do
    c =
      WsClient.send_frame(c, %ClientFrame{
        frame: {:connect, %Connect{access_token: "dev:#{user_id}", protocol_version: 1}}
      })

    {c, %ServerFrame{frame: {:welcome, %Welcome{}}}, _} = WsClient.recv_server_frame(c)
    c
  end

  defp refused?(result) do
    case result do
      {:refused, 503, headers} ->
        {_, seconds} = List.keyfind(headers, "retry-after", 0)
        assert String.to_integer(seconds) in 1..5
        true

      {:ok, _} ->
        false
    end
  end

  test "new sessions beyond the per-second rate are told to come back", %{port: port} do
    set(:admission_rate, 3)
    # Start at the beginning of a second so the window doesn't roll over mid-test.
    wait_for_next_second()
    results = for _ <- 1..5, do: WsClient.try_connect(port)
    assert Enum.count(results, &refused?/1) == 2
    # The next second admits again.
    wait_for_next_second()
    assert {:ok, _} = WsClient.try_connect(port)
  end

  test "a full gateway refuses new connections", %{port: port} do
    set(:admission_rate, 0)
    assert {:ok, c} = WsClient.try_connect(port)
    _c = connect_as(c, 501)
    # This test's own connection keeps the count at 1 or more, whatever else closes.
    set(:max_connections, 1)
    assert refused?(WsClient.try_connect(port))
    set(:max_connections, 0)
    assert {:ok, _} = WsClient.try_connect(port)
  end

  test "unauthenticated sockets are capped per address, and authenticating frees a slot", %{
    port: port
  } do
    set(:admission_rate, 0)
    set(:proxy_hops, 1)
    set(:preauth_per_ip, 2)
    from = [{"x-forwarded-for", "203.0.113.7"}]
    {:ok, a} = WsClient.try_connect(port, from)
    {:ok, _b} = WsClient.try_connect(port, from)
    # The 101 can reach us before the socket's init has registered it: wait for both.
    wait_preauth("203.0.113.7", 2)
    assert refused?(WsClient.try_connect(port, from))
    # Another address isn't affected.
    assert {:ok, _} = WsClient.try_connect(port, [{"x-forwarded-for", "203.0.113.8"}])
    _a = connect_as(a, 502)
    assert {:ok, _} = WsClient.try_connect(port, from)
  end

  test "a connection sending far too many frames is closed", %{port: port} do
    set(:admission_rate, 0)
    set(:frame_rate, 1)
    set(:frame_burst, 5)
    {:ok, c} = WsClient.try_connect(port)
    c = connect_as(c, 503)
    ping = %ClientFrame{frame: {:ping, %Ping{}}}
    c = Enum.reduce(1..10, c, fn _, c -> WsClient.send_frame(c, ping) end)
    assert drain_until_close(c) == {:close, 1008, "too many frames"}
  end

  test "client addresses come from the trusted end of X-Forwarded-For" do
    peer = {10, 0, 0, 9}
    assert ClientIp.from(peer, "1.2.3.4", 0) == "10.0.0.9"
    assert ClientIp.from(peer, "1.2.3.4, 10.0.0.5", 2) == "1.2.3.4"
    assert ClientIp.from(peer, "6.6.6.6, 1.2.3.4, 10.0.0.5", 2) == "1.2.3.4"
    assert ClientIp.from(peer, "1.2.3.4", 2) == nil
    assert ClientIp.from(peer, nil, 1) == nil
    assert ClientIp.from(peer, "junk", 1) == nil
    assert ClientIp.from(peer, "2001:db8::1", 1) == "2001:db8::1"
  end

  defp drain_until_close(c) do
    case WsClient.recv(c, 2_000) do
      {_c, {:close, _, _} = close} -> close
      {c, {:binary, _}} -> drain_until_close(c)
      {_c, other} -> other
    end
  end

  defp wait_for_next_second do
    now = System.monotonic_time(:millisecond)
    Process.sleep(1_000 - rem(now, 1_000) + 5)
  end

  defp wait_preauth(ip, n, tries \\ 100) do
    count = length(Registry.lookup(XmsgGateway.Admission.preauth_registry(), ip))

    cond do
      count >= n ->
        :ok

      tries == 0 ->
        flunk("expected #{n} unauthenticated sockets from #{ip}, have #{count}")

      true ->
        Process.sleep(10)
        wait_preauth(ip, n, tries - 1)
    end
  end
end
