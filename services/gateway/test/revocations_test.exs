# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.RevocationsTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, TestJwt, WsClient}
  alias XmsgGateway.Auth.Jwks
  alias Xmsg.V1.{AuthEvent, ClientFrame, Connect, ServerFrame, SessionRevoked, Welcome}

  setup do
    key = TestJwt.keypair()
    previous = Jwks.keys()
    Jwks.put_keys(%{key.kid => key.public})
    on_exit(fn -> Jwks.put_keys(previous) end)
    %{key: key, port: XmsgGateway.Application.port()}
  end

  # Session IDs unique per test run, so deny entries of one test don't leak into another.
  defp sid, do: System.unique_integer([:positive]) + 1_000_000

  defp connect(port, key, user, sid) do
    token = TestJwt.sign(key, TestJwt.claims(user, %{"sid" => sid}))
    c = WsClient.connect(port)
    WsClient.send_frame(c, %ClientFrame{frame: {:connect, %Connect{access_token: token}}})
  end

  defp connected(port, key, user, sid) do
    c = connect(port, key, user, sid)
    {c, %ServerFrame{frame: {:welcome, %Welcome{}}}, _} = WsClient.recv_server_frame(c)
    c
  end

  defp revoke(sid, refuse_for_ms \\ 900_000) do
    now = System.os_time(:millisecond)

    event = %AuthEvent{
      event:
        {:session_revoked,
         %SessionRevoked{
           user_id: 1,
           session_id: sid,
           revoked_at_ms: now,
           refuse_until_ms: now + refuse_for_ms,
           reason: "logout"
         }}
    }

    Bus.Local.publish("auth.revocations", AuthEvent.encode(event))
  end

  test "a revocation closes the session's sockets, and only those", %{key: key, port: port} do
    {revoked, other} = {sid(), sid()}
    tab1 = connected(port, key, 77, revoked)
    tab2 = connected(port, key, 77, revoked)
    phone = connected(port, key, 77, other)

    revoke(revoked)

    assert {_c, {:close, 1008, "session revoked"}} = WsClient.recv(tab1, 2_000)
    assert {_c, {:close, 1008, "session revoked"}} = WsClient.recv(tab2, 2_000)
    # The same user's other session is untouched.
    phone = WsClient.send_frame(phone, %ClientFrame{frame: {:ping, %Xmsg.V1.Ping{sent_at_ms: 1}}})
    assert {_c, %ServerFrame{frame: {:pong, _}}, _} = WsClient.recv_server_frame(phone)
  end

  test "a revoked session's still-valid token can't reconnect", %{key: key, port: port} do
    s = sid()
    revoke(s)
    # The revocation is applied asynchronously by the Revocations process.
    assert eventually(fn -> XmsgGateway.Revocations.revoked?(s) end)
    c = connect(port, key, 78, s)
    assert {_c, {:close, 1008, "session revoked"}} = WsClient.recv(c, 2_000)
  end

  test "once every token of the session has expired, the deny entry no longer applies", %{
    key: key,
    port: port
  } do
    s = sid()
    revoke(s, -1)
    Process.sleep(50)
    refute XmsgGateway.Revocations.revoked?(s)
    connected(port, key, 79, s)
  end

  test "repeated and garbage messages are harmless" do
    s = sid()
    revoke(s)
    # an earlier deadline doesn't shorten the existing one
    revoke(s, 60_000)
    Bus.Local.publish("auth.revocations", "not protobuf")
    assert eventually(fn -> XmsgGateway.Revocations.revoked?(s) end)
    [{^s, until}] = :ets.lookup(:xmsg_revoked_sessions, s)
    assert until > System.os_time(:millisecond) + 600_000
    assert Process.alive?(Process.whereis(XmsgGateway.Revocations))
  end

  test "dev tokens (session 0) are never revoked" do
    refute XmsgGateway.Revocations.revoked?(0)
  end

  defp eventually(fun, tries \\ 50) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(10) && eventually(fun, tries - 1)
    end
  end
end
