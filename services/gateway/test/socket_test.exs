# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.SocketTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, WsClient}

  alias Xmsg.V1.{
    ClientFrame,
    Connect,
    InboundEvent,
    Ping,
    SendMessageRequest,
    SendResult,
    ServerFrame,
    Welcome
  }

  setup do
    Bus.Local.put_responder(fn _subject, _body -> {:error, :no_responders} end)
    %{port: XmsgGateway.Application.port()}
  end

  defp connect_as(port, user_id) do
    c = WsClient.connect(port)

    c =
      WsClient.send_frame(c, %ClientFrame{
        frame: {:connect, %Connect{access_token: "dev:#{user_id}", protocol_version: 1}}
      })

    {c, %ServerFrame{frame: {:welcome, %Welcome{} = welcome}}, _} = WsClient.recv_server_frame(c)
    {c, welcome}
  end

  test "a valid connect gets a welcome with the installation's partition count", %{port: port} do
    {_c, welcome} = connect_as(port, 1)
    assert welcome.partition_count == XmsgGateway.Config.partition_count()
    assert byte_size(welcome.resume_token) > 0
  end

  test "a socket that never authenticates is closed after the auth timeout", %{port: port} do
    c = WsClient.connect(port)
    assert {_c, {:close, 1008, "authentication timeout"}} = WsClient.recv(c, 2_000)
  end

  test "a JWT from the auth service authenticates the connection", %{port: port} do
    key = XmsgGateway.TestJwt.keypair()
    previous = XmsgGateway.Auth.Jwks.keys()
    XmsgGateway.Auth.Jwks.put_keys(%{key.kid => key.public})
    on_exit(fn -> XmsgGateway.Auth.Jwks.put_keys(previous) end)

    c = WsClient.connect(port)
    token = XmsgGateway.TestJwt.sign(key, XmsgGateway.TestJwt.claims(77))
    c = WsClient.send_frame(c, %ClientFrame{frame: {:connect, %Connect{access_token: token}}})
    assert {_c, %ServerFrame{frame: {:welcome, %Welcome{}}}, _} = WsClient.recv_server_frame(c)
    assert [{_pid, nil}] = Registry.lookup(XmsgGateway.Users, 77)
  end

  test "an invalid token closes the socket", %{port: port} do
    c = WsClient.connect(port)
    c = WsClient.send_frame(c, %ClientFrame{frame: {:connect, %Connect{access_token: "nope"}}})
    assert {_c, {:close, 1008, "invalid token"}} = WsClient.recv(c, 2_000)
  end

  test "frames other than connect are refused before authentication", %{port: port} do
    c = WsClient.connect(port)
    c = WsClient.send_frame(c, %ClientFrame{frame: {:ping, %Ping{sent_at_ms: 1}}})
    assert {_c, {:close, 1008, "connect first"}} = WsClient.recv(c, 2_000)
  end

  test "ping gets pong", %{port: port} do
    {c, _} = connect_as(port, 2)
    c = WsClient.send_frame(c, %ClientFrame{frame: {:ping, %Ping{sent_at_ms: 42}}})

    assert {_c, %ServerFrame{frame: {:pong, %{sent_at_ms: 42}}}, _} =
             WsClient.recv_server_frame(c)
  end

  test "a send is forwarded to the messages service as the authenticated user and acked", %{
    port: port
  } do
    test_pid = self()

    Bus.Local.put_responder(fn subject, body ->
      send(test_pid, {:request, subject, InboundEvent.decode(body)})
      {:ok, SendResult.encode(%SendResult{accepted: true})}
    end)

    {c, _} = connect_as(port, 7)
    req = %SendMessageRequest{channel_id: 100, client_msg_id: "c-1", body: "hi"}
    c = WsClient.send_frame(c, %ClientFrame{frame: {:send_message, req}})

    assert {_c, %ServerFrame{frame: {:ack, %{client_msg_id: "c-1"}}}, _} =
             WsClient.recv_server_frame(c)

    assert_received {:request, "xmsg.svc.messages.send", event}
    assert event.actor_id == 7, "actor comes from the token, never from the client"
    assert event.channel_id == 100
    assert event.dedupe_id == "c-1"
    assert {:send_message, %{body: "hi"}} = event.payload
  end

  test "a thread reply carries its root through to the messages service", %{port: port} do
    test_pid = self()

    Bus.Local.put_responder(fn _subject, body ->
      send(test_pid, {:request, InboundEvent.decode(body)})
      {:ok, SendResult.encode(%SendResult{accepted: true})}
    end)

    {c, _} = connect_as(port, 8)
    root = %Xmsg.V1.ThreadRoot{msg_seq: 4, message_id: 4004}

    req = %SendMessageRequest{
      channel_id: 100,
      client_msg_id: "r-1",
      body: "agreed",
      thread_root: root
    }

    c = WsClient.send_frame(c, %ClientFrame{frame: {:send_message, req}})

    assert {_c, %ServerFrame{frame: {:ack, %{client_msg_id: "r-1"}}}, _} =
             WsClient.recv_server_frame(c)

    assert_received {:request, %InboundEvent{payload: {:send_message, %{thread_root: ^root}}}}
  end

  test "no ack when the messages service doesn't answer; the client retries later", %{port: port} do
    {c, _} = connect_as(port, 8)
    req = %SendMessageRequest{channel_id: 100, client_msg_id: "c-2", body: "hi"}
    c = WsClient.send_frame(c, %ClientFrame{frame: {:send_message, req}})
    assert {_c, :timeout} = WsClient.recv(c, 700)
  end

  test "a send the service rejects is reported to the client, not acked", %{port: port} do
    Bus.Local.put_responder(fn _subject, _body ->
      {:ok,
       SendResult.encode(%SendResult{
         accepted: false,
         reason: :REJECT_REASON_INVALID,
         detail: "message body is empty"
       })}
    end)

    {c, _} = connect_as(port, 9)
    req = %SendMessageRequest{channel_id: 100, client_msg_id: "c-3", body: " "}
    c = WsClient.send_frame(c, %ClientFrame{frame: {:send_message, req}})

    assert {_c, %ServerFrame{frame: {:send_rejected, rejected}}, _} =
             WsClient.recv_server_frame(c)

    assert rejected.client_msg_id == "c-3"
    assert rejected.reason == :REJECT_REASON_INVALID
    assert rejected.detail == "message body is empty"
  end

  test "an unavailable service gets no ack and no rejection, so the client retries", %{port: port} do
    Bus.Local.put_responder(fn _subject, _body ->
      {:ok, SendResult.encode(%SendResult{accepted: false, reason: :REJECT_REASON_UNAVAILABLE})}
    end)

    {c, _} = connect_as(port, 10)
    req = %SendMessageRequest{channel_id: 100, client_msg_id: "c-4", body: "hi"}
    c = WsClient.send_frame(c, %ClientFrame{frame: {:send_message, req}})
    assert {_c, :timeout} = WsClient.recv(c, 700)
  end

  test "an authenticated connection is published as a session and removed when it closes", %{
    port: port
  } do
    {c, _} = connect_as(port, 11)
    Mint.HTTP.close(c.conn)

    events =
      Enum.reduce_while(1..40, [], fn _, _ ->
        Process.sleep(50)
        events = XmsgGateway.Sessions.Local.events()

        if Enum.any?(events, &match?({:disconnected, _}, &1)),
          do: {:halt, events},
          else: {:cont, events}
      end)

    assert [{:connected, conn_id, 11}] = for({:connected, _, 11} = e <- events, do: e)
    assert {:disconnected, conn_id} in events
  end
end
