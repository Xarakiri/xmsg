# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.PullTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, Pull, WsClient}

  alias Xmsg.V1.{
    AuthEvent,
    ChannelBump,
    ClientFrame,
    Connect,
    MemberJoined,
    MessageCreated,
    SequencedEvent,
    ViewChannel,
    Pong,
    PullControl,
    ServerFrame,
    Welcome,
    WorkspaceMemberRemoved
  }

  # A stand-in for the channels service: the caller's pull-mode channels, by token.
  defmodule Channels do
    use Plug.Router
    plug(:match)
    plug(:dispatch)

    get "/v1/me/channels" do
      %{"mode" => "pull"} = Plug.Conn.fetch_query_params(conn).query_params

      case get_req_header(conn, "authorization") do
        ["Bearer dev:711"] ->
          send_resp(conn, 200, ~s({"channels": [{"channel_id": "9711", "workspace_id": "1"}]}))

        _ ->
          send_resp(conn, 401, ~s({"error": "invalid credentials"}))
      end
    end
  end

  setup do
    previous = Application.get_env(:xmsg_gateway, :channels_url)
    on_exit(fn -> Application.put_env(:xmsg_gateway, :channels_url, previous) end)
    %{port: XmsgGateway.Application.port()}
  end

  defp connect_as(port, user_id) do
    c =
      port
      |> WsClient.connect()
      |> WsClient.send_frame(%ClientFrame{
        frame: {:connect, %Connect{access_token: "dev:#{user_id}", protocol_version: 1}}
      })

    {c, %ServerFrame{frame: {:welcome, %Welcome{}}}, _} = WsClient.recv_server_frame(c)
    c
  end

  defp control(user_id, channel_id, subscribe, workspace_id \\ 1) do
    body =
      PullControl.encode(%PullControl{
        user_id: user_id,
        channel_id: channel_id,
        subscribe: subscribe,
        workspace_id: workspace_id
      })

    Bus.Local.publish(XmsgGateway.Config.control_subject(), body)
  end

  defp frame(n), do: ServerFrame.encode(%ServerFrame{frame: {:pong, %Pong{sent_at_ms: n}}})

  defp eventually(f, tries \\ 50) do
    cond do
      f.() -> true
      tries == 0 -> false
      true -> Process.sleep(20) && eventually(f, tries - 1)
    end
  end

  defp next_pong(c) do
    case WsClient.recv(c, 500) do
      {c, {:binary, bin}} ->
        case ServerFrame.decode(bin) do
          %ServerFrame{frame: {:pong, %Pong{sent_at_ms: n}}} -> {c, n}
          _ -> next_pong(c)
        end

      {c, other} ->
        {c, other}
    end
  end

  test "fan-out's control messages start and stop a channel for a connected member", %{port: port} do
    member = connect_as(port, 701)
    other = connect_as(port, 702)

    control(701, 9701, true)
    assert eventually(fn -> 9701 in Pull.subscribed() end)
    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9701) != [] end)

    # One publish on ch.{id} reaches the member, not the other user.
    Bus.Local.publish("ch.9701", frame(1))
    assert {member, 1} = next_pong(member)
    assert {_other, :timeout} = next_pong(other)

    # Left: nothing more for them.
    control(701, 9701, false)
    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9701) == [] end)
    Bus.Local.publish("ch.9701", frame(2))
    assert {_member, :timeout} = next_pong(member)
  end

  test "a connecting user's pull-mode channels come from the channels service", %{port: port} do
    {:ok, server} = Bandit.start_link(plug: Channels, port: 0, ip: :loopback)
    {:ok, {_ip, http_port}} = ThousandIsland.listener_info(server)
    Application.put_env(:xmsg_gateway, :channels_url, "http://127.0.0.1:#{http_port}")

    c = connect_as(port, 711)
    assert eventually(fn -> 9711 in Pull.subscribed() end)
    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9711) != [] end)
    Bus.Local.publish("ch.9711", frame(3))
    assert {_c, 3} = next_pong(c)

    # Someone the service turns away gets nothing subscribed.
    _c = connect_as(port, 712)
    Process.sleep(200)
    assert Registry.lookup(Pull.members_registry(), 9712) == []
  end

  test "removal from a workspace stops its pull channels at once, and keeps them off", %{
    port: port
  } do
    c = connect_as(port, 721)
    control(721, 9721, true, 5)
    control(721, 9722, true, 6)
    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9722) != [] end)

    removed = %WorkspaceMemberRemoved{
      workspace_id: 5,
      user_id: 721,
      removed_by: 1,
      removed_at_ms: 1
    }

    Bus.Local.publish(
      "auth.revocations",
      AuthEvent.encode(%AuthEvent{event: {:workspace_member_removed, removed}})
    )

    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9721) == [] end)
    # Fan-out's (or a late re-fetch's) subscribe for that workspace is refused now.
    control(721, 9723, true, 5)
    Process.sleep(100)
    assert Registry.lookup(Pull.members_registry(), 9723) == []
    # Another workspace's channel is untouched.
    Bus.Local.publish("ch.9722", frame(7))
    assert {_c, 7} = next_pong(c)
  end

  defp event(channel_id, event_seq, payload) do
    ServerFrame.encode(%ServerFrame{
      frame:
        {:event,
         %SequencedEvent{
           channel_id: channel_id,
           event_seq: event_seq,
           msg_seq: event_seq,
           payload: payload
         }}
    })
  end

  defp message(channel_id, n),
    do: event(channel_id, n, {:message_created, %MessageCreated{body: "m#{n}"}})

  defp next_frame(c, timeout \\ 500) do
    case WsClient.recv(c, timeout) do
      {c, {:binary, bin}} -> {c, ServerFrame.decode(bin).frame}
      {c, other} -> {c, other}
    end
  end

  test "members not viewing a pull-mode channel get a bump at most every interval", %{port: port} do
    c = connect_as(port, 731)
    # Until a client says what it views it gets everything in full (older clients).
    control(731, 9731, true)
    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9731) != [] end)
    Bus.Local.publish("ch.9731", message(9731, 0))
    assert {c, {:event, %SequencedEvent{event_seq: 0}}} = next_frame(c)
    c = WsClient.send_frame(c, %ClientFrame{frame: {:view_channel, %ViewChannel{channel_id: 0}}})
    Process.sleep(50)
    control(731, 9731, true)
    assert eventually(fn -> Registry.lookup(Pull.members_registry(), 9731) != [] end)

    # Not open: the first event is a bump with its positions; the next ones within the
    # interval fold into one more bump carrying the latest.
    Bus.Local.publish("ch.9731", message(9731, 1))
    assert {c, {:bump, %ChannelBump{channel_id: 9731, event_seq: 1, msg_seq: 1}}} = next_frame(c)
    Bus.Local.publish("ch.9731", message(9731, 2))
    Bus.Local.publish("ch.9731", message(9731, 3))
    assert {c, :timeout} = next_frame(c, 150)
    assert {c, {:bump, %ChannelBump{event_seq: 3}}} = next_frame(c, 1_000)

    # Membership changes always arrive in full.
    Bus.Local.publish("ch.9731", event(9731, 4, {:member_joined, %MemberJoined{user_id: 9}}))
    assert {c, {:event, %SequencedEvent{event_seq: 4}}} = next_frame(c)

    # Open: full events.
    c =
      WsClient.send_frame(c, %ClientFrame{frame: {:view_channel, %ViewChannel{channel_id: 9731}}})

    Process.sleep(50)
    Bus.Local.publish("ch.9731", message(9731, 5))
    assert {c, {:event, %SequencedEvent{event_seq: 5}}} = next_frame(c)

    # Closed again (0): bumps again.
    c = WsClient.send_frame(c, %ClientFrame{frame: {:view_channel, %ViewChannel{channel_id: 0}}})
    Process.sleep(350)
    Bus.Local.publish("ch.9731", message(9731, 6))
    assert {_c, {:bump, %ChannelBump{event_seq: 6}}} = next_frame(c)
  end

  test "the channels service's answer is parsed" do
    assert {:error, _} = Pull.pull_channels("http://127.0.0.1:1", "dev:1")
  end
end
