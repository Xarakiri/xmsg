# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.PullTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, Pull, WsClient}
  alias Xmsg.V1.{ClientFrame, Connect, Pong, PullControl, ServerFrame, Welcome}

  # A stand-in for the channels service: the caller's pull-mode channels, by token.
  defmodule Channels do
    use Plug.Router
    plug(:match)
    plug(:dispatch)

    get "/v1/me/channels" do
      %{"mode" => "pull"} = Plug.Conn.fetch_query_params(conn).query_params

      case get_req_header(conn, "authorization") do
        ["Bearer dev:711"] -> send_resp(conn, 200, ~s({"channel_ids": ["9711"]}))
        _ -> send_resp(conn, 401, ~s({"error": "invalid credentials"}))
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

  defp control(user_id, channel_id, subscribe) do
    body =
      PullControl.encode(%PullControl{
        user_id: user_id,
        channel_id: channel_id,
        subscribe: subscribe
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

  test "the channels service's answer is parsed" do
    assert {:error, _} = Pull.pull_channels("http://127.0.0.1:1", "dev:1")
  end
end
