# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.PresenceTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, Config, WsClient}

  alias Xmsg.V1.{
    Activity,
    ClientFrame,
    Connect,
    PresencePing,
    PresenceQuery,
    PresenceReply,
    PresenceUpdate,
    PresenceUpdates,
    ServerFrame,
    WatchPresence
  }

  setup do
    test_pid = self()

    Bus.Local.put_responder(fn subject, body ->
      if subject == Config.presence_query_subject() do
        %PresenceQuery{user_ids: ids} = PresenceQuery.decode(body)
        updates = Enum.map(ids, &%PresenceUpdate{user_id: &1, status: :PRESENCE_STATUS_ONLINE})
        send(test_pid, {:queried, ids})
        {:ok, PresenceReply.encode(%PresenceReply{updates: updates})}
      else
        {:error, :no_responders}
      end
    end)

    {:ok, _} = Bus.Local.subscribe(Config.presence_ping_subject())
    %{port: XmsgGateway.Application.port()}
  end

  defp connect_as(port, user_id) do
    c = WsClient.connect(port)

    c =
      WsClient.send_frame(c, %ClientFrame{
        frame: {:connect, %Connect{access_token: "dev:#{user_id}"}}
      })

    {c, %ServerFrame{frame: {:welcome, _}}, _} = WsClient.recv_server_frame(c)
    c
  end

  defp next_ping(user_id) do
    subject = Config.presence_ping_subject()

    receive do
      {:bus_msg, ^subject, body} ->
        case PresencePing.decode(body) do
          %PresencePing{user_id: ^user_id} = ping -> ping
          _ -> next_ping(user_id)
        end
    after
      2_000 -> flunk("no presence ping for #{user_id}")
    end
  end

  test "a connection pings active on connect, idle and active on activity, gone when it closes",
       %{port: port} do
    c = connect_as(port, 31)
    assert %PresencePing{state: :CONNECTION_STATE_ACTIVE, conn_id: conn} = next_ping(31)
    assert conn != ""

    c = WsClient.send_frame(c, %ClientFrame{frame: {:activity, %Activity{active: false}}})
    assert %PresencePing{state: :CONNECTION_STATE_IDLE, conn_id: ^conn} = next_ping(31)
    c = WsClient.send_frame(c, %ClientFrame{frame: {:activity, %Activity{active: false}}})
    c = WsClient.send_frame(c, %ClientFrame{frame: {:activity, %Activity{active: true}}})

    assert %PresencePing{state: :CONNECTION_STATE_ACTIVE} = next_ping(31),
           "no ping for an unchanged state"

    Mint.HTTP.close(c.conn)
    assert %PresencePing{state: :CONNECTION_STATE_GONE, conn_id: ^conn} = next_ping(31)
  end

  test "watching gets current statuses at once, then only the watched users' changes", %{
    port: port
  } do
    c = connect_as(port, 32)

    c =
      WsClient.send_frame(c, %ClientFrame{
        frame: {:watch_presence, %WatchPresence{user_ids: [40, 41, 40]}}
      })

    assert_receive {:queried, [40, 41]}

    assert {c, %ServerFrame{frame: {:presence, %PresenceUpdates{updates: now}}}, _} =
             WsClient.recv_server_frame(c)

    assert Enum.map(now, & &1.user_id) == [40, 41]

    # Replace the set: 41 is no longer watched.
    c =
      WsClient.send_frame(c, %ClientFrame{
        frame: {:watch_presence, %WatchPresence{user_ids: [40]}}
      })

    {c, _, _} = WsClient.recv_server_frame(c)

    for user <- [41, 40] do
      update = %PresenceUpdate{user_id: user, status: :PRESENCE_STATUS_AWAY}
      Bus.Local.publish("presence.#{user}", PresenceUpdate.encode(update))
    end

    assert {_c,
            %ServerFrame{
              frame:
                {:presence,
                 %PresenceUpdates{updates: [%{user_id: 40, status: :PRESENCE_STATUS_AWAY}]}}
            }, _} =
             WsClient.recv_server_frame(c)
  end
end
