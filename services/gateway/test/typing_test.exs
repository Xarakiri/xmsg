# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.TypingTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, Config, WsClient}

  alias Xmsg.V1.{
    ClientFrame,
    Connect,
    Delivery,
    ServerFrame,
    Typing,
    TypingIndicator
  }

  setup do
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

  defp typing(c, channel_id),
    do: WsClient.send_frame(c, %ClientFrame{frame: {:typing, %Typing{channel_id: channel_id}}})

  test "typing goes to the channel's partition as the authenticated user, at most once per 3 s per channel",
       %{port: port} do
    {:ok, _} = Bus.Local.subscribe(Config.typing_subject(300))
    {:ok, _} = Bus.Local.subscribe(Config.typing_subject(301))
    c = connect_as(port, 21)

    c = typing(c, 300)
    subject = Config.typing_subject(300)
    assert_receive {:bus_msg, ^subject, body}, 1_000
    assert TypingIndicator.decode(body) == %TypingIndicator{channel_id: 300, user_id: 21}

    # Again at once: throttled. Another channel: not.
    c = typing(c, 300)
    _c = typing(c, 301)
    other = Config.typing_subject(301)
    assert_receive {:bus_msg, ^other, body}, 1_000
    assert TypingIndicator.decode(body).channel_id == 301
    # Give a wrongly sent second notice for channel 300 time to arrive.
    Process.sleep(200)

    received =
      Stream.repeatedly(fn ->
        receive do
          {:bus_msg, _, b} -> TypingIndicator.decode(b).channel_id
        after
          0 -> nil
        end
      end)
      |> Enum.take_while(& &1)

    refute 300 in received, "a second notice for channel 300 within 3 s"
  end

  test "best-effort frames on the ephemeral subject reach their users without a stream", %{
    port: port
  } do
    c = connect_as(port, 22)

    frame =
      ServerFrame.encode(%ServerFrame{
        frame: {:typing, %TypingIndicator{channel_id: 5, user_id: 9}}
      })

    Bus.Local.publish(
      Config.ephemeral_subject(),
      Delivery.encode(%Delivery{user_ids: [22], server_frame: frame})
    )

    assert {_c, %ServerFrame{frame: {:typing, %TypingIndicator{channel_id: 5, user_id: 9}}}, _} =
             WsClient.recv_server_frame(c)
  end
end
