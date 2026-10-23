# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.ReceiptsTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, Config, WsClient}
  alias Xmsg.V1.{ClientFrame, Connect, ReadReceipt, Receipt, ServerFrame}

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

  defp receipt(c, channel_id, seq),
    do:
      WsClient.send_frame(c, %ClientFrame{
        frame: {:receipt, %Receipt{channel_id: channel_id, msg_seq: seq}}
      })

  defp published(within) do
    receive do
      {:bus_msg, "receipts." <> _, body} -> ReadReceipt.decode(body)
    after
      within -> nil
    end
  end

  test "receipts go out at once, then at most once a second with the latest position", %{
    port: port
  } do
    {:ok, _} = Bus.Local.subscribe(Config.receipts_subject(500))
    {:ok, _} = Bus.Local.subscribe(Config.receipts_subject(501))
    c = connect_as(port, 51)

    c = receipt(c, 500, 3)
    assert %ReadReceipt{channel_id: 500, user_id: 51, msg_seq: 3} = published(500)

    # Within the second: coalesced into one, the newest.
    c = receipt(c, 500, 5)
    c = receipt(c, 500, 7)
    # Another channel isn't held back.
    c = receipt(c, 501, 2)
    assert %ReadReceipt{channel_id: 501, msg_seq: 2} = published(500)
    assert published(300) == nil
    assert %ReadReceipt{channel_id: 500, msg_seq: 7} = published(1_500)

    # Never backwards.
    _c = receipt(c, 500, 6)
    assert published(1_300) == nil
  end
end
