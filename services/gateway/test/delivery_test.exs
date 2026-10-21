# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.DeliveryTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Bus, Config, WsClient}

  alias Xmsg.V1.{
    ClientFrame,
    Connect,
    Delivery,
    GatewayBatch,
    MessageCreated,
    Resync,
    SequencedEvent,
    ServerFrame,
    StreamHead,
    StreamHeads
  }

  setup do
    # Each test uses its own partition numbers, so stream tracking never leaks between tests.
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

  defp event_frame(body) do
    event = %SequencedEvent{
      channel_id: 100,
      event_seq: 1,
      msg_seq: 1,
      payload: {:message_created, %MessageCreated{message_id: 1, body: body}}
    }

    ServerFrame.encode(%ServerFrame{frame: {:event, event}})
  end

  defp publish_batch(partition, epoch, seq, deliveries) do
    batch = %GatewayBatch{
      partition: partition,
      owner_epoch: epoch,
      seq: seq,
      deliveries: deliveries
    }

    Bus.Local.publish(Config.delivery_subject(), GatewayBatch.encode(batch))
  end

  defp publish_heads(heads) do
    msg = %StreamHeads{
      heads: for({p, e, s} <- heads, do: %StreamHead{partition: p, owner_epoch: e, seq: s})
    }

    Bus.Local.publish(Config.stream_heads_subject(), StreamHeads.encode(msg))
  end

  test "frames reach exactly their recipients, byte for byte (encoded once)", %{port: port} do
    alice_phone = connect_as(port, 101)
    alice_laptop = connect_as(port, 101)
    bob = connect_as(port, 102)

    frame = event_frame("hello alice")
    publish_batch(1, 1, 1, [%Delivery{user_ids: [101], server_frame: frame}])

    assert {_, %ServerFrame{frame: {:event, _}}, ^frame} = WsClient.recv_server_frame(alice_phone)

    assert {_, %ServerFrame{frame: {:event, _}}, ^frame} =
             WsClient.recv_server_frame(alice_laptop)

    assert {_, :timeout} = WsClient.recv(bob, 300)
  end

  test "a missing batch triggers a resync for that partition only", %{port: port} do
    c = connect_as(port, 201)
    publish_batch(2, 1, 1, [%Delivery{user_ids: [201], server_frame: event_frame("1")}])
    {c, %ServerFrame{frame: {:event, _}}, _} = WsClient.recv_server_frame(c)

    # seq 2 lost; seq 3 arrives
    publish_batch(2, 1, 3, [%Delivery{user_ids: [201], server_frame: event_frame("3")}])
    {c, first, _} = WsClient.recv_server_frame(c)
    {_c, second, _} = WsClient.recv_server_frame(c)

    frames = [first, second]
    assert %ServerFrame{frame: {:resync, %Resync{partition: 2}}} in frames
    assert Enum.any?(frames, &match?(%ServerFrame{frame: {:event, _}}, &1))
  end

  test "a new owner epoch resets tracking instead of counting as a gap", %{port: port} do
    c = connect_as(port, 301)
    publish_batch(3, 1, 10, [])

    publish_batch(3, 2, 1, [
      %Delivery{user_ids: [301], server_frame: event_frame("after rebalance")}
    ])

    assert {c, %ServerFrame{frame: {:event, _}}, _} = WsClient.recv_server_frame(c)
    assert {_c, :timeout} = WsClient.recv(c, 300)
    assert XmsgGateway.Delivery.streams()[3] == {2, 1}
  end

  test "stream heads reveal a lost last batch on a quiet stream", %{port: port} do
    c = connect_as(port, 401)
    publish_batch(4, 1, 5, [])
    publish_heads([{4, 1, 5}])
    assert {c, :timeout} = WsClient.recv(c, 300)

    publish_heads([{4, 1, 6}])

    assert {_c, %ServerFrame{frame: {:resync, %Resync{partition: 4}}}, _} =
             WsClient.recv_server_frame(c)
  end

  test "duplicate or old batches are ignored for gap purposes", %{port: port} do
    c = connect_as(port, 501)
    publish_batch(5, 1, 1, [])
    publish_batch(5, 1, 1, [])
    publish_batch(5, 1, 2, [])
    assert {_c, :timeout} = WsClient.recv(c, 300)
    assert XmsgGateway.Delivery.streams()[5] == {1, 2}
  end
end
