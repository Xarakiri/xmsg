// SPDX-License-Identifier: AGPL-3.0-only
//! End-to-end smoke test of the message path that exists today:
//!
//!   client --WebSocket--> gateway --NATS--> messages --Kafka--> sequencer --> messages.persisted
//!
//! 1. Creates a channel by producing `channel.created` to `messages.in`
//!    (the channels service doesn't exist yet).
//! 2. Two users connect to the gateway and each sends a message.
//! 3. Waits for both `SendAck`s, then reads `messages.persisted` and checks the
//!    sequenced events: consecutive msg_seq, the right authors, and a rejection
//!    for a non-member.
//!
//! Exits 0 on success, 1 on failure.
//! Env: XMSG_GATEWAY_URL (ws://127.0.0.1:4000/ws), XMSG_KAFKA_BOOTSTRAP (127.0.0.1:9092).

use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio_tungstenite::{connect_async, tungstenite::Message as Ws};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    ChannelCreated, ChannelType, ClientFrame, Connect, InboundEvent, RejectReason,
    SendMessageRequest, SequencedEvent, ServerFrame, client_frame, inbound_event, sequenced_event,
    server_frame,
};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => {
            println!("✅ end-to-end smoke test passed");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("❌ end-to-end smoke test failed: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let gateway =
        std::env::var("XMSG_GATEWAY_URL").unwrap_or_else(|_| "ws://127.0.0.1:4000/ws".into());
    let bootstrap =
        std::env::var("XMSG_KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let channel = run_id; // unique per run
    let (alice, bob, mallory) = (
        1_000 + run_id % 1_000,
        2_000 + run_id % 1_000,
        3_000 + run_id % 1_000,
    );

    // 1. Create the channel (alice and bob are members).
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("partitioner", "murmur2_random")
        .create()
        .map_err(|e| e.to_string())?;
    let created = InboundEvent {
        channel_id: channel,
        workspace_id: 1,
        actor_id: alice,
        dedupe_id: format!("create-{channel}"),
        payload: Some(inbound_event::Payload::ChannelCreated(ChannelCreated {
            r#type: ChannelType::Private as i32,
            initial_member_ids: vec![alice, bob],
        })),
        ..Default::default()
    };
    let key = xmsg_partition::channel_key(channel);
    producer
        .send(
            FutureRecord::to("messages.in")
                .key(&key[..])
                .payload(&created.encode_to_vec()),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| format!("producing channel.created: {e}"))?;
    println!("created channel {channel} with members {alice}, {bob}");

    // 2. Users send through the gateway.
    let mut sockets = [
        connect(&gateway, alice).await?,
        connect(&gateway, bob).await?,
        connect(&gateway, mallory).await?,
    ];
    // (socket index, client_msg_id): alice, bob, alice again, then mallory (not a member).
    for (i, id) in [(0, "a-1"), (1, "b-1"), (0, "a-2"), (2, "m-1")] {
        send(&mut sockets[i], channel, id).await?;
        // Each must be acked (accepted into messages.in); membership is checked later.
        expect_ack(&mut sockets[i], id).await?;
    }
    println!("all four sends acked by the gateway");

    // 3. Check what the sequencer produced.
    let events = read_persisted(&bootstrap, channel, 5).await?;
    let messages: Vec<&SequencedEvent> = events
        .iter()
        .filter(|e| matches!(e.payload, Some(sequenced_event::Payload::MessageCreated(_))))
        .collect();
    let seqs: Vec<i64> = messages.iter().map(|e| e.msg_seq).collect();
    let ids: Vec<&str> = messages.iter().map(|e| e.dedupe_id.as_str()).collect();
    if seqs != [1, 2, 3] || ids != ["a-1", "b-1", "a-2"] {
        return Err(format!(
            "unexpected messages: msg_seq {seqs:?}, ids {ids:?}"
        ));
    }
    let rejected = events.iter().find_map(|e| match &e.payload {
        Some(sequenced_event::Payload::Rejected(r)) if e.dedupe_id == "m-1" => Some(r.reason),
        _ => None,
    });
    if rejected != Some(RejectReason::NotAMember as i32) {
        return Err(format!(
            "expected m-1 to be rejected as NOT_A_MEMBER, got {rejected:?}"
        ));
    }
    println!("sequencer output: msg_seq {seqs:?} for {ids:?}; non-member send rejected");
    Ok(())
}

async fn connect(url: &str, user: i64) -> Result<Socket, String> {
    let (mut ws, _) = connect_async(url)
        .await
        .map_err(|e| format!("connecting to {url}: {e}"))?;
    let frame = ClientFrame {
        frame: Some(client_frame::Frame::Connect(Connect {
            access_token: format!("dev:{user}"),
            protocol_version: 1,
            ..Default::default()
        })),
    };
    ws.send(Ws::Binary(frame.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    match next_frame(&mut ws).await? {
        server_frame::Frame::Welcome(_) => Ok(ws),
        other => Err(format!("expected Welcome, got {other:?}")),
    }
}

async fn send(ws: &mut Socket, channel: i64, client_msg_id: &str) -> Result<(), String> {
    let frame = ClientFrame {
        frame: Some(client_frame::Frame::SendMessage(SendMessageRequest {
            channel_id: channel,
            client_msg_id: client_msg_id.into(),
            body: format!("hello from {client_msg_id}"),
        })),
    };
    ws.send(Ws::Binary(frame.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())
}

async fn expect_ack(ws: &mut Socket, client_msg_id: &str) -> Result<(), String> {
    match next_frame(ws).await? {
        server_frame::Frame::Ack(ack) if ack.client_msg_id == client_msg_id => Ok(()),
        other => Err(format!(
            "expected SendAck for {client_msg_id}, got {other:?}"
        )),
    }
}

async fn next_frame(ws: &mut Socket) -> Result<server_frame::Frame, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, ws.next())
            .await
            .map_err(|_| "timed out waiting for a server frame".to_string())?
            .ok_or("socket closed")?
            .map_err(|e| e.to_string())?;
        if let Ws::Binary(bytes) = msg {
            let frame = ServerFrame::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
            return frame.frame.ok_or_else(|| "empty server frame".into());
        }
    }
}

async fn read_persisted(
    bootstrap: &str,
    channel: i64,
    expected: usize,
) -> Result<Vec<SequencedEvent>, String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("e2e-smoke-{channel}"))
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| e.to_string())?;
    consumer
        .subscribe(&["messages.persisted"])
        .map_err(|e| e.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut events = Vec::new();
    while events.len() < expected && Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(500)) {
            if let Ok(e) = SequencedEvent::decode(m.payload().unwrap_or_default()) {
                if e.channel_id == channel {
                    events.push(e);
                }
            }
        }
    }
    if events.len() < expected {
        return Err(format!(
            "only {} of {expected} events for channel {channel} in messages.persisted (is the sequencer running?)",
            events.len()
        ));
    }
    Ok(events)
}
