// SPDX-License-Identifier: AGPL-3.0-only
//! End-to-end smoke test of the message path that exists today:
//!
//!   client --WebSocket--> gateway --NATS--> messages --Kafka--> sequencer --> messages.persisted
//!   messages.persisted --> persister --> Cassandra, and --> fan-out --NATS--> gateway --WebSocket--> clients
//!
//! 1. Creates a channel by producing `channel.created` to `messages.in`
//!    (the channels service doesn't exist yet).
//! 2. Two users connect to the gateway and each sends a message.
//! 3. Waits for both `SendAck`s, then reads `messages.persisted` and checks the
//!    sequenced events: consecutive msg_seq, the right authors, and a rejection
//!    for a non-member.
//!
//! 4. Checks fan-out delivery: both members receive all three messages over
//!    their WebSockets in order, and the non-member gets only their rejection.
//! 5. If `XMSG_CQL_NODES` is set, checks what the persister wrote: the three
//!    messages, the members and the channel head.
//!
//! Exits 0 on success, 1 on failure.
//! Env: XMSG_GATEWAY_URL (ws://127.0.0.1:4000/ws), XMSG_KAFKA_BOOTSTRAP (127.0.0.1:9092),
//! XMSG_CQL_NODES (optional, e.g. 127.0.0.1:9042).

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

/// A connected user and the channel events it has received so far.
struct Client {
    ws: Socket,
    events: Vec<SequencedEvent>,
}

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
            ..Default::default()
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
    let mut clients = [
        connect(&gateway, alice).await?,
        connect(&gateway, bob).await?,
        connect(&gateway, mallory).await?,
    ];
    // (socket index, client_msg_id): alice, bob, alice again, then mallory (not a member).
    for (i, id) in [(0, "a-1"), (1, "b-1"), (0, "a-2"), (2, "m-1")] {
        send(&mut clients[i].ws, channel, id).await?;
        // Each must be acked (accepted into messages.in); membership is checked later.
        expect_ack(&mut clients[i], id).await?;
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

    check_delivery(&mut clients, channel).await?;

    if let Ok(nodes) = std::env::var("XMSG_CQL_NODES") {
        check_store(&nodes, channel, alice, bob).await?;
    }
    Ok(())
}

/// What the persister wrote (plan §5.7): messages, membership and the head.
async fn check_store(nodes: &str, channel: i64, alice: i64, bob: i64) -> Result<(), String> {
    let db = scylla::client::session_builder::SessionBuilder::new()
        .known_nodes(nodes.split(','))
        .build()
        .await
        .map_err(|e| format!("connecting to {nodes}: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let messages: Vec<(i64, i64)> = db
            .query_unpaged(
                "SELECT msg_seq, author_id FROM xmsg.messages WHERE channel_id = ? AND bucket = 0",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .rows::<(i64, i64)>()
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let members: Vec<(i64,)> = db
            .query_unpaged(
                "SELECT user_id FROM xmsg.members_by_channel WHERE channel_id = ?",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .rows::<(i64,)>()
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let head: Option<(i64, i64)> = db
            .query_unpaged(
                "SELECT event_seq, msg_seq FROM xmsg.channel_heads WHERE channel_id = ?",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .maybe_first_row::<(i64, i64)>()
            .map_err(|e| e.to_string())?;

        let mut member_ids: Vec<i64> = members.into_iter().map(|(u,)| u).collect();
        member_ids.sort();
        if messages == [(3, alice), (2, bob), (1, alice)]
            && member_ids == [alice, bob]
            && head == Some((4, 3))
        {
            println!("persister output: 3 messages, 2 members, head event_seq 4 / msg_seq 3");
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!(
                "store not as expected: messages {messages:?}, members {member_ids:?}, head {head:?} (is the persister running?)"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

async fn connect(url: &str, user: i64) -> Result<Client, String> {
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
    let mut client = Client {
        ws,
        events: Vec::new(),
    };
    match next_frame(&mut client).await? {
        server_frame::Frame::Welcome(_) => Ok(client),
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

/// Waits for the SendAck; channel events arriving meanwhile are kept.
async fn expect_ack(client: &mut Client, client_msg_id: &str) -> Result<(), String> {
    loop {
        match next_frame(client).await? {
            server_frame::Frame::Ack(ack) if ack.client_msg_id == client_msg_id => return Ok(()),
            server_frame::Frame::Event(_) => {} // kept by next_frame
            other => {
                return Err(format!(
                    "expected SendAck for {client_msg_id}, got {other:?}"
                ));
            }
        }
    }
}

/// Reads frames until `done` holds for the events received, or 20 s pass.
async fn wait_events(
    client: &mut Client,
    done: impl Fn(&[SequencedEvent]) -> bool,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done(&client.events) {
        if tokio::time::Instant::now() > deadline {
            return Ok(()); // the caller reports what arrived
        }
        let _ = tokio::time::timeout(Duration::from_millis(500), next_frame(client)).await;
    }
    Ok(())
}

/// Fan-out: members receive every message in order; the non-member only its rejection.
async fn check_delivery(clients: &mut [Client; 3], channel: i64) -> Result<(), String> {
    let messages = |events: &[SequencedEvent]| -> Vec<(i64, String)> {
        let mut seen = Vec::new();
        for e in events.iter().filter(|e| e.channel_id == channel) {
            if let Some(sequenced_event::Payload::MessageCreated(_)) = e.payload {
                // Clients dedupe by event_seq (at-least-once delivery).
                if !seen.iter().any(|(s, _): &(i64, String)| *s == e.msg_seq) {
                    seen.push((e.msg_seq, e.dedupe_id.clone()));
                }
            }
        }
        seen
    };
    let expected: Vec<(i64, String)> =
        vec![(1, "a-1".into()), (2, "b-1".into()), (3, "a-2".into())];
    for (i, name) in [(0, "alice"), (1, "bob")] {
        wait_events(&mut clients[i], |e| messages(e).len() >= 3).await?;
        let got = messages(&clients[i].events);
        if got != expected {
            return Err(format!("{name} received {got:?}, expected {expected:?}"));
        }
    }
    let mallory = &mut clients[2];
    wait_events(mallory, |e| e.iter().any(|e| e.dedupe_id == "m-1")).await?;
    let got = messages(&mallory.events);
    let rejected = mallory.events.iter().any(|e| {
        e.dedupe_id == "m-1"
            && matches!(&e.payload, Some(sequenced_event::Payload::Rejected(r)) if r.reason == RejectReason::NotAMember as i32)
    });
    if !got.is_empty() || !rejected {
        return Err(format!(
            "non-member got messages {got:?}, rejection delivered: {rejected}"
        ));
    }
    println!(
        "fan-out delivered msg_seq 1..3 to both members over WebSocket; the non-member got only its rejection"
    );
    Ok(())
}

async fn next_frame(client: &mut Client) -> Result<server_frame::Frame, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, client.ws.next())
            .await
            .map_err(|_| "timed out waiting for a server frame".to_string())?
            .ok_or("socket closed")?
            .map_err(|e| e.to_string())?;
        if let Ws::Binary(bytes) = msg {
            let frame = ServerFrame::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
            let frame = frame
                .frame
                .ok_or_else(|| "empty server frame".to_string())?;
            if let server_frame::Frame::Event(e) = &frame {
                client.events.push(e.clone());
            }
            return Ok(frame);
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
