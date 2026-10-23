// SPDX-License-Identifier: AGPL-3.0-only
//! Integration tests against a real Kafka broker and NATS server.
//!
//! Ignored by default; run with
//!   XMSG_IT_KAFKA=127.0.0.1:9092 XMSG_IT_NATS=nats://127.0.0.1:4222 \
//!     cargo test -p xmsg-messages --test integration -- --ignored --test-threads=1
//! CI starts both from deploy/compose.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use xmsg_messages::EventLog;
use xmsg_messages::kafka::KafkaLog;
use xmsg_proto::Message;
use xmsg_proto::v1::{InboundEvent, RejectReason, SendMessage, SendResult, inbound_event::Payload};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for integration tests"))
}

async fn create_topic(bootstrap: &str, topic: &str, partitions: i32) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client");
    let results = admin
        .create_topics(
            &[NewTopic::new(topic, partitions, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .expect("create topic");
    for r in results {
        if let Err((name, code)) = r {
            assert_eq!(
                code,
                rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists,
                "creating {name}"
            );
        }
    }
}

type Record = (i32, Vec<u8>, Vec<u8>);

/// Reads `topic` from the beginning until `done` holds for what was read, or 60 s pass.
fn read_until(bootstrap: &str, topic: &str, done: impl Fn(&[Record]) -> bool) -> Vec<Record> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("it-{}", std::process::id()))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .expect("consumer");
    consumer.subscribe(&[topic]).expect("subscribe");
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut records = Vec::new();
    while !done(&records) && Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(500)) {
            records.push((
                m.partition(),
                m.key().unwrap_or_default().to_vec(),
                m.payload().unwrap_or_default().to_vec(),
            ));
        }
    }
    records
}

fn send(channel_id: i64, dedupe: &str) -> InboundEvent {
    InboundEvent {
        channel_id,
        actor_id: 7,
        dedupe_id: dedupe.into(),
        payload: Some(Payload::SendMessage(SendMessage {
            body: format!("hello {dedupe}"),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// ADR-0004: what librdkafka (murmur2_random) picks must equal spec/partitioning.
#[tokio::test]
#[ignore = "needs Kafka (XMSG_IT_KAFKA)"]
async fn records_land_on_the_spec_partition() {
    let bootstrap = env("XMSG_IT_KAFKA");
    let topic = format!("it-partitioning-{}", std::process::id());
    let partitions = 16;
    create_topic(&bootstrap, &topic, partitions).await;

    let log = KafkaLog::with_topic(&bootstrap, Duration::from_secs(10), &topic).unwrap();
    let channels: Vec<i64> = (0..300)
        .map(|i| 1_000_000_007_i64.wrapping_mul(i + 1))
        .collect();
    for &channel in &channels {
        log.append(channel, send(channel, "p").encode_to_vec())
            .await
            .unwrap();
    }

    let records = read_until(&bootstrap, &topic, |r| r.len() >= channels.len());
    assert_eq!(records.len(), channels.len());
    for (partition, key, _) in records {
        let channel = i64::from_be_bytes(key.as_slice().try_into().expect("8-byte key"));
        assert_eq!(
            partition as u32,
            xmsg_partition::partition(channel, partitions as u32),
            "channel {channel} landed on the wrong partition"
        );
    }
}

struct Service(Child);

impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_service(partitions: i32) -> Service {
    Service(
        Command::new(env!("CARGO_BIN_EXE_xmsg-messages"))
            .env("XMSG_KAFKA_BOOTSTRAP", env("XMSG_IT_KAFKA"))
            .env("XMSG_NATS_URL", env("XMSG_IT_NATS"))
            .env("XMSG_PARTITIONS", partitions.to_string())
            .env("RUST_LOG", "info")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start xmsg-messages"),
    )
}

/// The real binary: NATS request in, SendResult out, record in messages.in.
#[tokio::test]
#[ignore = "needs Kafka and NATS (XMSG_IT_KAFKA, XMSG_IT_NATS)"]
async fn sends_over_nats_are_accepted_and_appended() {
    let bootstrap = env("XMSG_IT_KAFKA");
    let partitions = std::env::var("XMSG_IT_PARTITIONS")
        .map(|p| p.parse().unwrap())
        .unwrap_or(16);
    create_topic(&bootstrap, "messages.in", partitions).await;
    let _service = start_service(partitions);

    let nats = async_nats::connect(env("XMSG_IT_NATS")).await.unwrap();
    let marker = format!("it-{}", std::process::id());

    // Retry until the service has subscribed.
    let deadline = Instant::now() + Duration::from_secs(60);
    let reply = loop {
        let request = send(4242, &marker).encode_to_vec();
        match tokio::time::timeout(
            Duration::from_secs(2),
            nats.request(xmsg_messages::SEND_SUBJECT, request.into()),
        )
        .await
        {
            Ok(Ok(msg)) => break SendResult::decode(msg.payload.as_ref()).unwrap(),
            _ if Instant::now() < deadline => tokio::time::sleep(Duration::from_millis(500)).await,
            other => panic!("no reply from the messages service: {other:?}"),
        }
    };
    assert!(reply.accepted, "{reply:?}");

    let invalid = InboundEvent {
        dedupe_id: String::new(),
        ..send(4242, "x")
    }
    .encode_to_vec();
    let msg = nats
        .request(xmsg_messages::SEND_SUBJECT, invalid.into())
        .await
        .unwrap();
    let rejected = SendResult::decode(msg.payload.as_ref()).unwrap();
    assert!(!rejected.accepted);
    assert_eq!(rejected.reason, RejectReason::Invalid as i32);

    // The accepted event is in messages.in, unchanged, on the spec partition.
    let is_marker = |(_, _, p): &Record| {
        InboundEvent::decode(p.as_slice()).is_ok_and(|e| e.dedupe_id == marker)
    };
    let records = read_until(&bootstrap, "messages.in", |r| r.iter().any(is_marker));
    let (partition, _, payload) = records
        .iter()
        .find(|r| is_marker(r))
        .expect("accepted event not found in messages.in");
    assert_eq!(
        InboundEvent::decode(payload.as_slice()).unwrap(),
        send(4242, &marker)
    );
    assert_eq!(
        *partition as u32,
        xmsg_partition::partition(4242, partitions as u32)
    );
}

/// ADR-0021: the service refuses to start against a topic with a different partition count.
#[tokio::test]
#[ignore = "needs Kafka and NATS (XMSG_IT_KAFKA, XMSG_IT_NATS)"]
async fn wrong_partition_count_is_refused() {
    let bootstrap = env("XMSG_IT_KAFKA");
    let partitions = std::env::var("XMSG_IT_PARTITIONS")
        .map(|p| p.parse().unwrap())
        .unwrap_or(16);
    create_topic(&bootstrap, "messages.in", partitions).await;

    let mut service = start_service(partitions + 1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = service.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "service should exit");
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(status.code(), Some(2));
}
