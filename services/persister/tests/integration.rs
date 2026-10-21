// SPDX-License-Identifier: AGPL-3.0-only
//! Integration test against real Kafka and Cassandra 5 (or ScyllaDB).
//!
//! Ignored by default; run with
//!   XMSG_IT_KAFKA=127.0.0.1:9092 XMSG_IT_CQL=127.0.0.1:9042 \
//!     cargo test -p xmsg-persister --test integration -- --ignored
//! The schema in deploy/compose/init/cassandra is applied first.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::producer::{FutureProducer, FutureRecord};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use xmsg_proto::Message;
use xmsg_proto::v1::{
    ChannelCreated, ChannelModeChanged, ChannelType, FanoutMode, MemberJoined, MemberLeft,
    MemberRole, MessageCreated, SequencedEvent, sequenced_event::Payload,
};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for integration tests"))
}

const SCHEMA: &str = include_str!("../../../deploy/compose/init/cassandra/001_schema.cql");

async fn session() -> Session {
    let session = SessionBuilder::new()
        .known_node(env("XMSG_IT_CQL"))
        .build()
        .await
        .expect("connect to CQL");
    // Apply the Small-tier schema (idempotent: IF NOT EXISTS).
    let without_comments: String = SCHEMA
        .lines()
        .filter(|l| !l.trim_start().starts_with("--"))
        .collect::<Vec<_>>()
        .join("\n");
    for statement in without_comments
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        session
            .query_unpaged(statement, &[])
            .await
            .unwrap_or_else(|e| panic!("schema: {statement}: {e}"));
    }
    session
}

struct Persister(Child);
impl Drop for Persister {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn start_persister(topic: &str, group: &str, partitions: i32) -> Persister {
    Persister(
        Command::new(env!("CARGO_BIN_EXE_xmsg-persister"))
            .env("XMSG_KAFKA_BOOTSTRAP", env("XMSG_IT_KAFKA"))
            .env("XMSG_CQL_NODES", env("XMSG_IT_CQL"))
            .env("XMSG_PERSISTED_TOPIC", topic)
            .env("XMSG_PERSISTER_GROUP", group)
            .env("XMSG_PARTITIONS", partitions.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start xmsg-persister"),
    )
}

fn event(
    channel: i64,
    event_seq: i64,
    membership_version: i64,
    payload: Payload,
) -> SequencedEvent {
    SequencedEvent {
        channel_id: channel,
        workspace_id: 10,
        actor_id: 1,
        event_seq,
        membership_version,
        fanout_mode: FanoutMode::Push as i32,
        channel_type: ChannelType::Private as i32,
        payload: Some(payload),
        ..Default::default()
    }
}

async fn wait_for<T: PartialEq + std::fmt::Debug>(
    what: &str,
    expected: T,
    mut read: impl AsyncFnMut() -> T,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let actual = read().await;
        if actual == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: expected {expected:?}, got {actual:?}"
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
}

#[tokio::test]
#[ignore = "needs Kafka and Cassandra (XMSG_IT_KAFKA, XMSG_IT_CQL)"]
async fn sequenced_events_become_rows_and_replays_change_nothing() {
    let bootstrap = env("XMSG_IT_KAFKA");
    let db = session().await;
    let run = std::process::id() as i64;
    let topic = format!("it-persisted-{run}");
    let channel = 9_000_000 + run;

    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .create()
        .unwrap();
    admin
        .create_topics(
            &[NewTopic::new(&topic, 4, TopicReplication::Fixed(1))],
            &AdminOptions::new(),
        )
        .await
        .unwrap();
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("partitioner", "murmur2_random")
        .create()
        .unwrap();

    let created = Payload::ChannelCreated(ChannelCreated {
        r#type: ChannelType::Private as i32,
        initial_member_ids: vec![1, 2],
        members: vec![
            MemberJoined {
                user_id: 1,
                role: MemberRole::Admin as i32,
            },
            MemberJoined {
                user_id: 2,
                role: MemberRole::Member as i32,
            },
        ],
    });
    let mut events = vec![event(channel, 1, 1, created)];
    for (i, body) in ["one", "two", "three"].iter().enumerate() {
        let mut e = event(
            channel,
            2 + i as i64,
            1,
            Payload::MessageCreated(MessageCreated {
                message_id: 500 + i as i64,
                body: body.to_string(),
            }),
        );
        e.msg_seq = 1 + i as i64;
        events.push(e);
    }
    events.push(event(
        channel,
        5,
        5,
        Payload::MemberJoined(MemberJoined {
            user_id: 3,
            role: MemberRole::Member as i32,
        }),
    ));
    events.push(event(
        channel,
        6,
        6,
        Payload::MemberLeft(MemberLeft { user_id: 2 }),
    ));
    let mut mode = event(
        channel,
        7,
        6,
        Payload::ChannelModeChanged(ChannelModeChanged {
            mode: FanoutMode::Pull as i32,
        }),
    );
    mode.fanout_mode = FanoutMode::Pull as i32;
    events.push(mode);

    let key = channel.to_be_bytes();
    let produce_all = async |events: &[SequencedEvent]| {
        for e in events {
            producer
                .send(
                    FutureRecord::to(&topic)
                        .key(&key[..])
                        .payload(&e.encode_to_vec()),
                    Duration::from_secs(10),
                )
                .await
                .unwrap();
        }
    };
    produce_all(&events).await;
    let persister = start_persister(&topic, &format!("it-persister-{run}"), 4);

    let head = async || -> (i64, i64, i64, String, String) {
        db.query_unpaged(
            "SELECT event_seq, msg_seq, membership_version, fanout_mode, channel_type FROM xmsg.channel_heads WHERE channel_id = ?",
            (channel,),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .maybe_first_row::<(i64, i64, i64, String, String)>()
        .unwrap()
        .unwrap_or_default()
    };
    wait_for(
        "channel head",
        (7, 3, 6, "pull".to_string(), "private".to_string()),
        head,
    )
    .await;

    let messages = async || -> Vec<(i64, String)> {
        db.query_unpaged(
            "SELECT msg_seq, body FROM xmsg.messages WHERE channel_id = ? AND bucket = 0",
            (channel,),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i64, String)>()
        .unwrap()
        .map(Result::unwrap)
        .collect()
    };
    let expected_messages = vec![
        (3, "three".to_string()),
        (2, "two".to_string()),
        (1, "one".to_string()),
    ];
    assert_eq!(
        messages().await,
        expected_messages,
        "newest first (clustering order)"
    );

    let members = async || -> Vec<(i64, String)> {
        db.query_unpaged(
            "SELECT user_id, role FROM xmsg.members_by_channel WHERE channel_id = ?",
            (channel,),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .rows::<(i64, String)>()
        .unwrap()
        .map(Result::unwrap)
        .collect()
    };
    assert_eq!(
        members().await,
        vec![(1, "admin".to_string()), (3, "member".to_string())]
    );

    let users_channel = async |user: i64| -> Option<String> {
        db.query_unpaged(
            "SELECT fanout_mode FROM xmsg.channels_by_user WHERE user_id = ? AND workspace_id = 10 AND channel_id = ?",
            (user, channel),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .maybe_first_row::<(String,)>()
        .unwrap()
        .map(|(m,)| m)
    };
    assert_eq!(
        users_channel(1).await.as_deref(),
        Some("pull"),
        "mode switch reaches channels_by_user"
    );
    assert_eq!(users_channel(2).await, None, "removed member's row is gone");

    let pointer = db
        .query_unpaged(
            "SELECT type, payload FROM xmsg.channel_events WHERE channel_id = ? AND bucket = 0 AND event_seq = 2",
            (channel,),
        )
        .await
        .unwrap()
        .into_rows_result()
        .unwrap()
        .first_row::<(String, Vec<u8>)>()
        .unwrap();
    assert_eq!(
        pointer,
        ("message.created".to_string(), 1_i64.to_be_bytes().to_vec()),
        "a pointer, not the body"
    );

    // Replay everything (as after a crash before committing offsets): state must not change.
    drop(persister);
    produce_all(&events).await;
    let _persister = start_persister(&topic, &format!("it-persister-replay-{run}"), 4);
    tokio::time::sleep(Duration::from_secs(5)).await;
    wait_for(
        "channel head after replay",
        (7, 3, 6, "pull".to_string(), "private".to_string()),
        head,
    )
    .await;
    assert_eq!(messages().await, expected_messages);
    assert_eq!(
        members().await,
        vec![(1, "admin".to_string()), (3, "member".to_string())]
    );
}
