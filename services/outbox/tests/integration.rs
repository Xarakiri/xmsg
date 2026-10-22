// SPDX-License-Identifier: AGPL-3.0-only
//! Integration test against real PostgreSQL (Small-tier schema) and Kafka.
//!
//! Ignored by default; run with
//!   XMSG_IT_PG=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg XMSG_IT_KAFKA=127.0.0.1:9092 \
//!     cargo test -p xmsg-outbox --test integration -- --ignored --test-threads=1
//! Runs the real binaries. Nothing else may be relaying the same outbox meanwhile.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::{Offset, TopicPartitionList};
use sqlx::PgPool;
use xmsg_outbox::postgres::Leader;
use xmsg_proto::Message;
use xmsg_proto::v1::{ChannelCreated, ChannelType, InboundEvent, inbound_event};

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for integration tests"))
}

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn run(bin: &str) -> Running {
    Running(
        Command::new(bin)
            .env("XMSG_DATABASE_URL", env("XMSG_IT_PG"))
            .env("XMSG_KAFKA_BOOTSTRAP", env("XMSG_IT_KAFKA"))
            .env(
                "XMSG_PARTITIONS",
                std::env::var("XMSG_PARTITIONS").unwrap_or_else(|_| "16".into()),
            )
            .env("XMSG_OUTBOX_POLL_MS", "50")
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("starting relay binary"),
    )
}

/// A consumer positioned at the current end of every partition of `topic`.
fn tail(topic: &str) -> BaseConsumer {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", env("XMSG_IT_KAFKA"))
        .set("group.id", "xmsg-outbox-it")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let md = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(10))
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for p in md.topics()[0].partitions() {
        let (_, high) = consumer
            .fetch_watermarks(topic, p.id(), Duration::from_secs(10))
            .unwrap();
        tpl.add_partition_offset(topic, p.id(), Offset::Offset(high))
            .unwrap();
    }
    consumer.assign(&tpl).unwrap();
    consumer
}

/// Waits for a record on `consumer` whose payload decodes to an event for `channel_id`.
fn wait_for(consumer: &BaseConsumer, channel_id: i64) -> InboundEvent {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(200))
            && let Ok(e) = InboundEvent::decode(m.payload().unwrap_or_default())
            && e.channel_id == channel_id
        {
            assert_eq!(
                m.key(),
                Some(&xmsg_partition::channel_key(channel_id)[..]),
                "keyed by channel"
            );
            return e;
        }
    }
    panic!("no event for channel {channel_id} within 30 s");
}

#[tokio::test]
#[ignore]
async fn only_one_poller_is_active_at_a_time() {
    let url = env("XMSG_IT_PG");
    let first = Leader::try_acquire(&url).await.unwrap();
    assert!(
        first.is_some(),
        "the lock is free (is another relay running?)"
    );
    assert!(
        Leader::try_acquire(&url).await.unwrap().is_none(),
        "a second poller stands by"
    );
    drop(first); // closing the connection releases the session lock
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if Leader::try_acquire(&url).await.unwrap().is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "the lock was not released");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[ignore]
async fn committed_channel_events_reach_messages_in_and_are_marked_published() {
    let pool = PgPool::connect(&env("XMSG_IT_PG")).await.unwrap();
    let channel_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
        & i64::MAX;
    let event = InboundEvent {
        channel_id,
        workspace_id: 1,
        actor_id: 7,
        dedupe_id: format!("create:{channel_id}"),
        payload: Some(inbound_event::Payload::ChannelCreated(ChannelCreated {
            r#type: ChannelType::Private as i32,
            initial_member_ids: vec![7],
            ..Default::default()
        })),
        ..Default::default()
    };
    let meta = tail("channels.meta");
    let messages_in = tail("messages.in");

    // A row only a future relay knows must stay where it is, and not hold up the others.
    let unknown_id: i64 = sqlx::query_scalar(
        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('from-the-future', 1, 'x', '') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    // As the channels service commits it (without a fast path: the relay is the only route here).
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO outbox (aggregate_type, aggregate_id, event_type, payload) VALUES ('channel', $1, 'channel.created', $2) RETURNING id",
    )
    .bind(channel_id)
    .bind(event.encode_to_vec())
    .fetch_one(&pool)
    .await
    .unwrap();

    let _poller = run(env!("CARGO_BIN_EXE_xmsg-outbox-relay"));
    let _meta_relay = run(env!("CARGO_BIN_EXE_xmsg-meta-relay"));

    assert_eq!(
        wait_for(&meta, channel_id),
        event,
        "outbox -> channels.meta, unchanged"
    );
    assert_eq!(
        wait_for(&messages_in, channel_id),
        event,
        "channels.meta -> messages.in, unchanged"
    );

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let published: Option<bool> =
            sqlx::query_scalar("SELECT published_at IS NOT NULL FROM outbox WHERE id = $1")
                .bind(id)
                .fetch_optional(&pool)
                .await
                .unwrap();
        if published == Some(true) {
            break;
        }
        assert!(Instant::now() < deadline, "row {id} not marked published");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let unknown_published: bool =
        sqlx::query_scalar("SELECT published_at IS NOT NULL FROM outbox WHERE id = $1")
            .bind(unknown_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        !unknown_published,
        "an unknown aggregate type is left for a newer relay"
    );
    sqlx::query("DELETE FROM outbox WHERE id = $1")
        .bind(unknown_id)
        .execute(&pool)
        .await
        .unwrap();
}
