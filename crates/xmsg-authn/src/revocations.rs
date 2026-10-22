// SPDX-License-Identifier: AGPL-3.0-only
//! Revoked sessions for HTTP services (plan §6.4), the same deny set the gateway keeps.
//! Fed by NATS `auth.revocations` (auth's fast path, and meta-relay's copy of the
//! outbox) and, after a (re)start, rebuilt from the last minutes of Kafka `auth.events`:
//! a session revoked before this process started must stay refused too.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use xmsg_proto::Message;
use xmsg_proto::v1::{AuthEvent, auth_event};

pub const SUBJECT: &str = "auth.revocations";
pub const TOPIC: &str = "auth.events";
/// Longer than the access-token lifetime (15 min) plus verify leeway: older revocations
/// can't matter any more.
const REPLAY_WINDOW: Duration = Duration::from_secs(16 * 60);

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// `session_id -> refuse_until_ms`. Session 0 (development tokens) is never revoked.
#[derive(Default)]
pub struct Revocations {
    denied: RwLock<HashMap<i64, i64>>,
}

impl Revocations {
    pub fn is_revoked(&self, session_id: i64) -> bool {
        session_id != 0
            && self
                .denied
                .read()
                .unwrap()
                .get(&session_id)
                .is_some_and(|until| now_ms() < *until)
    }

    /// Applies an encoded `AuthEvent`; anything else is ignored. Repeats change nothing,
    /// and a later deadline wins.
    pub fn apply(&self, bytes: &[u8]) {
        let Ok(AuthEvent {
            event: Some(auth_event::Event::SessionRevoked(r)),
        }) = AuthEvent::decode(bytes)
        else {
            return;
        };
        if r.session_id == 0 || r.refuse_until_ms <= now_ms() {
            return;
        }
        let mut denied = self.denied.write().unwrap();
        let until = denied.entry(r.session_id).or_insert(r.refuse_until_ms);
        *until = (*until).max(r.refuse_until_ms);
        // Drop expired entries now and then, so the map only holds the last ~16 minutes.
        if denied.len().is_multiple_of(1024) {
            let now = now_ms();
            denied.retain(|_, until| *until > now);
        }
    }

    /// Subscribes to NATS first (so nothing published from now on is missed), then replays
    /// `auth.events` from Kafka when a bootstrap is given. Returns once both are in place.
    pub async fn start(nats_url: &str, kafka_bootstrap: Option<&str>) -> Result<Arc<Self>, String> {
        let me = Arc::new(Revocations::default());
        let client = async_nats::connect(nats_url)
            .await
            .map_err(|e| format!("connecting to NATS at {nats_url}: {e}"))?;
        let mut sub = client
            .subscribe(SUBJECT)
            .await
            .map_err(|e| format!("subscribing to {SUBJECT}: {e}"))?;
        let live = me.clone();
        tokio::spawn(async move {
            // The client is kept alive here; async-nats resubscribes after reconnects.
            let _client = client;
            while let Some(msg) = sub.next().await {
                live.apply(&msg.payload);
            }
        });
        if let Some(bootstrap) = kafka_bootstrap {
            let (bootstrap, target) = (bootstrap.to_string(), me.clone());
            let replayed = tokio::task::spawn_blocking(move || replay(&bootstrap, &target))
                .await
                .map_err(|e| e.to_string())??;
            tracing::info!(replayed, "revocations replayed from auth.events");
        }
        Ok(me)
    }
}

/// Reads `auth.events` from `REPLAY_WINDOW` ago to the current end of every partition.
fn replay(bootstrap: &str, into: &Revocations) -> Result<usize, String> {
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::{ClientConfig, Message as _, Offset, TopicPartitionList};

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "xmsg-authn-replay-unused")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| format!("Kafka consumer: {e}"))?;
    let timeout = Duration::from_secs(10);
    let md = consumer
        .fetch_metadata(Some(TOPIC), timeout)
        .map_err(|e| format!("metadata of {TOPIC}: {e}"))?;
    let partitions: Vec<i32> = match md.topics().first() {
        Some(t) if t.error().is_none() => t.partitions().iter().map(|p| p.id()).collect(),
        _ => return Err(format!("topic {TOPIC} does not exist")),
    };
    let since = now_ms() - REPLAY_WINDOW.as_millis() as i64;
    let mut by_time = TopicPartitionList::new();
    for &p in &partitions {
        by_time
            .add_partition_offset(TOPIC, p, Offset::Offset(since))
            .map_err(|e| e.to_string())?;
    }
    let starts = consumer
        .offsets_for_times(by_time, timeout)
        .map_err(|e| format!("offsets_for_times: {e}"))?;
    let mut ends = HashMap::new();
    let mut assignment = TopicPartitionList::new();
    for &p in &partitions {
        let (_, high) = consumer
            .fetch_watermarks(TOPIC, p, timeout)
            .map_err(|e| e.to_string())?;
        // No record at or after `since`: the partition holds nothing worth replaying.
        if let Some(Offset::Offset(start)) = starts.find_partition(TOPIC, p).map(|e| e.offset())
            && start < high
        {
            assignment
                .add_partition_offset(TOPIC, p, Offset::Offset(start))
                .map_err(|e| e.to_string())?;
            ends.insert(p, high);
        }
    }
    if ends.is_empty() {
        return Ok(0);
    }
    consumer.assign(&assignment).map_err(|e| e.to_string())?;
    let mut seen = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !ends.is_empty() && std::time::Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(200)) {
            into.apply(m.payload().unwrap_or_default());
            seen += 1;
            if ends
                .get(&m.partition())
                .is_some_and(|high| m.offset() + 1 >= *high)
            {
                ends.remove(&m.partition());
            }
        }
    }
    Ok(seen)
}

#[cfg(test)]
mod tests {
    use super::*;
    use xmsg_proto::v1::SessionRevoked;

    fn revoked(session_id: i64, refuse_until_ms: i64) -> Vec<u8> {
        AuthEvent {
            event: Some(auth_event::Event::SessionRevoked(SessionRevoked {
                user_id: 1,
                session_id,
                refuse_until_ms,
                ..Default::default()
            })),
        }
        .encode_to_vec()
    }

    #[test]
    fn revoked_sessions_are_refused_until_their_tokens_expire() {
        let r = Revocations::default();
        r.apply(&revoked(5, now_ms() + 60_000));
        r.apply(&revoked(6, now_ms() - 1));
        assert!(r.is_revoked(5));
        assert!(!r.is_revoked(6), "already expired");
        assert!(!r.is_revoked(7), "never revoked");
    }

    #[test]
    fn repeats_keep_the_later_deadline_and_garbage_or_dev_sessions_are_ignored() {
        let r = Revocations::default();
        r.apply(&revoked(5, now_ms() + 600_000));
        r.apply(&revoked(5, now_ms() + 1_000));
        assert!(*r.denied.read().unwrap().get(&5).unwrap() > now_ms() + 500_000);
        r.apply(b"\xff\xff");
        r.apply(&revoked(0, now_ms() + 60_000));
        assert!(!r.is_revoked(0));
    }
}
