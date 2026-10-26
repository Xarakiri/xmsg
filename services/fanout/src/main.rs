// SPDX-License-Identifier: AGPL-3.0-only
use std::process::ExitCode;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::{ClientConfig, ClientContext, Message as _, Offset, TopicPartitionList};
use tracing_subscriber::EnvFilter;
use xmsg_fanout::config::Config;
use xmsg_fanout::denylist::DenyList;
use xmsg_fanout::membership::{Fallback, Membership};
use xmsg_fanout::sequencer::Sequencer;
use xmsg_fanout::sessions::SessionMap;
use xmsg_fanout::store::Store;
use xmsg_fanout::streams::Streams;
use xmsg_fanout::{by_gateway, plan, typing_recipients};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    Delivery, GatewayBatch, PullControl, ReadReceipt, SequencedEvent, ServerFrame, SessionEvent,
    TypingIndicator, sequenced_event::Payload, server_frame,
};

const EVENTS_TOPIC: &str = "messages.persisted";
const SESSIONS_TOPIC: &str = "sessions.events";
const USERS_EVENTS_TOPIC: &str = "users.events";

/// Records partitions assigned in a rebalance, so their streams get a new epoch.
struct Ctx {
    assigned: Arc<Mutex<Vec<i32>>>,
    /// Partitions this instance owns now: typing for any other is someone else's.
    owned: Arc<Mutex<std::collections::HashSet<i32>>>,
}
impl ClientContext for Ctx {}
impl ConsumerContext for Ctx {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                let mut assigned = self.assigned.lock().unwrap();
                assigned.extend(tpl.elements().iter().map(|e| e.partition()));
                self.owned
                    .lock()
                    .unwrap()
                    .extend(tpl.elements().iter().map(|e| e.partition()));
                tracing::info!(partitions = ?tpl.elements().iter().map(|e| e.partition()).collect::<Vec<_>>(), "partitions assigned");
            }
            Rebalance::Revoke(tpl) => {
                let mut owned = self.owned.lock().unwrap();
                for e in tpl.elements() {
                    owned.remove(&e.partition());
                }
            }
            Rebalance::Error(_) => {}
        }
    }
}

/// Starts new stream epochs for (re)assigned partitions. "Ready" is announced on the first
/// assignment: a new consumer group starts at the latest offset, so events produced before
/// it aren't delivered.
fn take_assignments(assigned: &Mutex<Vec<i32>>, streams: &mut Streams, announced: &mut bool) {
    let partitions: Vec<i32> = assigned.lock().unwrap().drain(..).collect();
    for partition in &partitions {
        streams.reset_partition(*partition as u32, now_ms());
    }
    if !partitions.is_empty() && !*announced {
        tracing::info!("fanout ready");
        *announced = true;
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };

    let store = match Store::connect(&config.cql_nodes, &config.keyspace).await {
        Ok(s) => s,
        Err(e) => return fail(&e),
    };
    // Snapshots: the wide-column store, and the sequencer when the store is behind (§5.6).
    let snapshots = Fallback {
        primary: store,
        secondary: config.sequencer_url.clone().map(Sequencer::new),
    };
    let nats = match async_nats::connect(&config.nats_url).await {
        Ok(c) => c,
        Err(e) => return fail(&format!("connecting to NATS at {}: {e}", config.nats_url)),
    };

    // Sessions: read the whole compacted topic, then keep following it.
    let sessions = Arc::new(RwLock::new(SessionMap::default()));
    let tracked = sessions.clone();
    let sessions_caught_up = match follow(
        &config.kafka_bootstrap,
        SESSIONS_TOPIC,
        move |key, payload| {
            let key = key
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .unwrap_or_default();
            let event = payload
                .filter(|p| !p.is_empty())
                .and_then(|p| SessionEvent::decode(p).ok());
            tracked.write().unwrap().apply(&key, event);
        },
    ) {
        Ok(rx) => rx,
        Err(e) => return fail(&e),
    };
    // Workspace removals (plan §6.5): the outbox copy, from the start of its 7-day
    // retention, plus the users service's fast path on NATS.
    let deny = Arc::new(DenyList::default());
    let from_outbox = deny.clone();
    let removals_caught_up = match follow(
        &config.kafka_bootstrap,
        USERS_EVENTS_TOPIC,
        move |_, payload| {
            from_outbox.apply_users_event(payload.unwrap_or_default());
        },
    ) {
        Ok(rx) => rx,
        Err(e) => return fail(&e),
    };
    match nats.subscribe("auth.revocations").await {
        Ok(mut sub) => {
            let fast = deny.clone();
            tokio::spawn(async move {
                use futures_util::StreamExt;
                while let Some(msg) = sub.next().await {
                    fast.apply_auth_event(&msg.payload);
                }
            });
        }
        Err(e) => return fail(&format!("subscribing to auth.revocations: {e}")),
    }
    // Nothing is delivered before both are loaded: a restarted fan-out must not leak a
    // removed workspace's events to its former member.
    let deadline = Instant::now() + Duration::from_secs(60);
    while !*sessions_caught_up.borrow() || !*removals_caught_up.borrow() {
        if Instant::now() > deadline {
            return fail("timed out reading sessions.events / users.events");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tracing::info!(
        connections = sessions.read().unwrap().connection_count(),
        "session map loaded"
    );

    let assigned = Arc::new(Mutex::new(Vec::new()));
    let owned = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let consumer: StreamConsumer<Ctx> = match ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_bootstrap)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create_with_context(Ctx {
            assigned: assigned.clone(),
            owned: owned.clone(),
        }) {
        Ok(c) => c,
        Err(e) => return fail(&format!("creating Kafka consumer: {e}")),
    };
    match consumer.fetch_metadata(Some(EVENTS_TOPIC), Duration::from_secs(10)) {
        Ok(md) => match md.topics().first() {
            Some(t) if t.error().is_none() && t.partitions().len() as i32 == config.partitions => {}
            Some(t) if t.error().is_none() => {
                return fail(&format!(
                    "{EVENTS_TOPIC} has {} partitions but XMSG_PARTITIONS is {} (ADR-0021)",
                    t.partitions().len(),
                    config.partitions
                ));
            }
            _ => return fail(&format!("topic {EVENTS_TOPIC} does not exist")),
        },
        Err(e) => return fail(&format!("fetching metadata: {e}")),
    }
    if let Err(e) = consumer.subscribe(&[EVENTS_TOPIC]) {
        return fail(&format!("subscribing to {EVENTS_TOPIC}: {e}"));
    }
    // Typing (plan §5.9): every instance hears all partitions and keeps its own. Best-effort:
    // dropped unless the channel's members are already in memory.
    let mut typing = match nats.subscribe("typing.*").await {
        Ok(s) => s,
        Err(e) => return fail(&format!("subscribing to typing.*: {e}")),
    };
    // Read receipts (ADR-0012): the same path, for DMs and group DMs only.
    let mut receipts = match nats.subscribe("receipts.*").await {
        Ok(s) => s,
        Err(e) => return fail(&format!("subscribing to receipts.*: {e}")),
    };
    let mut membership = Membership::default();
    let mut streams = Streams::default();
    let mut heads_tick = tokio::time::interval(Duration::from_secs(1));
    let mut announced = false;
    // Pull-mode subscribers too new to rely on ch.{id} yet (ADR-0008).
    let mut fresh = xmsg_fanout::Fresh::default();
    // Channels that switched push/pull moments ago (ADR-0008).
    let mut switched: std::collections::HashMap<i64, std::time::Instant> =
        std::collections::HashMap::new();

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    loop {
        take_assignments(&assigned, &mut streams, &mut announced);
        let message = tokio::select! {
            m = consumer.recv() => m,
            _ = heads_tick.tick() => {
                for gateway in streams.gateways() {
                    let heads = streams.heads_for(&gateway).encode_to_vec();
                    let _ = nats.publish(format!("stream_heads.{gateway}"), heads.into()).await;
                }
                continue;
            }
            Some(msg) = futures_util::StreamExt::next(&mut typing) => {
                let partition = msg.subject.rsplit('.').next().and_then(|p| p.parse::<i32>().ok());
                if partition.is_some_and(|p| owned.lock().unwrap().contains(&p))
                    && let Ok(notice) = TypingIndicator::decode(msg.payload.as_ref())
                    && let Some(members) = membership.get(notice.channel_id)
                    && let Some(to) = typing_recipients(members, notice.user_id)
                {
                    let frame = server_frame::Frame::Typing(notice);
                    relay_ephemeral(&nats, &sessions, &to, frame).await;
                }
                continue;
            }
            Some(msg) = futures_util::StreamExt::next(&mut receipts) => {
                let partition = msg.subject.rsplit('.').next().and_then(|p| p.parse::<i32>().ok());
                if partition.is_some_and(|p| owned.lock().unwrap().contains(&p))
                    && let Ok(receipt) = ReadReceipt::decode(msg.payload.as_ref())
                    && receipt.msg_seq > 0
                    && membership.is_dm(receipt.channel_id)
                    && let Some(members) = membership.get(receipt.channel_id)
                    && let Some(to) = typing_recipients(members, receipt.user_id)
                {
                    relay_ephemeral(&nats, &sessions, &to, server_frame::Frame::Receipt(receipt)).await;
                }
                continue;
            }
            _ = &mut shutdown => break,
        };
        // Assignments arrive inside recv() (rebalance callback): take them before the message.
        take_assignments(&assigned, &mut streams, &mut announced);
        let message = match message {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "Kafka receive error");
                continue;
            }
        };
        let Some(Ok(event)) = message.payload().map(SequencedEvent::decode) else {
            let _ = consumer.store_offset_from_message(&message);
            continue;
        };

        // Membership must be known before delivering; the store may lag by milliseconds.
        let mut backoff = Duration::from_millis(20);
        loop {
            match membership.ensure_loaded(&event, &snapshots).await {
                Ok(()) => break,
                Err(reason) => {
                    if backoff >= Duration::from_secs(1) {
                        tracing::warn!(
                            ?reason,
                            channel_id = event.channel_id,
                            "waiting for membership snapshot"
                        );
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
                }
            }
        }
        membership.apply(&event);
        let members = membership
            .get(event.channel_id)
            .cloned()
            .unwrap_or_default();
        // Around a push/pull switch both paths are used for a while (ADR-0008).
        let now = std::time::Instant::now();
        if matches!(event.payload, Some(Payload::ChannelModeChanged(_))) {
            switched.retain(|_, at| now.duration_since(*at) < DUAL_PUBLISH);
            switched.insert(event.channel_id, now);
        }
        let dual = switched
            .get(&event.channel_id)
            .is_some_and(|at| now.duration_since(*at) < DUAL_PUBLISH);
        let mut route = plan(&event, &members, dual);
        // Just subscribed: their gateway may not be listening on ch.{id} yet.
        if route.publish_channel {
            route.push.extend(
                fresh
                    .users(event.channel_id, now)
                    .into_iter()
                    .filter(|u| members.contains(u)),
            );
        }
        deny.filter(event.workspace_id, &mut route.push);

        // Encoded once, the same bytes for every recipient on every gateway.
        let frame = ServerFrame {
            frame: Some(server_frame::Frame::Event(event.clone())),
        }
        .encode_to_vec();
        // Subscriptions first, so a joiner's gateway is listening as soon as it can be.
        send_controls(&nats, &sessions, &deny, &event, &route.controls).await;
        for &(user, subscribe) in &route.controls {
            if subscribe {
                fresh.note(event.channel_id, user, now);
            } else {
                fresh.forget(event.channel_id, user);
            }
        }
        if route.publish_channel
            && let Err(e) = nats
                .publish(format!("ch.{}", event.channel_id), frame.clone().into())
                .await
        {
            tracing::warn!(error = %e, channel_id = event.channel_id, "pull-mode publish failed; clients will resync");
        }

        let grouped = by_gateway(&route.push, &sessions.read().unwrap());
        if !grouped.is_empty() {
            let partition = message.partition() as u32;
            for (gateway, user_ids) in grouped {
                let (owner_epoch, seq) = streams.next(&gateway, partition, now_ms());
                let batch = GatewayBatch {
                    partition,
                    owner_epoch,
                    seq,
                    deliveries: vec![Delivery {
                        user_ids,
                        server_frame: frame.clone(),
                    }],
                };
                if let Err(e) = nats
                    .publish(format!("gw.{gateway}"), batch.encode_to_vec().into())
                    .await
                {
                    tracing::warn!(error = %e, %gateway, "publish failed; the gateway will see a gap and resync");
                }
            }
        }
        let _ = consumer.store_offset_from_message(&message);
    }
    let _ = nats.flush().await;
    ExitCode::SUCCESS
}

/// How long both paths are used after a push/pull switch (ADR-0008).
const DUAL_PUBLISH: Duration = Duration::from_secs(10);

/// Tells the gateways of each `(user, subscribe)` to start or stop the channel's pull-mode
/// subject. A user removed from the workspace is never subscribed.
async fn send_controls(
    nats: &async_nats::Client,
    sessions: &RwLock<SessionMap>,
    deny: &DenyList,
    event: &SequencedEvent,
    controls: &[(i64, bool)],
) {
    if controls.is_empty() {
        return;
    }
    // Bound first, so the sessions lock is released before any await.
    let mut messages = Vec::new();
    {
        let sessions = sessions.read().unwrap();
        for &(user_id, subscribe) in controls {
            if subscribe && deny.denied(user_id, event.workspace_id) {
                continue;
            }
            let control = PullControl {
                user_id,
                channel_id: event.channel_id,
                subscribe,
                workspace_id: event.workspace_id,
            }
            .encode_to_vec();
            for gateway in sessions.gateways_of(user_id) {
                messages.push((format!("gw.{gateway}.control"), control.clone()));
            }
        }
    }
    for (subject, body) in messages {
        let _ = nats.publish(subject, body.into()).await;
    }
}

/// Sends a best-effort frame (typing, receipts) to `to` on their gateways' unsequenced
/// `gw.{id}.ephemeral` subjects: a lost one is never a delivery gap.
async fn relay_ephemeral(
    nats: &async_nats::Client,
    sessions: &RwLock<SessionMap>,
    to: &std::collections::HashSet<i64>,
    frame: server_frame::Frame,
) {
    let frame = ServerFrame { frame: Some(frame) }.encode_to_vec();
    // Bound first, so the sessions lock is released before any await.
    let grouped = by_gateway(to, &sessions.read().unwrap());
    for (gateway, user_ids) in grouped {
        let delivery = Delivery {
            user_ids,
            server_frame: frame.clone(),
        };
        let _ = nats
            .publish(
                format!("gw.{gateway}.ephemeral"),
                delivery.encode_to_vec().into(),
            )
            .await;
    }
}

/// Follows `topic` from the beginning on a background thread, calling `apply(key, payload)`
/// for every record. The returned flag turns true once the initial read reached the end.
fn follow(
    bootstrap: &str,
    topic_name: &'static str,
    apply: impl Fn(Option<&[u8]>, Option<&[u8]>) + Send + 'static,
) -> Result<tokio::sync::watch::Receiver<bool>, String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "xmsg-fanout-follow-unused")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| format!("creating {topic_name} consumer: {e}"))?;
    let md = consumer
        .fetch_metadata(Some(topic_name), Duration::from_secs(10))
        .map_err(|e| format!("fetching {topic_name} metadata: {e}"))?;
    let topic = md
        .topics()
        .first()
        .filter(|t| t.error().is_none())
        .ok_or(format!("topic {topic_name} does not exist"))?;
    let mut tpl = TopicPartitionList::new();
    let mut targets = Vec::new();
    for p in topic.partitions() {
        tpl.add_partition_offset(topic_name, p.id(), Offset::Beginning)
            .map_err(|e| e.to_string())?;
        let (low, high) = consumer
            .fetch_watermarks(topic_name, p.id(), Duration::from_secs(10))
            .map_err(|e| format!("watermarks: {e}"))?;
        // Retention may have removed everything below `low`: then there is nothing to read.
        targets.push((p.id(), if high > low { high } else { 0 }));
    }
    consumer
        .assign(&tpl)
        .map_err(|e| format!("assigning {topic_name}: {e}"))?;

    let (tx, rx) = tokio::sync::watch::channel(targets.iter().all(|(_, high)| *high == 0));
    std::thread::spawn(move || {
        let mut remaining: std::collections::HashMap<i32, i64> =
            targets.into_iter().filter(|(_, h)| *h > 0).collect();
        loop {
            match consumer.poll(Duration::from_millis(200)) {
                Some(Ok(m)) => {
                    apply(m.key(), m.payload());
                    if let Some(high) = remaining.get(&m.partition())
                        && m.offset() + 1 >= *high
                    {
                        remaining.remove(&m.partition());
                    }
                }
                Some(Err(e)) => tracing::warn!(error = %e, topic = topic_name, "consumer error"),
                None => {}
            }
            if remaining.is_empty() && !*tx.borrow() {
                let _ = tx.send(true);
            }
        }
    });
    Ok(rx)
}

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-fanout: {message}");
    ExitCode::from(2)
}

/// SIGINT or SIGTERM. Kubernetes and systemd stop services with SIGTERM; without a
/// handler the process dies on the spot and skips the clean shutdown below.
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
