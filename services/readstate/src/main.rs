// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-readstate: the owner (applies `readstate.events`, the only writer of
//! `read_state`), the feeder (`messages.persisted` -> `readstate.events`) and the HTTP API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{ClientConfig, ClientContext, Message as _};
use tracing_subscriber::EnvFilter;
use xmsg_proto::Message;
use xmsg_proto::v1::{ReadStateEvent, SequencedEvent, sequenced_event::Payload};
use xmsg_readstate::api::{App, Events, router};
use xmsg_readstate::cassandra::Cassandra;
use xmsg_readstate::{TOPIC, apply, feed, mentioned};

struct Kafka(FutureProducer);

impl Kafka {
    /// Enqueues without waiting for the delivery (retried by librdkafka meanwhile).
    async fn enqueue(&self, event: &ReadStateEvent) -> rdkafka::producer::DeliveryFuture {
        let key = event.user_id.to_be_bytes();
        let payload = event.encode_to_vec();
        loop {
            match self
                .0
                .send_result(FutureRecord::to(TOPIC).key(&key[..]).payload(&payload))
            {
                Ok(delivery) => return delivery,
                // The local queue is full: let it drain.
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    }
}

impl Events for Kafka {
    async fn produce(&self, event: &ReadStateEvent) -> Result<(), String> {
        // Keyed by user: one partition, one owner, in order.
        let key = event.user_id.to_be_bytes();
        let payload = event.encode_to_vec();
        self.0
            .send(
                FutureRecord::to(TOPIC).key(&key[..]).payload(&payload),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

fn fail(message: &str) -> ! {
    eprintln!("xmsg-readstate: {message}");
    std::process::exit(2);
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct Readiness {
    group: String,
    assigned: tokio::sync::watch::Sender<bool>,
}

impl ClientContext for Readiness {}
impl ConsumerContext for Readiness {
    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = rebalance {
            let partitions: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
            tracing::info!(group = %self.group, ?partitions, "partitions assigned");
            let _ = self.assigned.send(true);
        }
    }
}

fn consumer(
    bootstrap: &str,
    group: &str,
    topic: &str,
    assigned: tokio::sync::watch::Sender<bool>,
) -> StreamConsumer<Readiness> {
    let consumer: StreamConsumer<Readiness> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", group)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        // Offsets are stored explicitly (after the work) and committed by us.
        .set("enable.auto.offset.store", "false")
        .create_with_context(Readiness {
            group: group.to_string(),
            assigned,
        })
        .unwrap_or_else(|e| fail(&format!("Kafka consumer: {e}")));
    consumer
        .subscribe(&[topic])
        .unwrap_or_else(|e| fail(&format!("subscribing to {topic}: {e}")));
    consumer
}

/// The owner: applies each user's events in partition order. Offsets are committed after
/// the write, so a crash re-applies a suffix in order, which converges to the same position.
async fn own(bootstrap: String, store: Arc<Cassandra>, assigned: tokio::sync::watch::Sender<bool>) {
    let consumer = consumer(&bootstrap, "xmsg-readstate-owner", TOPIC, assigned);
    loop {
        let message = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "owner receive error");
                continue;
            }
        };
        if let Ok(event) = ReadStateEvent::decode(message.payload().unwrap_or_default())
            && let Some(change) = &event.change
        {
            let mut backoff = Duration::from_millis(100);
            loop {
                let result = async {
                    let current = store.get(event.user_id, event.channel_id).await?;
                    let next = apply(current, change);
                    if next != current {
                        store.set(event.user_id, event.channel_id, next).await?;
                    }
                    Ok::<_, String>(())
                }
                .await;
                match result {
                    Ok(()) => break,
                    Err(e) => {
                        tracing::warn!(error = %e, "read_state write failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
        }
        let _ = consumer.commit_message(&message, CommitMode::Async);
    }
}

/// The feeder: turns own sends and joins into events. Never writes `read_state`.
///
/// It only produces `read_up_to`, which the owner merges with max, so their order
/// doesn't matter: produces are enqueued without waiting, and every second (or 1,000
/// records) the feeder waits for their deliveries, re-sends any that failed, and only
/// then commits its offsets, so nothing is lost on a crash.
async fn feeder(
    bootstrap: String,
    store: Arc<Cassandra>,
    events: Arc<Kafka>,
    assigned: tokio::sync::watch::Sender<bool>,
) {
    let consumer = consumer(
        &bootstrap,
        "xmsg-readstate-feeder",
        "messages.persisted",
        assigned,
    );
    // Each channel's latest msg_seq, learnt from its messages in order (channel_heads
    // fills in channels seen first through a join after a restart).
    let mut latest: HashMap<i64, i64> = HashMap::new();
    let mut inflight: Vec<(ReadStateEvent, rdkafka::producer::DeliveryFuture)> = Vec::new();
    let mut since_flush = 0usize;
    let mut last_flush = std::time::Instant::now();
    loop {
        let received = tokio::time::timeout(Duration::from_millis(500), consumer.recv()).await;
        match received {
            Ok(Ok(message)) => {
                if let Ok(event) = SequencedEvent::decode(message.payload().unwrap_or_default()) {
                    let channel_msg_seq = match (latest.get(&event.channel_id), &event.payload) {
                        (Some(seq), _) => *seq,
                        (None, Some(Payload::MemberJoined(_))) => {
                            store.head_msg_seq(event.channel_id).await.unwrap_or(0)
                        }
                        (None, _) => 0,
                    };
                    // Mention rows before the offset can be stored: a crash repeats them, harmlessly.
                    for &user in mentioned(&event) {
                        while let Err(e) = store
                            .insert_mention(user, event.channel_id, event.msg_seq)
                            .await
                        {
                            tracing::warn!(error = %e, "inserting a mention failed; retrying");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                    for out in feed(&event, channel_msg_seq, now_ms()) {
                        let delivery = events.enqueue(&out).await;
                        inflight.push((out, delivery));
                    }
                    if matches!(event.payload, Some(Payload::MessageCreated(_))) {
                        latest.insert(event.channel_id, event.msg_seq);
                    }
                }
                // Stored now, committed only at the next flush (after the deliveries).
                let _ = consumer.store_offset_from_message(&message);
                since_flush += 1;
            }
            Ok(Err(e)) => tracing::warn!(error = %e, "feeder receive error"),
            Err(_) => {} // idle: still flush below
        }
        if since_flush > 0
            && (since_flush >= 1_000 || last_flush.elapsed() >= Duration::from_secs(1))
        {
            for (event, delivery) in inflight.drain(..) {
                if !matches!(delivery.await, Ok(Ok(_))) {
                    while let Err(e) = events.produce(&event).await {
                        tracing::warn!(error = %e, "producing a readstate event failed; retrying");
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
            if let Err(e) = consumer.commit_consumer_state(CommitMode::Async) {
                tracing::warn!(error = %e, "feeder offset commit failed (records will be fed again, harmlessly)");
            }
            since_flush = 0;
            last_flush = std::time::Instant::now();
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let get =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let bootstrap = get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let nodes: Vec<String> = get("XMSG_CQL_NODES", "127.0.0.1:9042")
        .split(',')
        .map(str::to_string)
        .collect();
    let port: u16 = get("XMSG_READSTATE_PORT", "8085")
        .parse()
        .unwrap_or_else(|_| fail("XMSG_READSTATE_PORT must be a port"));
    let jwks = std::env::var("XMSG_AUTH_JWKS_URL")
        .unwrap_or_else(|_| fail("XMSG_AUTH_JWKS_URL must be set"));

    let store = Arc::new(
        Cassandra::connect(&nodes, &get("XMSG_CQL_KEYSPACE", "xmsg"))
            .await
            .unwrap_or_else(|e| fail(&e)),
    );
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "5")
        .create()
        .unwrap_or_else(|e| fail(&format!("Kafka producer: {e}")));
    let events = Arc::new(Kafka(producer));

    let (owner_assigned, mut owner_ready) = tokio::sync::watch::channel(false);
    let (feeder_assigned, mut feeder_ready) = tokio::sync::watch::channel(false);
    tokio::spawn(own(bootstrap.clone(), store.clone(), owner_assigned));
    tokio::spawn(feeder(
        bootstrap,
        store.clone(),
        events.clone(),
        feeder_assigned,
    ));
    tokio::spawn(async move {
        owner_ready.changed().await.expect("owner consumer dropped");
        feeder_ready
            .changed()
            .await
            .expect("feeder consumer dropped");
        tracing::info!(port, "readstate ready");
    });

    let verifier = xmsg_authn::Verifier::from_env(jwks)
        .await
        .unwrap_or_else(|e| fail(&e));
    let app = Arc::new(App {
        positions: StoreRef(store),
        events: EventsRef(events),
        verifier,
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{port}: {e}")));
    axum::serve(listener, router(app))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server");
}

/// Shared handles for the API (the owner and feeder hold the same Arcs).
struct StoreRef(Arc<Cassandra>);
impl xmsg_readstate::cassandra::Positions for StoreRef {
    async fn positions(&self, user_id: i64) -> Result<Vec<(i64, i64)>, String> {
        xmsg_readstate::cassandra::Positions::positions(&*self.0, user_id).await
    }
    async fn mentions(&self, user_id: i64) -> Result<Vec<(i64, Vec<i64>)>, String> {
        xmsg_readstate::cassandra::Positions::mentions(&*self.0, user_id).await
    }
}
struct EventsRef(Arc<Kafka>);
impl Events for EventsRef {
    async fn produce(&self, event: &ReadStateEvent) -> Result<(), String> {
        self.0.produce(event).await
    }
}

/// SIGINT or SIGTERM (Kubernetes and systemd stop services with SIGTERM).
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
