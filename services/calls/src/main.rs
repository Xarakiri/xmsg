// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-calls: LiveKit access tokens and webhooks (ADR-0025).

use std::sync::Arc;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing_subscriber::EnvFilter;
use xmsg_calls::EventLog;
use xmsg_calls::api::{App, router};
use xmsg_calls::cassandra::Cassandra;
use xmsg_calls::livekit::Keys;

fn fail(message: &str) -> ! {
    eprintln!("xmsg-calls: {message}");
    std::process::exit(2);
}

fn get(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn required(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| fail(&format!("{name} must be set")))
}

/// `messages.in`, keyed by channel with Kafka's default partitioner, like every producer
/// (ADR-0004).
struct Kafka(FutureProducer);

impl EventLog for Kafka {
    async fn append(&self, channel_id: i64, event: Vec<u8>) -> Result<(), String> {
        let key = xmsg_partition::channel_key(channel_id);
        self.0
            .send(
                FutureRecord::to("messages.in")
                    .key(&key[..])
                    .payload(&event),
                Duration::from_secs(5),
            )
            .await
            .map(|_| ())
            .map_err(|(e, _)| e.to_string())
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let nodes: Vec<String> = get("XMSG_CQL_NODES", "127.0.0.1:9042")
        .split(',')
        .map(str::to_string)
        .collect();
    let port: u16 = get("XMSG_CALLS_PORT", "8088")
        .parse()
        .unwrap_or_else(|_| fail("XMSG_CALLS_PORT must be a port"));
    let keys = Keys {
        api_key: required("XMSG_LIVEKIT_API_KEY"),
        api_secret: required("XMSG_LIVEKIT_API_SECRET"),
    };
    if keys.api_secret.len() < 32 {
        fail(
            "XMSG_LIVEKIT_API_SECRET must be at least 32 characters (LiveKit refuses shorter ones)",
        );
    }
    let calls = Cassandra::connect(&nodes, &get("XMSG_CQL_KEYSPACE", "xmsg"))
        .await
        .unwrap_or_else(|e| fail(&e));
    let producer: FutureProducer = ClientConfig::new()
        .set(
            "bootstrap.servers",
            get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
        )
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("linger.ms", "1")
        .set("partitioner", "murmur2_random")
        .create()
        .unwrap_or_else(|e| fail(&format!("Kafka producer: {e}")));
    let verifier = xmsg_authn::Verifier::from_env(required("XMSG_AUTH_JWKS_URL"))
        .await
        .unwrap_or_else(|e| fail(&e));
    let app = Arc::new(App {
        calls,
        log: Kafka(producer),
        verifier,
        keys,
        livekit_url: required("XMSG_LIVEKIT_PUBLIC_URL"),
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{port}: {e}")));
    tracing::info!(port, "calls ready");
    axum::serve(listener, router(app))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server");
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("installing the SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}
