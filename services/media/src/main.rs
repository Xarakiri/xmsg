// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-media: the files API (ADR-0024). `xmsg-media garage-init` sets up a bundled Garage
//! (layout, key, bucket) and exits; the Small tier runs it as a one-shot job.

use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as _};
use xmsg_proto::Message as _;
use xmsg_proto::v1::SequencedEvent;

use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use xmsg_media::api::{App, router};
use xmsg_media::cassandra::Cassandra;
use xmsg_media::garage::{Garage, Setup};
use xmsg_media::lifecycle;
use xmsg_media::s3::S3;

fn fail(message: &str) -> ! {
    eprintln!("xmsg-media: {message}");
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

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let key_id = required("XMSG_MEDIA_S3_KEY_ID");
    let secret = required("XMSG_MEDIA_S3_SECRET");
    let bucket = get("XMSG_MEDIA_BUCKET", "xmsg-files");

    if std::env::args().nth(1).as_deref() == Some("garage-init") {
        let garage = Garage::new(
            &get("XMSG_GARAGE_ADMIN_URL", "http://127.0.0.1:3903"),
            &required("XMSG_GARAGE_ADMIN_TOKEN"),
        );
        let setup = Setup {
            key_id: &key_id,
            secret: &secret,
            bucket: &bucket,
            capacity: 1 << 40,
        };
        // Garage answers a moment after its container starts.
        let mut attempt = 0;
        let done = loop {
            match garage.setup(&setup).await {
                Ok(done) => break done,
                Err(e) if attempt < 30 => {
                    tracing::info!(error = %e, "Garage not ready yet");
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Err(e) => fail(&format!("setting up Garage: {e}")),
            }
        };
        for step in &done {
            tracing::info!("{step}");
        }
        tracing::info!(changes = done.len(), "Garage is set up");
        return;
    }

    let nodes: Vec<String> = get("XMSG_CQL_NODES", "127.0.0.1:9042")
        .split(',')
        .map(str::to_string)
        .collect();
    let port: u16 = get("XMSG_MEDIA_PORT", "8087")
        .parse()
        .unwrap_or_else(|_| fail("XMSG_MEDIA_PORT must be a port"));
    let max_bytes: usize = get("XMSG_MEDIA_MAX_BYTES", "26214400")
        .parse()
        .unwrap_or_else(|_| fail("XMSG_MEDIA_MAX_BYTES must be a number of bytes"));
    let concurrent: usize = get("XMSG_MEDIA_CONCURRENT_UPLOADS", "4")
        .parse()
        .unwrap_or_else(|_| fail("XMSG_MEDIA_CONCURRENT_UPLOADS must be a number"));
    let grace_secs: i64 = get("XMSG_MEDIA_PURGE_AFTER_SECS", "86400")
        .parse()
        .unwrap_or_else(|_| fail("XMSG_MEDIA_PURGE_AFTER_SECS must be a number of seconds"));
    let default_quota: i64 = get("XMSG_MEDIA_WORKSPACE_QUOTA_BYTES", "10737418240")
        .parse()
        .unwrap_or_else(|_| {
            fail("XMSG_MEDIA_WORKSPACE_QUOTA_BYTES must be a number of bytes (0: unlimited)")
        });
    let keyspace = get("XMSG_CQL_KEYSPACE", "xmsg");
    let mut store = Cassandra::connect(&nodes, &keyspace)
        .await
        .unwrap_or_else(|e| fail(&e));
    // Quota overrides and workspace roles (ADR-0024); without Postgres every workspace
    // gets the default and nobody can read usage.
    match std::env::var("XMSG_DATABASE_URL")
        .ok()
        .filter(|u| !u.is_empty())
    {
        Some(url) => {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect_lazy(&url)
                .unwrap_or_else(|e| fail(&format!("XMSG_DATABASE_URL: {e}")));
            store = store.with_postgres(pool);
        }
        None => tracing::warn!("XMSG_DATABASE_URL not set: every workspace gets the default quota"),
    }
    let lifecycle_store = Arc::new(
        Cassandra::connect(&nodes, &keyspace)
            .await
            .unwrap_or_else(|e| fail(&e)),
    );
    let blobs = S3::new(
        &get("XMSG_MEDIA_S3_ENDPOINT", "http://127.0.0.1:3900"),
        &bucket,
        &get("XMSG_MEDIA_S3_REGION", "garage"),
        &key_id,
        &secret,
    );
    let sweeper_blobs = S3::new(
        &get("XMSG_MEDIA_S3_ENDPOINT", "http://127.0.0.1:3900"),
        &bucket,
        &get("XMSG_MEDIA_S3_REGION", "garage"),
        &key_id,
        &secret,
    );
    tokio::spawn(follow(
        get("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
        lifecycle_store.clone(),
    ));
    tokio::spawn(sweeper(lifecycle_store, sweeper_blobs, grace_secs * 1000));
    let verifier = xmsg_authn::Verifier::from_env(required("XMSG_AUTH_JWKS_URL"))
        .await
        .unwrap_or_else(|e| fail(&e));
    let app = Arc::new(App {
        store,
        blobs,
        verifier,
        max_bytes,
        uploads: Semaphore::new(concurrent.max(1)),
        default_quota,
    });
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .unwrap_or_else(|e| fail(&format!("bind :{port}: {e}")));
    tracing::info!(port, max_bytes, "media ready");
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

/// Follows `messages.persisted` (ADR-0024): claims sent files, marks deleted messages'
/// files. Offsets are committed after the writes, so a crash replays (all idempotent).
async fn follow(bootstrap: String, store: Arc<Cassandra>) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "xmsg-media")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .create()
        .unwrap_or_else(|e| fail(&format!("Kafka consumer: {e}")));
    consumer
        .subscribe(&["messages.persisted"])
        .unwrap_or_else(|e| fail(&format!("subscribing to messages.persisted: {e}")));
    loop {
        let message = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "messages.persisted receive error");
                continue;
            }
        };
        if let Ok(event) = SequencedEvent::decode(message.payload().unwrap_or_default()) {
            let at = message
                .timestamp()
                .to_millis()
                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
            let mut backoff = Duration::from_millis(100);
            loop {
                match lifecycle::on_event(store.as_ref(), &event, at).await {
                    Ok(_) => break,
                    Err(e) => {
                        tracing::warn!(error = %e, "file lifecycle write failed; retrying");
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(5));
                    }
                }
            }
        }
        let _ = consumer.commit_message(&message, CommitMode::Async);
    }
}

/// Removes deleted files' bytes once their grace period is over, every few minutes.
async fn sweeper(store: Arc<Cassandra>, blobs: S3, grace_ms: i64) {
    let every = Duration::from_secs(if grace_ms < 600_000 { 5 } else { 300 });
    loop {
        tokio::time::sleep(every).await;
        match lifecycle::sweep(
            store.as_ref(),
            &blobs,
            chrono::Utc::now().timestamp_millis(),
            grace_ms,
        )
        .await
        {
            Ok(0) => {}
            Ok(n) => tracing::info!(files = n, "removed deleted files' bytes"),
            Err(e) => tracing::warn!(error = %e, "purge sweep failed"),
        }
    }
}
