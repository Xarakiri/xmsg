// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-media: the files API (ADR-0024). `xmsg-media garage-init` sets up a bundled Garage
//! (layout, key, bucket) and exits; the Small tier runs it as a one-shot job.

use std::sync::Arc;

use tokio::sync::Semaphore;
use tracing_subscriber::EnvFilter;
use xmsg_media::api::{App, router};
use xmsg_media::cassandra::Cassandra;
use xmsg_media::garage::{Garage, Setup};
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
    let store = Cassandra::connect(&nodes, &get("XMSG_CQL_KEYSPACE", "xmsg"))
        .await
        .unwrap_or_else(|e| fail(&e));
    let blobs = S3::new(
        &get("XMSG_MEDIA_S3_ENDPOINT", "http://127.0.0.1:3900"),
        &bucket,
        &get("XMSG_MEDIA_S3_REGION", "garage"),
        &key_id,
        &secret,
    );
    let verifier = xmsg_authn::Verifier::from_env(required("XMSG_AUTH_JWKS_URL"))
        .await
        .unwrap_or_else(|e| fail(&e));
    let app = Arc::new(App {
        store,
        blobs,
        verifier,
        max_bytes,
        uploads: Semaphore::new(concurrent.max(1)),
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
