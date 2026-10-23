// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-push: Web Push subscriptions (HTTP) and notifications for new messages (Kafka).

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{Consumer, StreamConsumer};
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use serde::Deserialize;
use serde_json::json;
use sqlx::postgres::PgPool;
use tracing_subscriber::EnvFilter;
use web_push::{ContentEncoding, SubscriptionInfo, VapidSignatureBuilder, WebPushMessageBuilder};
use xmsg_proto::Message;
use xmsg_proto::v1::{SequencedEvent, sequenced_event::Payload};
use xmsg_push::{LARGE_CHANNEL, Level, Recipient, payload, should_notify};

const TOPIC: &str = "messages.persisted";
/// How long a push service keeps an undelivered notification.
const PUSH_TTL_SECS: u32 = 4 * 3600;

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-push: {message}");
    ExitCode::from(2)
}

struct App {
    pg: PgPool,
    verifier: xmsg_authn::Verifier,
    vapid_public_key: String,
    allow_http: bool,
}

#[derive(Deserialize)]
struct Keys {
    p256dh: String,
    auth: String,
}

#[derive(Deserialize)]
struct Subscription {
    endpoint: String,
    keys: Keys,
}

#[derive(Deserialize)]
struct Unsubscribe {
    endpoint: String,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

/// The caller's user ID, or the status and message to answer.
async fn caller(app: &App, headers: &HeaderMap) -> Result<i64, (StatusCode, &'static str)> {
    const INVALID: (StatusCode, &str) =
        (StatusCode::UNAUTHORIZED, "invalid or missing access token");
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(INVALID)?;
    match app.verifier.verify(auth).await {
        Ok(p) => Ok(p.user_id),
        Err(xmsg_authn::Error::KeysUnavailable) => {
            Err((StatusCode::SERVICE_UNAVAILABLE, "try again later"))
        }
        Err(_) => Err(INVALID),
    }
}

/// The key browsers need to subscribe (`applicationServerKey`).
async fn vapid_key(State(app): State<Arc<App>>) -> Json<serde_json::Value> {
    Json(json!({ "key": app.vapid_public_key }))
}

async fn subscribe(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(s): Json<Subscription>,
) -> Response {
    let user = match caller(&app, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    let scheme_ok =
        s.endpoint.starts_with("https://") || (app.allow_http && s.endpoint.starts_with("http://"));
    if !scheme_ok || s.endpoint.len() > 2048 || s.keys.p256dh.is_empty() || s.keys.auth.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid subscription");
    }
    let stored = sqlx::query(
        "INSERT INTO push_endpoints (endpoint, user_id, p256dh, auth) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (endpoint) DO UPDATE SET user_id = $2, p256dh = $3, auth = $4",
    )
    .bind(&s.endpoint)
    .bind(user)
    .bind(&s.keys.p256dh)
    .bind(&s.keys.auth)
    .execute(&app.pg)
    .await;
    match stored {
        Ok(_) => StatusCode::CREATED.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "storing a push subscription failed");
            error(StatusCode::SERVICE_UNAVAILABLE, "try again later")
        }
    }
}

async fn unsubscribe(
    State(app): State<Arc<App>>,
    headers: HeaderMap,
    Json(u): Json<Unsubscribe>,
) -> Response {
    let user = match caller(&app, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    match sqlx::query("DELETE FROM push_endpoints WHERE endpoint = $1 AND user_id = $2")
        .bind(&u.endpoint)
        .bind(user)
        .execute(&app.pg)
        .await
    {
        Ok(_) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => {
            tracing::error!(error = %e, "deleting a push subscription failed");
            error(StatusCode::SERVICE_UNAVAILABLE, "try again later")
        }
    }
}

/// Everything the sender reads: members and settings (wide-column store), endpoints and
/// channel names (Postgres), presence (Valkey, optional).
struct Sender {
    pg: PgPool,
    cql: Session,
    members: PreparedStatement,
    setting: PreparedStatement,
    valkey: Option<redis::aio::MultiplexedConnection>,
    /// The VAPID private key (base64url); each push is signed for its endpoint's origin.
    vapid_private_key: String,
    subject: String,
    http: reqwest::Client,
}

impl Sender {
    async fn members(&self, channel_id: i64) -> Result<Vec<i64>, String> {
        self.cql
            .execute_unpaged(&self.members, (channel_id,))
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .rows::<(i64,)>()
            .map_err(|e| e.to_string())?
            .map(|r| r.map(|(u,)| u).map_err(|e| e.to_string()))
            .collect()
    }

    async fn setting(&self, user_id: i64, channel_id: i64) -> (Level, bool) {
        let row = match self
            .cql
            .execute_unpaged(&self.setting, (user_id, channel_id))
            .await
        {
            Ok(r) => r.into_rows_result().ok().and_then(|r| {
                r.maybe_first_row::<(Option<String>, Option<bool>)>()
                    .ok()
                    .flatten()
            }),
            Err(_) => None,
        };
        let (level, muted) = row.unwrap_or_default();
        (Level::parse(level.as_deref()), muted.unwrap_or(false))
    }

    /// Online and active right now (presence `3`), so the app shows it already.
    async fn online(&self, user_id: i64) -> bool {
        let Some(valkey) = &self.valkey else {
            return false;
        };
        let status: Option<String> = redis::cmd("GET")
            .arg(format!("presence:last:{user_id}"))
            .query_async(&mut valkey.clone())
            .await
            .unwrap_or(None);
        status.as_deref() == Some("3")
    }

    async fn channel_name(&self, channel_id: i64) -> String {
        sqlx::query_scalar::<_, Option<String>>("SELECT name FROM channels WHERE id = $1")
            .bind(channel_id)
            .fetch_optional(&self.pg)
            .await
            .ok()
            .flatten()
            .flatten()
            .unwrap_or_default()
    }

    /// Pushes one sequenced message to everyone who should be told.
    async fn handle(&self, event: &SequencedEvent) {
        let (message_id, msg_seq, body, mentioned, everyone, replies_only) = match &event.payload {
            Some(Payload::MessageCreated(m)) if event.event_seq > 0 => (
                m.message_id,
                event.msg_seq,
                m.body.clone(),
                m.mentioned_user_ids.clone(),
                m.mentions_channel,
                false,
            ),
            // Thread replies notify only the people they mention (followers come later).
            Some(Payload::ThreadReply(r)) if event.event_seq > 0 => (
                r.message_id,
                r.root_msg_seq,
                r.body.clone(),
                r.mentioned_user_ids.clone(),
                false,
                true,
            ),
            _ => return,
        };
        let mentioned: HashSet<i64> = mentioned.into_iter().collect();
        let members = match self.members(event.channel_id).await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, channel_id = event.channel_id, "members read failed; no push");
                return;
            }
        };
        let large = members.len() > LARGE_CHANNEL;
        let mut to = Vec::new();
        for user in members {
            if user == event.actor_id || (replies_only && !mentioned.contains(&user)) {
                continue;
            }
            let (level, muted) = self.setting(user, event.channel_id).await;
            let recipient = Recipient {
                level,
                muted,
                mentioned: everyone || mentioned.contains(&user),
                online: self.online(user).await,
            };
            if should_notify(recipient, large) {
                to.push(user);
            }
        }
        if to.is_empty() {
            return;
        }
        let rows: Vec<(String, i64, String, String)> = match sqlx::query_as(
            "SELECT endpoint, user_id, p256dh, auth FROM push_endpoints WHERE user_id = ANY($1)",
        )
        .bind(&to)
        .fetch_all(&self.pg)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error = %e, "push endpoints read failed");
                return;
            }
        };
        if rows.is_empty() {
            return;
        }
        let name = self.channel_name(event.channel_id).await;
        let content = payload(event.channel_id, &name, msg_seq, message_id, &body).to_string();
        for (endpoint, _user, p256dh, auth) in rows {
            if let Err(e) = self
                .send(&endpoint, &p256dh, &auth, content.as_bytes())
                .await
            {
                tracing::debug!(error = %e, "push not delivered");
            }
        }
    }

    /// Encrypts (RFC 8291), signs (VAPID) and sends one notification. A push service that
    /// says the subscription is gone (404 / 410) makes us forget it.
    async fn send(
        &self,
        endpoint: &str,
        p256dh: &str,
        auth: &str,
        content: &[u8],
    ) -> Result<(), String> {
        let info = SubscriptionInfo::new(endpoint, p256dh, auth);
        let mut signature = VapidSignatureBuilder::from_base64(&self.vapid_private_key, &info)
            .map_err(|e| e.to_string())?;
        signature.add_claim("sub", self.subject.as_str());
        let mut builder = WebPushMessageBuilder::new(&info);
        builder.set_ttl(PUSH_TTL_SECS);
        builder.set_payload(ContentEncoding::Aes128Gcm, content);
        builder.set_vapid_signature(signature.build().map_err(|e| e.to_string())?);
        let message = builder.build().map_err(|e| e.to_string())?;
        // The same request web-push's own clients build (TTL, encoding, crypto headers).
        let mut request = self
            .http
            .post(message.endpoint.to_string())
            .header("TTL", message.ttl.to_string());
        if let Some(payload) = message.payload {
            request = request
                .header("Content-Encoding", payload.content_encoding.to_str())
                .header("Content-Type", "application/octet-stream");
            for (k, v) in payload.crypto_headers {
                request = request.header(k, v);
            }
            request = request.body(payload.content);
        }
        let response = request.send().await.map_err(|e| e.to_string())?;
        let status = response.status();
        if status == 404 || status == 410 {
            let _ = sqlx::query("DELETE FROM push_endpoints WHERE endpoint = $1")
                .bind(endpoint)
                .execute(&self.pg)
                .await;
            return Err(format!("subscription gone ({status}); removed"));
        }
        if !status.is_success() {
            return Err(format!("push service answered {status}"));
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let env =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let Ok(private_key) = std::env::var("XMSG_PUSH_VAPID_PRIVATE_KEY") else {
        return fail("XMSG_PUSH_VAPID_PRIVATE_KEY must be set (base64url P-256 private key)");
    };
    let vapid = match VapidSignatureBuilder::from_base64_no_sub(&private_key) {
        Ok(v) => v,
        Err(e) => return fail(&format!("XMSG_PUSH_VAPID_PRIVATE_KEY: {e}")),
    };
    let vapid_public_key = URL_SAFE_NO_PAD.encode(vapid.get_public_key());
    let Ok(jwks_url) = std::env::var("XMSG_AUTH_JWKS_URL") else {
        return fail("XMSG_AUTH_JWKS_URL must be set");
    };
    let database_url = env(
        "XMSG_DATABASE_URL",
        "postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg",
    );
    let pg = match PgPool::connect(&database_url).await {
        Ok(p) => p,
        Err(e) => return fail(&format!("postgres: {e}")),
    };
    let nodes: Vec<String> = env("XMSG_CQL_NODES", "127.0.0.1:9042")
        .split(',')
        .map(String::from)
        .collect();
    let keyspace = env("XMSG_CQL_KEYSPACE", "xmsg");
    let cql = match SessionBuilder::new().known_nodes(&nodes).build().await {
        Ok(s) => s,
        Err(e) => return fail(&format!("connecting to {nodes:?}: {e}")),
    };
    let prepare = async |cql_text: String| -> Result<PreparedStatement, String> {
        let mut p = cql
            .prepare(cql_text.clone())
            .await
            .map_err(|e| format!("preparing `{cql_text}`: {e}"))?;
        p.set_consistency(Consistency::LocalQuorum);
        Ok(p)
    };
    let (members, setting) = match (
        prepare(format!("SELECT user_id FROM {keyspace}.members_by_channel WHERE channel_id = ?")).await,
        prepare(format!("SELECT notify_level, muted FROM {keyspace}.member_settings WHERE user_id = ? AND channel_id = ?")).await,
    ) {
        (Ok(m), Ok(s)) => (m, s),
        (Err(e), _) | (_, Err(e)) => return fail(&e),
    };
    let valkey = match std::env::var("XMSG_VALKEY_URL") {
        Ok(url) => match redis::Client::open(url.as_str()) {
            Ok(c) => match c.get_multiplexed_async_connection().await {
                Ok(conn) => Some(conn),
                Err(e) => return fail(&format!("connecting to {url}: {e}")),
            },
            Err(e) => return fail(&format!("{url}: {e}")),
        },
        Err(_) => None, // no presence: everyone counts as away
    };
    let sender = Arc::new(Sender {
        pg: pg.clone(),
        cql,
        members,
        setting,
        valkey,
        vapid_private_key: private_key.clone(),
        subject: env("XMSG_PUSH_SUBJECT", "mailto:admin@localhost"),
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("http client"),
    });

    let consumer: StreamConsumer = match ClientConfig::new()
        .set(
            "bootstrap.servers",
            env("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
        )
        .set("group.id", env("XMSG_PUSH_GROUP", "xmsg-push"))
        // A new installation (or group) never pushes old history.
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false")
        .set("isolation.level", "read_committed")
        .create()
    {
        Ok(c) => c,
        Err(e) => return fail(&format!("creating Kafka consumer: {e}")),
    };
    if let Err(e) = consumer.subscribe(&[TOPIC]) {
        return fail(&format!("subscribing to {TOPIC}: {e}"));
    }
    {
        let sender = sender.clone();
        tokio::spawn(async move {
            loop {
                match consumer.recv().await {
                    Ok(message) => {
                        if let Some(Ok(event)) = message.payload().map(SequencedEvent::decode) {
                            sender.handle(&event).await;
                        }
                        let _ = consumer.store_offset_from_message(&message);
                    }
                    Err(e) => tracing::warn!(error = %e, "Kafka receive error"),
                }
            }
        });
    }

    let verifier = match xmsg_authn::Verifier::from_env(jwks_url).await {
        Ok(v) => v,
        Err(e) => return fail(&e),
    };
    let app = Arc::new(App {
        pg,
        verifier,
        vapid_public_key,
        allow_http: env("XMSG_PUSH_ALLOW_HTTP", "false") == "true",
    });
    let router = Router::new()
        .route("/v1/push/vapid-public-key", get(vapid_key))
        .route(
            "/v1/push/subscriptions",
            post(subscribe).delete(unsubscribe),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app);
    let port: u16 = env("XMSG_PUSH_PORT", "8086").parse().unwrap_or(8086);
    let listener = match tokio::net::TcpListener::bind(("0.0.0.0", port)).await {
        Ok(l) => l,
        Err(e) => return fail(&format!("bind :{port}: {e}")),
    };
    tracing::info!(port, "push ready");
    let _ = axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    ExitCode::SUCCESS
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
