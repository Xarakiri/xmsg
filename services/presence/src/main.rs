// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-presence: gateway pings in, status changes out (plan §5.9).

use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use tracing_subscriber::EnvFilter;
use xmsg_presence::{
    EXPIRIES_KEY, MAX_QUERY, PINGS_SUBJECT, QUERY_SUBJECT, QUEUE_GROUP, SCRIPT, keys, state_arg,
    status_from, update_subject,
};
use xmsg_proto::Message;
use xmsg_proto::v1::{PresencePing, PresenceQuery, PresenceReply, PresenceStatus, PresenceUpdate};

fn fail(message: &str) -> ExitCode {
    eprintln!("xmsg-presence: {message}");
    ExitCode::from(2)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[derive(Clone)]
struct Presence {
    valkey: MultiplexedConnection,
    nats: async_nats::Client,
    script: redis::Script,
    ttl_ms: i64,
}

impl Presence {
    /// Applies `state` for `conn_id` (or just recomputes) and publishes the user's new
    /// status if it changed. Returns the status after.
    async fn apply(
        &self,
        user_id: i64,
        conn_id: &str,
        state: &str,
    ) -> Result<PresenceStatus, String> {
        let [hash, last, expiries] = keys(user_id);
        let (before, after): (i64, i64) = self
            .script
            .key(hash)
            .key(last)
            .key(expiries)
            .arg(conn_id)
            .arg(state)
            .arg(now_ms())
            .arg(self.ttl_ms)
            .arg(user_id)
            .invoke_async(&mut self.valkey.clone())
            .await
            .map_err(|e| format!("valkey: {e}"))?;
        let status = status_from(after);
        if before != after {
            let update = PresenceUpdate {
                user_id,
                status: status as i32,
            };
            if let Err(e) = self
                .nats
                .publish(update_subject(user_id), update.encode_to_vec().into())
                .await
            {
                tracing::warn!(error = %e, user_id, "publishing a presence change failed");
            }
        }
        Ok(status)
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let get =
        |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.to_string());
    let valkey_url = get("XMSG_VALKEY_URL", "redis://127.0.0.1:6379");
    let nats_url = get("XMSG_NATS_URL", "nats://127.0.0.1:4222");
    let ttl_ms: i64 = get("XMSG_PRESENCE_TTL_MS", "75000")
        .parse()
        .unwrap_or(75_000);

    let valkey = match redis::Client::open(valkey_url.as_str()) {
        Ok(c) => match c.get_multiplexed_async_connection().await {
            Ok(conn) => conn,
            Err(e) => return fail(&format!("connecting to {valkey_url}: {e}")),
        },
        Err(e) => return fail(&format!("{valkey_url}: {e}")),
    };
    let nats = match async_nats::connect(&nats_url).await {
        Ok(c) => c,
        Err(e) => return fail(&format!("connecting to NATS at {nats_url}: {e}")),
    };
    let presence = Presence {
        valkey,
        nats: nats.clone(),
        script: redis::Script::new(SCRIPT),
        ttl_ms,
    };

    let mut pings = match nats
        .queue_subscribe(PINGS_SUBJECT, QUEUE_GROUP.into())
        .await
    {
        Ok(s) => s,
        Err(e) => return fail(&format!("subscribing to {PINGS_SUBJECT}: {e}")),
    };
    let mut queries = match nats
        .queue_subscribe(QUERY_SUBJECT, QUEUE_GROUP.into())
        .await
    {
        Ok(s) => s,
        Err(e) => return fail(&format!("subscribing to {QUERY_SUBJECT}: {e}")),
    };

    // Pings: one at a time per message, in arrival order (a user's connections are rare
    // enough that ordering across them doesn't matter; the script is atomic).
    {
        let presence = presence.clone();
        tokio::spawn(async move {
            while let Some(msg) = pings.next().await {
                let Ok(ping) = PresencePing::decode(msg.payload.as_ref()) else {
                    continue;
                };
                if ping.user_id <= 0 || ping.conn_id.is_empty() {
                    continue;
                }
                if let Err(e) = presence
                    .apply(ping.user_id, &ping.conn_id, state_arg(ping.state))
                    .await
                {
                    tracing::warn!(error = %e, user_id = ping.user_id, "presence ping not applied");
                }
            }
        });
    }
    // Queries: current statuses (offline when unknown), each recomputed so expired
    // connections are dropped and any change is published.
    {
        let presence = presence.clone();
        let nats = nats.clone();
        tokio::spawn(async move {
            while let Some(msg) = queries.next().await {
                let Some(reply_to) = msg.reply.clone() else {
                    continue;
                };
                let Ok(query) = PresenceQuery::decode(msg.payload.as_ref()) else {
                    continue;
                };
                let mut updates = Vec::new();
                for user_id in query.user_ids.into_iter().take(MAX_QUERY) {
                    let status = presence
                        .apply(user_id, "", "0")
                        .await
                        .unwrap_or(PresenceStatus::Offline);
                    updates.push(PresenceUpdate {
                        user_id,
                        status: status as i32,
                    });
                }
                let reply = PresenceReply { updates }.encode_to_vec();
                let _ = nats.publish(reply_to, reply.into()).await;
            }
        });
    }
    // Sweeper: users whose latest connection expiry has passed (their gateway may have
    // died without saying goodbye) are recomputed, so they go offline or away on time.
    {
        let presence = presence.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let mut conn = presence.valkey.clone();
                let due: Vec<i64> = match redis::cmd("ZRANGEBYSCORE")
                    .arg(EXPIRIES_KEY)
                    .arg("-inf")
                    .arg(now_ms())
                    .arg("LIMIT")
                    .arg(0)
                    .arg(1000)
                    .query_async(&mut conn)
                    .await
                {
                    Ok(due) => due,
                    Err(e) => {
                        tracing::warn!(error = %e, "presence sweep failed");
                        continue;
                    }
                };
                for user_id in due {
                    let _: Result<i64, _> = redis::cmd("ZREM")
                        .arg(EXPIRIES_KEY)
                        .arg(user_id)
                        .query_async(&mut conn)
                        .await;
                    if let Err(e) = presence.apply(user_id, "", "0").await {
                        tracing::warn!(error = %e, user_id, "presence recompute failed");
                    }
                }
            }
        });
    }
    tracing::info!(ttl_ms, "presence ready");
    shutdown_signal().await;
    let _ = nats.flush().await;
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
