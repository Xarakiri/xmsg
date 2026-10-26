// SPDX-License-Identifier: AGPL-3.0-only
//! Rate limits (plan §11: per user, IP and workspace), counted in Valkey so every
//! instance of a service shares them.
//!
//! Each rule is a fixed window: at most `limit` events per `window` per key, counted with
//! one `INCR` (plus an expiry on the first) per check. Fixed windows allow up to twice the
//! limit across a window boundary, which is fine for abuse limits and keeps a check to a
//! single round trip.
//!
//! Limits fail **open**: if Valkey can't be reached the event is allowed (and logged), so
//! a Valkey outage never stops people from signing in or sending. Without a Valkey URL
//! each process counts on its own (`Limiter::memory`), which is what tests use.

use std::collections::HashMap;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// At most `limit` events per `window`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rule {
    pub limit: u32,
    pub window: Duration,
}

impl Rule {
    pub const fn new(limit: u32, window: Duration) -> Self {
        Rule { limit, window }
    }

    /// From an environment variable like `30/10s`, or `default` when unset.
    pub fn from_env(name: &str, default: Rule) -> Result<Rule, String> {
        match std::env::var(name) {
            Ok(v) if !v.trim().is_empty() => v.parse().map_err(|e| format!("{name}: {e}")),
            _ => Ok(default),
        }
    }
}

impl FromStr for Rule {
    type Err = String;

    /// `30/10s`, `10/5m`, `100/1h`: count, then a window in seconds, minutes or hours.
    fn from_str(s: &str) -> Result<Self, String> {
        let bad = || format!("{s:?} isn't a rate like 30/10s, 10/5m or 100/1h");
        let (limit, window) = s.trim().split_once('/').ok_or_else(bad)?;
        let limit: u32 = limit.trim().parse().map_err(|_| bad())?;
        let window = window.trim();
        let (number, unit) = window.split_at(window.len().saturating_sub(1));
        let number: u64 = number.parse().map_err(|_| bad())?;
        let seconds = match unit {
            "s" => number,
            "m" => number * 60,
            "h" => number * 3600,
            _ => return Err(bad()),
        };
        if limit == 0 || seconds == 0 {
            return Err(bad());
        }
        Ok(Rule::new(limit, Duration::from_secs(seconds)))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allowed,
    /// Over the limit; the window ends in `retry_after`.
    Limited {
        retry_after: Duration,
    },
}

impl Decision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allowed)
    }
}

/// `INCR` the window's counter, setting its expiry the first time; returns the count.
const SCRIPT: &str = r#"
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
return n
"#;

enum Backend {
    Valkey {
        conn: redis::aio::ConnectionManager,
        script: redis::Script,
    },
    /// key → (window index, count)
    Memory(Mutex<HashMap<String, (u64, u32)>>),
}

pub struct Limiter {
    backend: Backend,
    /// Prefix for every key, e.g. the service's name.
    prefix: String,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

impl Limiter {
    /// Counts in Valkey at `url`. Connects lazily, and reconnects on its own.
    pub async fn valkey(url: &str, prefix: &str) -> Result<Self, String> {
        let client = redis::Client::open(url).map_err(|e| format!("valkey {url}: {e}"))?;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(Duration::from_secs(2))
            .set_response_timeout(Duration::from_millis(500))
            .set_number_of_retries(2);
        // The manager retries on its own; the first connection mustn't hold up a start.
        let conn = tokio::time::timeout(
            Duration::from_secs(5),
            redis::aio::ConnectionManager::new_with_config(client, config),
        )
        .await
        .map_err(|_| format!("valkey {url}: no connection within 5 s"))?
        .map_err(|e| format!("valkey {url}: {e}"))?;
        Ok(Limiter {
            backend: Backend::Valkey {
                conn,
                script: redis::Script::new(SCRIPT),
            },
            prefix: prefix.into(),
        })
    }

    /// Counts in this process only.
    pub fn memory(prefix: &str) -> Self {
        Limiter {
            backend: Backend::Memory(Mutex::new(HashMap::new())),
            prefix: prefix.into(),
        }
    }

    /// Valkey when `XMSG_VALKEY_URL` is set (and reachable at start), else this process.
    pub async fn from_env(prefix: &str) -> Self {
        match std::env::var("XMSG_VALKEY_URL") {
            Ok(url) if !url.is_empty() => match Limiter::valkey(&url, prefix).await {
                Ok(limiter) => limiter,
                Err(e) => {
                    tracing::warn!(error = %e, "rate limits count per process until Valkey is reachable at the next start");
                    Limiter::memory(prefix)
                }
            },
            _ => {
                tracing::warn!("XMSG_VALKEY_URL not set: rate limits count per process");
                Limiter::memory(prefix)
            }
        }
    }

    /// Counts one event for `key` under `rule`.
    pub async fn check(&self, key: &str, rule: Rule) -> Decision {
        self.check_at(key, rule, now_ms()).await
    }

    async fn check_at(&self, key: &str, rule: Rule, now: u64) -> Decision {
        let window_ms = rule.window.as_millis() as u64;
        let index = now / window_ms;
        let retry_after = Duration::from_millis((index + 1) * window_ms - now);
        let count = match &self.backend {
            Backend::Memory(map) => {
                let mut map = map.lock().unwrap();
                if map.len() > 100_000 {
                    // Old windows are worthless; drop them all now and then.
                    map.retain(|_, (i, _)| *i + 1 >= index);
                }
                let entry = map
                    .entry(format!("{}:{key}:{window_ms}", self.prefix))
                    .or_insert((index, 0));
                if entry.0 != index {
                    *entry = (index, 0);
                }
                entry.1 += 1;
                entry.1
            }
            Backend::Valkey { conn, script } => {
                let mut conn = conn.clone();
                let counter = format!("rl:{}:{key}:{window_ms}:{index}", self.prefix);
                match script
                    .key(&counter)
                    .arg(window_ms)
                    .invoke_async::<u32>(&mut conn)
                    .await
                {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!(error = %e, "rate limit check failed; allowing");
                        return Decision::Allowed;
                    }
                }
            }
        };
        if count <= rule.limit {
            Decision::Allowed
        } else {
            Decision::Limited { retry_after }
        }
    }
}

/// The client's address behind `proxy_hops` trusted proxies (`XMSG_PROXY_HOPS`).
///
/// Each proxy appends the address it got the request from to `X-Forwarded-For`, so with
/// H proxies the client is the H-th entry from the right; anything further left was
/// written by the client and can't be trusted. With no proxies it's the TCP peer.
pub fn client_ip(
    peer: Option<IpAddr>,
    forwarded_for: Option<&str>,
    proxy_hops: usize,
) -> Option<IpAddr> {
    if proxy_hops == 0 {
        return peer;
    }
    let entries: Vec<&str> = forwarded_for?.split(',').map(str::trim).collect();
    let index = entries.len().checked_sub(proxy_hops)?;
    entries[index].parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rules_parse_from_short_strings() {
        assert_eq!("30/10s".parse(), Ok(Rule::new(30, Duration::from_secs(10))));
        assert_eq!(
            " 10 / 5m ".parse(),
            Ok(Rule::new(10, Duration::from_secs(300)))
        );
        assert_eq!(
            "100/1h".parse(),
            Ok(Rule::new(100, Duration::from_secs(3600)))
        );
        for bad in ["", "30", "30/10", "0/10s", "30/0s", "x/10s", "30/10d"] {
            assert!(bad.parse::<Rule>().is_err(), "{bad}");
        }
    }

    #[tokio::test]
    async fn a_window_allows_its_limit_then_says_when_to_retry() {
        let limiter = Limiter::memory("test");
        let rule = Rule::new(3, Duration::from_secs(10));
        let start = 1_000_000_000; // a window boundary (multiple of 10 000)
        for _ in 0..3 {
            assert!(
                limiter
                    .check_at("ann", rule, start + 2_000)
                    .await
                    .is_allowed()
            );
        }
        assert_eq!(
            limiter.check_at("ann", rule, start + 2_500).await,
            Decision::Limited {
                retry_after: Duration::from_millis(7_500)
            }
        );
        // Others have their own count; the next window starts afresh.
        assert!(
            limiter
                .check_at("bob", rule, start + 2_500)
                .await
                .is_allowed()
        );
        assert!(
            limiter
                .check_at("ann", rule, start + 10_000)
                .await
                .is_allowed()
        );
    }

    /// Against a real Valkey: `XMSG_IT_VALKEY=redis://127.0.0.1:6379 cargo test -p xmsg-ratelimit -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn valkey_counts_are_shared_between_limiters() {
        let url = std::env::var("XMSG_IT_VALKEY").expect("XMSG_IT_VALKEY");
        let prefix = format!("it-{}", now_ms());
        let (a, b) = (
            Limiter::valkey(&url, &prefix).await.unwrap(),
            Limiter::valkey(&url, &prefix).await.unwrap(),
        );
        // A year-long window: a short one could end between the checks.
        let rule = Rule::new(2, Duration::from_secs(365 * 24 * 3600));
        assert!(a.check("ann", rule).await.is_allowed());
        assert!(b.check("ann", rule).await.is_allowed());
        assert!(
            !a.check("ann", rule).await.is_allowed(),
            "the third, across instances"
        );
    }

    #[tokio::test]
    async fn an_unreachable_valkey_is_reported_quickly() {
        // Nothing listens on port 1: `from_env` falls back to counting per process.
        let started = std::time::Instant::now();
        assert!(Limiter::valkey("redis://127.0.0.1:1", "x").await.is_err());
        assert!(
            started.elapsed() < Duration::from_secs(8),
            "{:?}",
            started.elapsed()
        );
    }

    #[test]
    fn the_client_is_the_entry_the_trusted_proxies_wrote() {
        let peer: IpAddr = "10.0.0.9".parse().unwrap();
        let ip = |s: &str| Some(s.parse::<IpAddr>().unwrap());
        assert_eq!(
            client_ip(Some(peer), Some("1.2.3.4"), 0),
            Some(peer),
            "no proxies: the peer"
        );
        // Ingress then nginx: "client, ingress".
        assert_eq!(
            client_ip(Some(peer), Some("1.2.3.4, 10.0.0.5"), 2),
            ip("1.2.3.4")
        );
        // A forged entry on the left doesn't move it.
        assert_eq!(
            client_ip(Some(peer), Some("6.6.6.6, 1.2.3.4, 10.0.0.5"), 2),
            ip("1.2.3.4")
        );
        assert_eq!(client_ip(Some(peer), Some("1.2.3.4"), 1), ip("1.2.3.4"));
        assert_eq!(
            client_ip(Some(peer), Some("1.2.3.4"), 2),
            None,
            "fewer entries than proxies"
        );
        assert_eq!(client_ip(Some(peer), None, 1), None);
        assert_eq!(client_ip(Some(peer), Some("junk, 10.0.0.5"), 2), None);
    }
}
