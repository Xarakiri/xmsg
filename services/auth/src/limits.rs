// SPDX-License-Identifier: AGPL-3.0-only
//! Rate limits on sign-in (plan §11), counted across instances in Valkey: password
//! guessing per account and per address, sign-ups per address, recovery codes per account.
//! Every limit can be set with an environment variable (`10/5m` style).

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{ConnectInfo, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use xmsg_ratelimit::{Decision, Limiter, Rule};

use crate::api::{ApiError, Auth};

pub struct AuthLimits {
    pub limiter: Limiter,
    /// `XMSG_RATE_LOGIN_PER_ACCOUNT`, default 10 per 5 minutes.
    pub login_per_account: Rule,
    /// `XMSG_RATE_LOGIN_PER_IP`, default 50 per 5 minutes (offices share addresses).
    pub login_per_ip: Rule,
    /// `XMSG_RATE_SIGNUP_PER_IP`, default 10 per hour.
    pub signup_per_ip: Rule,
    /// `XMSG_RATE_RECOVERY_PER_ACCOUNT`, default 5 per 15 minutes.
    pub recovery_per_account: Rule,
    /// `XMSG_PROXY_HOPS`: trusted proxies in front (see `xmsg_ratelimit::client_ip`).
    pub proxy_hops: usize,
}

const fn per(limit: u32, minutes: u64) -> Rule {
    Rule::new(limit, Duration::from_secs(minutes * 60))
}

impl AuthLimits {
    pub const LOGIN_PER_ACCOUNT: Rule = per(10, 5);
    pub const LOGIN_PER_IP: Rule = per(50, 5);
    pub const SIGNUP_PER_IP: Rule = per(10, 60);
    pub const RECOVERY_PER_ACCOUNT: Rule = per(5, 15);

    /// The defaults, counted in this process (tests).
    pub fn memory() -> Self {
        AuthLimits {
            limiter: Limiter::memory("auth"),
            login_per_account: Self::LOGIN_PER_ACCOUNT,
            login_per_ip: Self::LOGIN_PER_IP,
            signup_per_ip: Self::SIGNUP_PER_IP,
            recovery_per_account: Self::RECOVERY_PER_ACCOUNT,
            proxy_hops: 0,
        }
    }

    pub async fn from_env() -> Result<Self, String> {
        Ok(AuthLimits {
            limiter: Limiter::from_env("auth").await,
            login_per_account: Rule::from_env(
                "XMSG_RATE_LOGIN_PER_ACCOUNT",
                Self::LOGIN_PER_ACCOUNT,
            )?,
            login_per_ip: Rule::from_env("XMSG_RATE_LOGIN_PER_IP", Self::LOGIN_PER_IP)?,
            signup_per_ip: Rule::from_env("XMSG_RATE_SIGNUP_PER_IP", Self::SIGNUP_PER_IP)?,
            recovery_per_account: Rule::from_env(
                "XMSG_RATE_RECOVERY_PER_ACCOUNT",
                Self::RECOVERY_PER_ACCOUNT,
            )?,
            proxy_hops: match std::env::var("XMSG_PROXY_HOPS") {
                Ok(v) => v
                    .parse()
                    .map_err(|_| "XMSG_PROXY_HOPS must be a number".to_string())?,
                Err(_) => 0,
            },
        })
    }

    /// Counts the event under every `(key, rule)`; refused if any is over its limit.
    pub async fn check(&self, checks: &[(String, Rule)]) -> Result<(), ApiError> {
        let mut wait = None;
        for (key, rule) in checks {
            if let Decision::Limited { retry_after } = self.limiter.check(key, *rule).await {
                wait = Some(wait.map_or(retry_after, |w: Duration| w.max(retry_after)));
            }
        }
        match wait {
            None => Ok(()),
            Some(retry_after) => {
                tracing::info!(keys = checks.len(), "sign-in rate-limited");
                Err(ApiError::TooManyRequests(retry_after))
            }
        }
    }
}

/// The client's address, when known (behind `proxy_hops` proxies).
#[derive(Debug, Clone, Copy)]
pub struct ClientIp(pub Option<IpAddr>);

impl ClientIp {
    /// The checks for this address, none when it's unknown.
    pub fn key(&self, kind: &str, rule: Rule) -> Option<(String, Rule)> {
        self.0.map(|ip| (format!("{kind}:ip:{ip}"), rule))
    }
}

/// Puts `ClientIp` on every request.
pub async fn client_ip<S: Send + Sync + 'static>(
    State(auth): State<Arc<Auth<S>>>,
    mut request: Request,
    next: Next,
) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    let forwarded = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok());
    let ip = xmsg_ratelimit::client_ip(peer, forwarded, auth.limits.proxy_hops);
    request.extensions_mut().insert(ClientIp(ip));
    next.run(request).await
}
