// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP calls to auth, channels and the history API.

use std::time::Duration;

use xmsg_proto::Message;
use xmsg_proto::v1::{EventPage, SequencedEvent};

#[derive(Clone)]
pub struct Urls {
    pub auth: String,
    pub channels: String,
    pub messages: String,
}

fn http() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("HTTP client")
}

/// Signs a user up and logs in; returns (user ID, access token). The workspace's
/// sign-up policy must be `open` (see `signup_policy`).
pub async fn sign_up(auth: &str, email: &str) -> Result<(i64, String), String> {
    let http = http();
    let password = "chaos test password";
    for (path, body) in [
        (
            "/v1/signup",
            serde_json::json!({"email": email, "password": password, "name": "Chaos"}),
        ),
        (
            "/v1/login",
            serde_json::json!({"email": email, "password": password}),
        ),
    ] {
        let response = http
            .post(format!("{auth}{path}"))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        let status = response.status();
        let reply: serde_json::Value = response.json().await.map_err(|e| format!("{path}: {e}"))?;
        if !status.is_success() {
            return Err(format!("{path}: {status} {reply}"));
        }
        if path == "/v1/login" {
            let id = reply["user_id"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or("login: no user_id")?;
            let token = reply["access_token"]
                .as_str()
                .ok_or("login: no access_token")?;
            return Ok((id, token.to_string()));
        }
    }
    unreachable!()
}

/// The sign-up policy (ADR-0026): `(first_account, invite_only)`.
pub async fn signup_policy(auth: &str) -> Result<(bool, bool), String> {
    let reply: serde_json::Value = http()
        .get(format!("{auth}/v1/signup-policy"))
        .send()
        .await
        .map_err(|e| format!("/v1/signup-policy: {e}"))?
        .json()
        .await
        .map_err(|e| format!("/v1/signup-policy: {e}"))?;
    Ok((reply["first_account"] == true, reply["invite_only"] == true))
}

/// An authenticated JSON call; returns the status and body (Null if not JSON).
pub async fn call(
    method: &str,
    url: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> Result<(u16, serde_json::Value), String> {
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
    let mut request = http().request(method, url).bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.map_err(|e| format!("{url}: {e}"))?;
    let status = response.status().as_u16();
    Ok((
        status,
        response.json().await.unwrap_or(serde_json::Value::Null),
    ))
}

pub async fn create_channel(
    channels: &str,
    token: &str,
    name: &str,
    members: &[i64],
) -> Result<i64, String> {
    let ids: Vec<String> = members.iter().map(|m| m.to_string()).collect();
    let (status, reply) = call(
        "POST",
        &format!("{channels}/v1/channels"),
        token,
        Some(serde_json::json!({"type": "private", "name": name, "member_ids": ids})),
    )
    .await?;
    if status != 201 {
        return Err(format!("creating channel {name}: {status} {reply}"));
    }
    reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("creating channel {name}: no channel_id in {reply}"))
}

pub enum History {
    Events(Vec<SequencedEvent>),
    /// HTTP status other than 200 (404 = not a member).
    Status(u16),
}

/// All events after `after`, following `has_more`.
pub async fn history(
    messages: &str,
    token: &str,
    channel: i64,
    mut after: i64,
) -> Result<History, String> {
    let http = http();
    let mut out = Vec::new();
    loop {
        let url = format!("{messages}/v1/channels/{channel}/events?after={after}&limit=500");
        let response = http
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let status = response.status().as_u16();
        if status != 200 {
            return Ok(History::Status(status));
        }
        let bytes = response.bytes().await.map_err(|e| e.to_string())?;
        let page = EventPage::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
        if page.truncated {
            return Err(format!("history of {channel} after {after} is truncated"));
        }
        let more = page.has_more;
        if let Some(last) = page.events.last() {
            after = last.event_seq;
        }
        let empty = page.events.is_empty();
        out.extend(page.events);
        if !more || empty {
            return Ok(History::Events(out));
        }
    }
}
