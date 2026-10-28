// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API. Bearer tokens as everywhere (`xmsg-authn`); IDs are JSON strings.
//!
//! - `POST /v1/channels/{channel_id}/call`: members only. Joins the channel's active call,
//!   or starts one, and answers `{call_id, huddle, url, token}`: the LiveKit URL and an
//!   access token for the room (camera, microphone, screen sharing).
//! - `GET /v1/channels/{channel_id}/call`: the active call (`{call_id, huddle,
//!   participant_ids, started_at_ms}`) or 404, for members.
//! - `POST /webhooks/livekit`: LiveKit's webhooks, signed with the installation's API
//!   secret; anything else is refused.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use xmsg_authn::{Error as AuthError, Verifier};

use crate::livekit::Keys;
use crate::{ActiveCall, Calls, EventLog, Webhook, call_room, huddle_room, on_webhook};

pub struct App<C, L> {
    pub calls: C,
    pub log: L,
    pub verifier: Verifier,
    pub keys: Keys,
    /// Where browsers reach LiveKit, e.g. `wss://chat.example.com/livekit`.
    pub livekit_url: String,
}

/// Long enough for a long meeting's reconnects; the room itself ends when it's empty.
const TOKEN_TTL_SECS: i64 = 6 * 3600;

pub fn router<C: Calls, L: EventLog>(app: Arc<App<C, L>>) -> Router {
    Router::new()
        .route(
            "/v1/channels/{channel_id}/call",
            post(join::<C, L>).get(current::<C, L>),
        )
        .route("/webhooks/livekit", post(webhook::<C, L>))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn unavailable(what: &str, e: &str) -> Response {
    tracing::error!(error = %e, "{what} failed");
    error(StatusCode::SERVICE_UNAVAILABLE, "try again later")
}

/// The caller, if a member of the channel; otherwise the response to give.
#[allow(clippy::result_large_err)]
async fn member<C: Calls, L>(
    app: &App<C, L>,
    headers: &HeaderMap,
    channel_id: i64,
) -> Result<i64, Response> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid or missing access token"))?;
    let user = match app.verifier.verify(auth).await {
        Ok(p) => p.user_id,
        Err(AuthError::KeysUnavailable) => {
            return Err(error(StatusCode::SERVICE_UNAVAILABLE, "try again later"));
        }
        Err(_) => {
            return Err(error(
                StatusCode::UNAUTHORIZED,
                "invalid or missing access token",
            ));
        }
    };
    match app.calls.is_member(channel_id, user).await {
        Ok(true) => Ok(user),
        // Same answer as for a channel that doesn't exist: IDs don't leak.
        Ok(false) => Err(error(StatusCode::NOT_FOUND, "unknown channel")),
        Err(e) => Err(unavailable("membership read", &e)),
    }
}

fn describe(call: &ActiveCall) -> serde_json::Value {
    json!({
        "call_id": call.call_id,
        "huddle": call.huddle,
        "participant_ids": call.participants.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        "started_at_ms": call.started_at_ms,
    })
}

async fn join<C: Calls, L: EventLog>(
    State(app): State<Arc<App<C, L>>>,
    headers: HeaderMap,
    Path(channel_id): Path<i64>,
) -> Response {
    let user = match member(&app, &headers, channel_id).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    let call = match app.calls.active(channel_id).await {
        Ok(Some(call)) => call,
        Ok(None) => {
            // DMs and group DMs ring; other channels have a drop-in huddle.
            let huddle = match app.calls.channel_type(channel_id).await {
                Ok(Some(t)) => t != "dm" && t != "group_dm",
                Ok(None) => return error(StatusCode::NOT_FOUND, "unknown channel"),
                Err(e) => return unavailable("channel read", &e),
            };
            let new = ActiveCall {
                call_id: if huddle {
                    huddle_room(channel_id)
                } else {
                    call_room(channel_id)
                },
                huddle,
                started_at_ms: chrono::Utc::now().timestamp_millis(),
                participants: Vec::new(),
                announced: false,
            };
            // Two people starting at once get the same call.
            match app.calls.start(channel_id, &new).await {
                Ok(call) => call,
                Err(e) => return unavailable("starting the call", &e),
            }
        }
        Err(e) => return unavailable("call read", &e),
    };
    let token = app.keys.token(
        &call.call_id,
        &user.to_string(),
        chrono::Utc::now().timestamp(),
        TOKEN_TTL_SECS,
    );
    let mut body = describe(&call);
    body["url"] = json!(app.livekit_url);
    body["token"] = json!(token);
    Json(body).into_response()
}

async fn current<C: Calls, L: EventLog>(
    State(app): State<Arc<App<C, L>>>,
    headers: HeaderMap,
    Path(channel_id): Path<i64>,
) -> Response {
    if let Err(r) = member(&app, &headers, channel_id).await {
        return r;
    }
    match app.calls.active(channel_id).await {
        Ok(Some(call)) => Json(describe(&call)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "no call"),
        Err(e) => unavailable("call read", &e),
    }
}

async fn webhook<C: Calls, L: EventLog>(
    State(app): State<Arc<App<C, L>>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !app
        .keys
        .verify_webhook(auth, &body, chrono::Utc::now().timestamp())
    {
        return error(StatusCode::UNAUTHORIZED, "invalid signature");
    }
    let hook: Webhook = match serde_json::from_slice(&body) {
        Ok(h) => h,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid webhook"),
    };
    match on_webhook(
        &app.calls,
        &app.log,
        &hook,
        chrono::Utc::now().timestamp_millis(),
    )
    .await
    {
        Ok(emitted) => {
            let room = hook.room.as_ref().map_or("", |r| r.name.as_str());
            tracing::info!(event = %hook.event, room, emitted, "LiveKit webhook");
            StatusCode::OK.into_response()
        }
        // LiveKit retries: the next try may work, and repeats are dropped by the sequencer.
        Err(e) => unavailable("applying the webhook", &e),
    }
}
