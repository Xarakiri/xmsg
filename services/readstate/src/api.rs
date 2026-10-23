// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API. Bearer tokens as everywhere (`xmsg-authn`); IDs are JSON strings.
//! Changes are produced to `readstate.events` and applied by the user's owner, so the
//! answer is 202: clients show the new position optimistically.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use xmsg_authn::{Error as AuthError, Verifier};
use xmsg_proto::v1::{ReadStateEvent, read_state_event::Change};

use crate::cassandra::Positions;

/// Where read-position changes go (Kafka `readstate.events` in production).
pub trait Events: Send + Sync + 'static {
    fn produce(&self, event: &ReadStateEvent) -> impl Future<Output = Result<(), String>> + Send;
}

pub struct App<P, E> {
    pub positions: P,
    pub events: E,
    pub verifier: Verifier,
}

pub fn router<P: Positions, E: Events>(app: Arc<App<P, E>>) -> Router {
    Router::new()
        .route("/v1/me/read-state", get(read_state::<P, E>))
        .route("/v1/me/mentions", get(mentions::<P, E>))
        .route("/v1/channels/{channel_id}/read", post(mark_read::<P, E>))
        .route(
            "/v1/channels/{channel_id}/unread",
            post(mark_unread::<P, E>),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

pub enum ApiError {
    BadRequest,
    Unauthorized,
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest => (StatusCode::BAD_REQUEST, "invalid id or msg_seq"),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid or missing access token"),
            ApiError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "try again later"),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

async fn caller<P, E>(app: &App<P, E>, headers: &HeaderMap) -> Result<i64, ApiError> {
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;
    match app.verifier.verify(header).await {
        Ok(p) => Ok(p.user_id),
        Err(AuthError::KeysUnavailable) => Err(ApiError::Unavailable),
        Err(_) => Err(ApiError::Unauthorized),
    }
}

/// `{positions: [{channel_id, last_read_msg_seq}]}`: channels without a row are at 0.
async fn read_state<P: Positions, E: Events>(
    State(app): State<Arc<App<P, E>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let rows = app.positions.positions(me).await.map_err(|e| {
        tracing::error!(error = %e, "read_state read failed");
        ApiError::Unavailable
    })?;
    let positions: Vec<Value> = rows
        .into_iter()
        .map(|(c, s)| json!({ "channel_id": c.to_string(), "last_read_msg_seq": s.to_string() }))
        .collect();
    Ok(Json(json!({ "positions": positions })))
}

/// `{mentions: [{channel_id, msg_seqs}]}`: unread mentions (yours and `@channel`) per
/// channel, after your read position, at most 100 per channel. Channels with none are left out.
async fn mentions<P: Positions, E: Events>(
    State(app): State<Arc<App<P, E>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let found = app.positions.mentions(me).await.map_err(|e| {
        tracing::error!(error = %e, "mentions read failed");
        ApiError::Unavailable
    })?;
    let mentions: Vec<Value> = found
        .into_iter()
        .map(|(c, seqs)| {
            json!({
                "channel_id": c.to_string(),
                "msg_seqs": seqs.iter().map(i64::to_string).collect::<Vec<_>>(),
            })
        })
        .collect();
    Ok(Json(json!({ "mentions": mentions })))
}

#[derive(Deserialize)]
pub struct Position {
    msg_seq: String,
}

async fn mark_read<P: Positions, E: Events>(
    State(app): State<Arc<App<P, E>>>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Json(body): Json<Position>,
) -> Result<StatusCode, ApiError> {
    change(&app, &headers, &channel_id, &body.msg_seq, Change::ReadUpTo).await
}

/// "Mark as unread from here": the position becomes `msg_seq - 1`, even if it was further.
async fn mark_unread<P: Positions, E: Events>(
    State(app): State<Arc<App<P, E>>>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Json(body): Json<Position>,
) -> Result<StatusCode, ApiError> {
    change(
        &app,
        &headers,
        &channel_id,
        &body.msg_seq,
        Change::UnreadFrom,
    )
    .await
}

async fn change<P, E: Events>(
    app: &App<P, E>,
    headers: &HeaderMap,
    channel_id: &str,
    msg_seq: &str,
    make: fn(i64) -> Change,
) -> Result<StatusCode, ApiError> {
    let me = caller(app, headers).await?;
    let (Ok(channel_id), Ok(msg_seq)) = (channel_id.parse::<i64>(), msg_seq.parse::<i64>()) else {
        return Err(ApiError::BadRequest);
    };
    if channel_id <= 0 || msg_seq < 0 {
        return Err(ApiError::BadRequest);
    }
    // A position in a channel you can't read reveals nothing and changes nothing anyone
    // else sees, so membership isn't checked here.
    let event = ReadStateEvent {
        user_id: me,
        channel_id,
        at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        change: Some(make(msg_seq)),
    };
    app.events.produce(&event).await.map_err(|e| {
        tracing::warn!(error = %e, "readstate event not produced");
        ApiError::Unavailable
    })?;
    Ok(StatusCode::ACCEPTED)
}
