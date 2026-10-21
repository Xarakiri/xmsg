// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API. Every request carries `Authorization: Bearer <access token>`; the caller
//! is the token's user. IDs are JSON strings. Errors are `{"error": "..."}`.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Value, json};
use xmsg_authn::{Error as AuthError, Verifier};
use xmsg_proto::v1::{
    ChannelType, InboundEvent, MemberJoinRequested, MemberKickRequested, RejectReason,
    inbound_event,
};

use crate::commands::Pending;
use crate::kafka::EventLog;
use crate::store::{Dm, NewChannel, Store, StoreError};
use crate::{channel_created, new_id, request_id};

/// Most users a channel can be created with; larger channels grow by joins.
pub const MAX_INITIAL_MEMBERS: usize = 500;
/// Group DMs hold up to 9 people, the creator included.
pub const MAX_GROUP_DM: usize = 8;
const DEFAULT_WORKSPACE: i64 = 1;

pub struct App<S, L> {
    pub store: S,
    pub log: L,
    pub pending: Arc<Pending>,
    pub verifier: Verifier,
    pub command_timeout: Duration,
}

pub fn router<S: Store, L: EventLog>(app: Arc<App<S, L>>) -> Router {
    Router::new()
        .route("/v1/channels", post(create_channel::<S, L>))
        .route("/v1/dms", post(create_dm::<S, L>))
        .route("/v1/channels/{channel_id}/members", post(join::<S, L>))
        .route(
            "/v1/channels/{channel_id}/members/{user_id}",
            delete(kick::<S, L>),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

pub enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    Forbidden,
    NotFound(&'static str),
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid or missing access token"),
            ApiError::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "try again later"),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(e: StoreError) -> Self {
        match e {
            StoreError::UnknownWorkspace => ApiError::BadRequest("unknown workspace"),
            StoreError::UnknownUser => ApiError::BadRequest("unknown user"),
            StoreError::Db(e) => {
                tracing::error!(error = %e, "store failed");
                ApiError::Unavailable
            }
        }
    }
}

async fn caller<S, L>(app: &App<S, L>, headers: &HeaderMap) -> Result<i64, ApiError> {
    let header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(ApiError::Unauthorized)?;
    match app.verifier.verify(header).await {
        Ok(principal) => Ok(principal.user_id),
        Err(AuthError::KeysUnavailable) => Err(ApiError::Unavailable),
        Err(_) => Err(ApiError::Unauthorized),
    }
}

fn parse_id(s: &str) -> Result<i64, ApiError> {
    s.parse::<i64>()
        .ok()
        .filter(|id| *id > 0)
        .ok_or(ApiError::BadRequest("invalid id"))
}

fn parse_ids(ids: &[String]) -> Result<Vec<i64>, ApiError> {
    ids.iter().map(|s| parse_id(s)).collect()
}

#[derive(Deserialize)]
pub struct CreateChannel {
    workspace_id: Option<String>,
    #[serde(rename = "type")]
    kind: String,
    name: String,
    topic: Option<String>,
    #[serde(default)]
    member_ids: Vec<String>,
}

async fn create_channel<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Json(req): Json<CreateChannel>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let creator = caller(&app, &headers).await?;
    let (kind, channel_type) = match req.kind.as_str() {
        "public" => ("public", ChannelType::Public),
        "private" => ("private", ChannelType::Private),
        "broadcast" => ("broadcast", ChannelType::Broadcast),
        _ => {
            return Err(ApiError::BadRequest(
                "type must be public, private or broadcast",
            ));
        }
    };
    let name = req.name.trim();
    if name.is_empty() || name.chars().count() > 80 {
        return Err(ApiError::BadRequest("name must be 1 to 80 characters"));
    }
    let topic = req
        .topic
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    if topic.as_ref().is_some_and(|t| t.chars().count() > 250) {
        return Err(ApiError::BadRequest("topic must be at most 250 characters"));
    }
    let workspace_id = req
        .workspace_id
        .as_deref()
        .map(parse_id)
        .transpose()?
        .unwrap_or(DEFAULT_WORKSPACE);
    let mut members = parse_ids(&req.member_ids)?;
    members.push(creator);
    members.sort_unstable();
    members.dedup();
    if members.len() > MAX_INITIAL_MEMBERS {
        return Err(ApiError::BadRequest("too many initial members"));
    }
    let channel = NewChannel {
        id: new_id(),
        workspace_id,
        kind,
        name: Some(name.to_string()),
        topic,
        created_by: creator,
    };
    let event = channel_created(channel.id, workspace_id, creator, channel_type, &members);
    app.store.create_channel(&channel, &members, &event).await?;
    Ok(fast_path(&app, &event, channel.id).await)
}

#[derive(Deserialize)]
pub struct CreateDm {
    user_ids: Vec<String>,
}

/// One other user: the pair's DM (created once, then returned again). More: a new group DM.
async fn create_dm<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Json(req): Json<CreateDm>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let creator = caller(&app, &headers).await?;
    let mut others = parse_ids(&req.user_ids)?;
    others.retain(|u| *u != creator);
    others.sort_unstable();
    others.dedup();
    let (kind, channel_type) = match others.len() {
        0 => return Err(ApiError::BadRequest("user_ids must name someone else")),
        1 => ("dm", ChannelType::Dm),
        n if n <= MAX_GROUP_DM => ("group_dm", ChannelType::GroupDm),
        _ => return Err(ApiError::BadRequest("group DMs hold at most 9 people")),
    };
    let channel = NewChannel {
        id: new_id(),
        workspace_id: DEFAULT_WORKSPACE,
        kind,
        name: None,
        topic: None,
        created_by: creator,
    };
    let mut members = others.clone();
    members.push(creator);
    let event = channel_created(
        channel.id,
        DEFAULT_WORKSPACE,
        creator,
        channel_type,
        &members,
    );
    if kind == "dm" {
        if let Dm::Existing(id) = app.store.create_dm(&channel, others[0], &event).await? {
            return Ok((
                StatusCode::OK,
                Json(json!({ "channel_id": id.to_string(), "status": "exists" })),
            ));
        }
    } else {
        app.store.create_channel(&channel, &members, &event).await?;
    }
    Ok(fast_path(&app, &event, channel.id).await)
}

/// The committed creation goes straight to `messages.in`. If that fails the channel
/// still exists (the outbox relay delivers it), so the answer is "pending", not an error.
async fn fast_path<S, L: EventLog>(
    app: &App<S, L>,
    event: &InboundEvent,
    channel_id: i64,
) -> (StatusCode, Json<Value>) {
    match app.log.append(event).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({ "channel_id": channel_id.to_string(), "status": "created" })),
        ),
        Err(e) => {
            tracing::warn!(channel_id, error = %e, "fast path failed; left to the outbox relay");
            (
                StatusCode::ACCEPTED,
                Json(json!({ "channel_id": channel_id.to_string(), "status": "pending" })),
            )
        }
    }
}

#[derive(Deserialize, Default)]
pub struct Join {
    /// Who joins; absent means the caller joins themselves (public channels only).
    user_id: Option<String>,
}

async fn join<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    body: Option<Json<Join>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let actor = caller(&app, &headers).await?;
    let channel_id = parse_id(&channel_id)?;
    let Json(req) = body.unwrap_or_default();
    let user_id = req
        .user_id
        .as_deref()
        .map(parse_id)
        .transpose()?
        .unwrap_or(actor);
    command(
        &app,
        channel_id,
        actor,
        inbound_event::Payload::MemberJoinRequested(MemberJoinRequested { user_id }),
    )
    .await
}

/// Removes `user_id` (admins), or leaves when it's the caller.
async fn kick<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path((channel_id, user_id)): Path<(String, String)>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let actor = caller(&app, &headers).await?;
    let (channel_id, user_id) = (parse_id(&channel_id)?, parse_id(&user_id)?);
    command(
        &app,
        channel_id,
        actor,
        inbound_event::Payload::MemberKickRequested(MemberKickRequested { user_id }),
    )
    .await
}

/// Sends a membership command and waits for the sequencer's verdict (ADR-0006).
async fn command<S, L: EventLog>(
    app: &App<S, L>,
    channel_id: i64,
    actor: i64,
    payload: inbound_event::Payload,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let request_id = request_id();
    let event = InboundEvent {
        channel_id,
        // The sequencer takes the workspace from the channel's head.
        workspace_id: 0,
        actor_id: actor,
        // A retried produce of the same command is dropped by the sequencer.
        dedupe_id: format!("cmd:{request_id}"),
        request_id: request_id.clone(),
        payload: Some(payload),
    };
    let waiter = app.pending.register(&request_id);
    app.log.append(&event).await.map_err(|e| {
        tracing::warn!(channel_id, error = %e, "membership command not sent");
        ApiError::Unavailable
    })?;
    let Some(result) = waiter.wait(app.command_timeout).await else {
        // Sent but not yet decided: the client learns the outcome from the channel's events.
        return Ok((
            StatusCode::ACCEPTED,
            Json(json!({ "status": "pending", "request_id": request_id })),
        ));
    };
    if result.accepted {
        return Ok((
            StatusCode::OK,
            Json(json!({ "status": "accepted", "event_seq": result.event_seq.to_string() })),
        ));
    }
    Err(
        match RejectReason::try_from(result.reason).unwrap_or(RejectReason::Unspecified) {
            RejectReason::UnknownChannel => ApiError::NotFound("unknown channel"),
            RejectReason::NotAMember => ApiError::NotFound("not a member"),
            RejectReason::Forbidden => ApiError::Forbidden,
            _ => ApiError::BadRequest("rejected"),
        },
    )
}
