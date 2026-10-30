// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API. Every request carries `Authorization: Bearer <access token>`; the caller
//! is the token's user. IDs are JSON strings. Errors are `{"error": "..."}`.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{Value, json};
use xmsg_authn::{Error as AuthError, Verifier};
use xmsg_proto::v1::{
    ChannelType, InboundEvent, MemberJoinRequested, MemberKickRequested, MemberRole,
    MemberRoleChangeRequested, RejectReason, inbound_event,
};

use crate::commands::Pending;
use crate::kafka::EventLog;
use crate::memberships::Memberships;
use crate::store::{Dm, NewChannel, Store, StoreError};
use crate::{channel_created, new_id, request_id};

/// Most users a channel can be created with; larger channels grow by joins.
pub const MAX_INITIAL_MEMBERS: usize = 500;
/// Group DMs hold up to 9 people, the creator included.
pub const MAX_GROUP_DM: usize = 8;
const DEFAULT_WORKSPACE: i64 = 1;

pub struct App<S, L, M> {
    pub store: S,
    pub log: L,
    /// The user's channels and their heads (the wide-column store).
    pub memberships: M,
    pub pending: Arc<Pending>,
    pub verifier: Verifier,
    pub command_timeout: Duration,
}

pub fn router<S: Store, L: EventLog, M: Memberships>(app: Arc<App<S, L, M>>) -> Router {
    Router::new()
        .route("/v1/channels", post(create_channel::<S, L, M>))
        .route("/v1/me/channels", get(my_channels::<S, L, M>))
        .route("/v1/me/settings", get(my_settings::<S, L, M>))
        .route(
            "/v1/channels/{channel_id}/settings",
            axum::routing::put(put_settings::<S, L, M>),
        )
        .route("/v1/dms", post(create_dm::<S, L, M>))
        .route(
            "/v1/workspaces/{id}/default-channels",
            get(list_default_channels::<S, L, M>).post(add_default_channel::<S, L, M>),
        )
        .route(
            "/v1/workspaces/{id}/default-channels/{channel_id}",
            delete(remove_default_channel::<S, L, M>),
        )
        .route(
            "/v1/me/default-channels/join",
            post(join_default_channels::<S, L, M>),
        )
        .route("/v1/channels/{channel_id}/members", post(join::<S, L, M>))
        .route(
            "/v1/channels/{channel_id}/members/{user_id}",
            delete(kick::<S, L, M>).patch(set_role::<S, L, M>),
        )
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(&'static str),
    Unauthorized,
    Forbidden(&'static str),
    NotFound(&'static str),
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid or missing access token"),
            ApiError::Forbidden(m) => (StatusCode::FORBIDDEN, m),
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

async fn caller<S, L, M>(app: &App<S, L, M>, headers: &HeaderMap) -> Result<i64, ApiError> {
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

async fn create_channel<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
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
    check_workspace(&app, workspace_id, creator, &members).await?;
    let channel = NewChannel {
        id: new_id(),
        workspace_id,
        kind,
        name: Some(name.to_string()),
        topic,
        created_by: creator,
    };
    let event = channel_created(
        channel.id,
        workspace_id,
        creator,
        channel_type,
        name,
        &members,
    );
    app.store.create_channel(&channel, &members, &event).await?;
    Ok(fast_path(&app, &event, channel.id).await)
}

/// The caller's channels with their heads (plan §6.6, cheap step): clients compare the
/// heads with what they have and fetch events only for channels that moved.
#[derive(Deserialize, Default)]
pub struct MyChannelsQuery {
    /// `pull`: only pull-mode channels, with their workspaces (what a gateway subscribes
    /// to, ADR-0008).
    mode: Option<String>,
}

async fn my_channels<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Query(query): Query<MyChannelsQuery>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let channels = app.memberships.channels_of(me).await.map_err(|e| {
        tracing::error!(error = %e, "membership read failed");
        ApiError::Unavailable
    })?;
    if let Some(mode) = query.mode.as_deref() {
        let list: Vec<Value> = channels
            .iter()
            .filter(|c| c.fanout_mode == mode)
            .map(|c| {
                json!({
                    "channel_id": c.channel_id.to_string(),
                    "workspace_id": c.workspace_id.to_string(),
                })
            })
            .collect();
        return Ok(Json(json!({ "channels": list })));
    }
    let ids: Vec<i64> = channels.iter().map(|c| c.channel_id).collect();
    let names = app.store.names(&ids).await?;
    let list: Vec<Value> = channels
        .iter()
        .map(|c| {
            json!({
                "channel_id": c.channel_id.to_string(),
                "workspace_id": c.workspace_id.to_string(),
                "type": c.kind,
                "name": names.get(&c.channel_id).cloned().unwrap_or_default(),
                "event_seq": c.event_seq.to_string(),
                "msg_seq": c.msg_seq.to_string(),
                "fanout_mode": c.fanout_mode,
            })
        })
        .collect();
    Ok(Json(json!({ "channels": list })))
}

/// The creator and every initial member must belong to the channel's workspace. An
/// outsider creator is refused (403), as is an unknown workspace, so its existence
/// doesn't leak; outsiders among the members make the request invalid (400).
async fn check_workspace<S: Store, L, M>(
    app: &App<S, L, M>,
    workspace_id: i64,
    creator: i64,
    members: &[i64],
) -> Result<(), ApiError> {
    let outside = app.store.outside_workspace(workspace_id, members).await?;
    if outside.contains(&creator) {
        return Err(ApiError::Forbidden("not a member of this workspace"));
    }
    if !outside.is_empty() {
        return Err(ApiError::BadRequest("user is not in the workspace"));
    }
    Ok(())
}

#[derive(Deserialize)]
pub struct Settings {
    /// `all`, `mentions` or `nothing`.
    notify_level: String,
    muted: bool,
}

/// Sets the caller's settings for a channel (ADR-0016): validated here, then produced to
/// `member.settings` with absolute values; the settings-writer stores them. 202: shown at
/// once by the client, stored a moment later. Only members can have settings.
async fn put_settings<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Path(channel_id): Path<String>,
    Json(req): Json<Settings>,
) -> Result<StatusCode, ApiError> {
    use xmsg_proto::v1::{MemberSettings, NotifyLevel};
    let me = caller(&app, &headers).await?;
    let channel_id = parse_id(&channel_id)?;
    let notify_level = match req.notify_level.as_str() {
        "all" => NotifyLevel::All,
        "mentions" => NotifyLevel::Mentions,
        "nothing" => NotifyLevel::Nothing,
        _ => {
            return Err(ApiError::BadRequest(
                "notify_level must be all, mentions or nothing",
            ));
        }
    };
    let member = app
        .memberships
        .channels_of(me)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "membership read failed");
            ApiError::Unavailable
        })?
        .iter()
        .any(|c| c.channel_id == channel_id);
    if !member {
        return Err(ApiError::NotFound("not a member"));
    }
    let settings = MemberSettings {
        user_id: me,
        channel_id,
        notify_level: notify_level as i32,
        muted: req.muted,
        updated_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    };
    app.log.publish_settings(&settings).await.map_err(|e| {
        tracing::warn!(error = %e, "settings not published");
        ApiError::Unavailable
    })?;
    Ok(StatusCode::ACCEPTED)
}

/// `{settings: [{channel_id, notify_level, muted}]}` for the caller's channels; rows of
/// channels they're no longer in (orphans, ADR-0016) are left out.
async fn my_settings<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    use xmsg_proto::v1::NotifyLevel;
    let me = caller(&app, &headers).await?;
    let unavailable = |e: String| {
        tracing::error!(error = %e, "settings read failed");
        ApiError::Unavailable
    };
    let mine: std::collections::HashSet<i64> = app
        .memberships
        .channels_of(me)
        .await
        .map_err(unavailable)?
        .iter()
        .map(|c| c.channel_id)
        .collect();
    let settings: Vec<Value> = app
        .memberships
        .settings_of(me)
        .await
        .map_err(unavailable)?
        .into_iter()
        .filter(|s| mine.contains(&s.channel_id))
        .map(|s| {
            let level = match NotifyLevel::try_from(s.notify_level) {
                Ok(NotifyLevel::Mentions) => "mentions",
                Ok(NotifyLevel::Nothing) => "nothing",
                _ => "all",
            };
            json!({ "channel_id": s.channel_id.to_string(), "notify_level": level, "muted": s.muted })
        })
        .collect();
    Ok(Json(json!({ "settings": settings })))
}

#[derive(Deserialize)]
pub struct CreateDm {
    user_ids: Vec<String>,
    /// Absent: a workspace everyone shares (the lowest-numbered one).
    workspace_id: Option<String>,
}

/// One other user: the pair's DM (created once, then returned again). More: a new group DM.
async fn create_dm<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
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
    let mut members = others.clone();
    members.push(creator);
    let workspace_id = match req.workspace_id.as_deref().map(parse_id).transpose()? {
        Some(id) => {
            check_workspace(&app, id, creator, &members).await?;
            id
        }
        None => app
            .store
            .shared_workspace(&members)
            .await?
            .ok_or(ApiError::Forbidden("no workspace in common"))?,
    };
    let channel = NewChannel {
        id: new_id(),
        workspace_id,
        kind,
        name: None,
        topic: None,
        created_by: creator,
    };
    let event = channel_created(
        channel.id,
        workspace_id,
        creator,
        channel_type,
        "",
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
async fn fast_path<S, L: EventLog, M>(
    app: &App<S, L, M>,
    event: &InboundEvent,
    channel_id: i64,
) -> (StatusCode, Json<Value>) {
    match app.log.append(event).await {
        Ok(()) => (
            StatusCode::CREATED,
            Json(json!({ "channel_id": channel_id.to_string(), "status": "created" })),
        ),
        Err(e) => {
            tracing::warn!(channel_id, error = %e, "fast path failed; left to the outbox relay (xmsg-outbox-relay + meta-relay)");
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

async fn join<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
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
    // Only members of the channel's workspace can join or be invited.
    let workspace_id = app
        .store
        .channel_workspace(channel_id)
        .await?
        .ok_or(ApiError::NotFound("unknown channel"))?;
    if !app
        .store
        .outside_workspace(workspace_id, &[user_id])
        .await?
        .is_empty()
    {
        return Err(ApiError::Forbidden(
            "user is not in the channel's workspace",
        ));
    }
    command(
        &app,
        channel_id,
        actor,
        inbound_event::Payload::MemberJoinRequested(MemberJoinRequested { user_id }),
    )
    .await
}

/// Default channels of a workspace (ADR-0026): any member may list them.
async fn list_default_channels<S: Store, L, M>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id = parse_id(&id)?;
    if app.store.workspace_role(workspace_id, me).await?.is_none() {
        return Err(ApiError::NotFound("unknown workspace"));
    }
    let channels: Vec<Value> = app
        .store
        .default_channels(workspace_id)
        .await?
        .iter()
        .map(|(channel_id, name)| {
            json!({ "channel_id": channel_id.to_string(), "name": name.clone().unwrap_or_default() })
        })
        .collect();
    Ok(Json(json!({ "channels": channels })))
}

/// The caller's role, or 404 for non-members so outsiders can't probe workspaces.
async fn require_workspace_admin<S: Store, L, M>(
    app: &App<S, L, M>,
    workspace_id: i64,
    user_id: i64,
) -> Result<(), ApiError> {
    match app.store.workspace_role(workspace_id, user_id).await? {
        None => Err(ApiError::NotFound("unknown workspace")),
        Some(role) if role == "owner" || role == "admin" => Ok(()),
        Some(_) => Err(ApiError::Forbidden(
            "only owners and admins manage default channels",
        )),
    }
}

#[derive(Deserialize)]
pub struct AddDefaultChannel {
    channel_id: String,
    /// Also join the members who already onboarded (in the background).
    #[serde(default)]
    add_existing_members: bool,
}

/// Makes a public channel a default (ADR-0026). 204 on its own; 202 when the
/// existing members are being added in the background.
async fn add_default_channel<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(req): Json<AddDefaultChannel>,
) -> Result<StatusCode, ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id = parse_id(&id)?;
    require_workspace_admin(&app, workspace_id, me).await?;
    let channel_id = parse_id(&req.channel_id)?;
    let Some((channel_ws, kind)) = app.store.channel_info(channel_id).await? else {
        return Err(ApiError::NotFound("unknown channel"));
    };
    if channel_ws != workspace_id {
        return Err(ApiError::BadRequest("channel is in another workspace"));
    }
    if kind != "public" {
        return Err(ApiError::BadRequest(
            "only public channels can be default channels",
        ));
    }
    app.store
        .add_default_channel(workspace_id, channel_id, me)
        .await?;
    tracing::info!(
        workspace_id,
        channel_id,
        user_id = me,
        "default channel added"
    );
    if req.add_existing_members {
        tokio::spawn(async move { add_existing_members(&app, workspace_id, channel_id).await });
        return Ok(StatusCode::ACCEPTED);
    }
    Ok(StatusCode::NO_CONTENT)
}

/// Joins everyone who already onboarded onto the defaults (they'd otherwise miss the
/// new channel), skipping current members — someone who left stays out. Bounded
/// concurrency; a failed command is logged and doesn't stop the rest.
async fn add_existing_members<S: Store, L: EventLog, M: Memberships>(
    app: &App<S, L, M>,
    workspace_id: i64,
    channel_id: i64,
) {
    let members = match app.store.onboarded_members(workspace_id).await {
        Ok(members) => members,
        Err(e) => {
            tracing::error!(workspace_id, channel_id, error = %e, "listing members for a default channel failed");
            return;
        }
    };
    futures_util::stream::iter(members)
        .for_each_concurrent(8, |user_id| async move {
            let already = app
                .memberships
                .channels_of(user_id)
                .await
                .map(|channels| channels.iter().any(|c| c.channel_id == channel_id))
                .unwrap_or(false);
            if already {
                return;
            }
            let request_id = request_id();
            if let Err(e) = send_command(
                app,
                channel_id,
                user_id,
                inbound_event::Payload::MemberJoinRequested(MemberJoinRequested { user_id }),
                &request_id,
            )
            .await
            {
                tracing::warn!(workspace_id, channel_id, user_id, error = ?e, "adding an existing member to a default channel failed");
            }
        })
        .await;
}

/// Removing a default channel never removes members. Idempotent: 204 either way.
async fn remove_default_channel<S: Store, L, M>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Path((id, channel_id)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    let me = caller(&app, &headers).await?;
    let (workspace_id, channel_id) = (parse_id(&id)?, parse_id(&channel_id)?);
    require_workspace_admin(&app, workspace_id, me).await?;
    app.store
        .remove_default_channel(workspace_id, channel_id)
        .await?;
    tracing::info!(
        workspace_id,
        channel_id,
        user_id = me,
        "default channel removed"
    );
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
pub struct JoinDefaultChannels {
    workspace_id: String,
}

/// First sign-in join onto the workspace's default channels (ADR-0026): idempotent,
/// and never rejoins someone who onboarded before and later left a default channel.
async fn join_default_channels<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Json(req): Json<JoinDefaultChannels>,
) -> Result<Json<Value>, ApiError> {
    let me = caller(&app, &headers).await?;
    let workspace_id = parse_id(&req.workspace_id)?;
    if app.store.workspace_role(workspace_id, me).await?.is_none() {
        return Err(ApiError::NotFound("unknown workspace"));
    }
    if app.store.defaults_joined(workspace_id, me).await? {
        return Ok(Json(json!({ "joined": Vec::<String>::new() })));
    }
    let mine: std::collections::HashSet<i64> = app
        .memberships
        .channels_of(me)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "membership read failed");
            ApiError::Unavailable
        })?
        .iter()
        .map(|c| c.channel_id)
        .collect();
    let mut joined = Vec::new();
    for (channel_id, _) in app.store.default_channels(workspace_id).await? {
        if mine.contains(&channel_id) {
            continue;
        }
        let request_id = request_id();
        send_command(
            &app,
            channel_id,
            me,
            inbound_event::Payload::MemberJoinRequested(MemberJoinRequested { user_id: me }),
            &request_id,
        )
        .await?;
        joined.push(channel_id.to_string());
    }
    app.store.mark_defaults_joined(workspace_id, me).await?;
    Ok(Json(json!({ "joined": joined })))
}

/// Removes `user_id` (admins), or leaves when it's the caller.
async fn kick<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
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

#[derive(Deserialize)]
pub struct SetRole {
    role: String,
}

/// A channel admin sets a member's role: `member`, `poster` (posts in a broadcast channel,
/// plan §6.7) or `admin`. The sequencer checks who may and emits `member.role_changed`.
async fn set_role<S: Store, L: EventLog, M: Memberships>(
    State(app): State<Arc<App<S, L, M>>>,
    headers: HeaderMap,
    Path((channel_id, user_id)): Path<(String, String)>,
    Json(req): Json<SetRole>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let actor = caller(&app, &headers).await?;
    let (channel_id, user_id) = (parse_id(&channel_id)?, parse_id(&user_id)?);
    let role = match req.role.as_str() {
        "member" => MemberRole::Member,
        "poster" => MemberRole::Poster,
        "admin" => MemberRole::Admin,
        _ => return Err(ApiError::BadRequest("role must be member, poster or admin")),
    };
    command(
        &app,
        channel_id,
        actor,
        inbound_event::Payload::MemberRoleChangeRequested(MemberRoleChangeRequested {
            user_id,
            role: role as i32,
        }),
    )
    .await
}

/// Sends a membership command without waiting for the sequencer's verdict: what the
/// default-channel flows use, so sequencer and fan-out see ordinary joins.
async fn send_command<S, L: EventLog, M>(
    app: &App<S, L, M>,
    channel_id: i64,
    actor: i64,
    payload: inbound_event::Payload,
    request_id: &str,
) -> Result<(), ApiError> {
    let event = InboundEvent {
        channel_id,
        // The sequencer takes the workspace from the channel's head.
        workspace_id: 0,
        actor_id: actor,
        // A retried produce of the same command is dropped by the sequencer.
        dedupe_id: format!("cmd:{request_id}"),
        request_id: request_id.to_string(),
        payload: Some(payload),
    };
    app.log.append(&event).await.map_err(|e| {
        tracing::warn!(channel_id, error = %e, "membership command not sent");
        ApiError::Unavailable
    })?;
    Ok(())
}

/// Sends a membership command and waits for the sequencer's verdict (ADR-0006).
async fn command<S, L: EventLog, M>(
    app: &App<S, L, M>,
    channel_id: i64,
    actor: i64,
    payload: inbound_event::Payload,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let request_id = request_id();
    let waiter = app.pending.register(&request_id);
    send_command(app, channel_id, actor, payload, &request_id).await?;
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
            RejectReason::Forbidden => ApiError::Forbidden("forbidden"),
            RejectReason::Invalid => ApiError::BadRequest("invalid for this channel"),
            _ => ApiError::BadRequest("rejected"),
        },
    )
}
