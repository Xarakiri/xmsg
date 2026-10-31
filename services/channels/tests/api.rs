// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API tests: in-memory store, a fake event log standing in for Kafka and the
//! sequencer, and real tokens signed by the auth service's key code.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;
use xmsg_auth::keys::Keys;
use xmsg_channels::api::{App, router};
use xmsg_channels::commands::Pending;
use xmsg_channels::kafka::EventLog;
use xmsg_channels::memberships::{Memberships, MyChannel};
use xmsg_channels::store::{Dm, NewChannel, Store, StoreError};
use xmsg_proto::v1::{
    ChannelType, CommandResult, InboundEvent, RejectReason, inbound_event::Payload,
};

#[derive(Default)]
struct Memory {
    users: Vec<i64>,
    /// workspace → members
    workspaces: HashMap<i64, Vec<i64>>,
    /// (workspace, user) → role
    roles: Mutex<HashMap<(i64, i64), String>>,
    /// (workspace, channel, added_by) default-channel rows
    defaults: Mutex<Vec<(i64, i64, i64)>>,
    /// (workspace, user) who onboarded onto the defaults
    onboarded: Mutex<std::collections::HashSet<(i64, i64)>>,
    channels: Mutex<Vec<(NewChannel, Vec<i64>)>>,
    outbox: Mutex<Vec<InboundEvent>>,
    dms: Mutex<HashMap<(i64, i64), i64>>,
    /// Workspace outbox events: (workspace, event_type, payload).
    workspace_events: Mutex<Vec<(i64, String, Vec<u8>)>>,
}

impl Memory {
    /// A public channel the store knows, as if created earlier.
    fn add_channel(&self, id: i64, workspace_id: i64, kind: &'static str, name: &str) {
        self.channels.lock().unwrap().push((
            NewChannel {
                id,
                workspace_id,
                kind,
                name: Some(name.into()),
                topic: None,
                created_by: ALICE,
            },
            vec![ALICE],
        ));
    }
}

impl Store for Memory {
    async fn workspace_role(&self, w: i64, user_id: i64) -> Result<Option<String>, StoreError> {
        Ok(self.roles.lock().unwrap().get(&(w, user_id)).cloned())
    }
    async fn channel_info(&self, channel_id: i64) -> Result<Option<(i64, String)>, StoreError> {
        Ok(self
            .channels
            .lock()
            .unwrap()
            .iter()
            .find(|(c, _)| c.id == channel_id)
            .map(|(c, _)| (c.workspace_id, c.kind.to_string())))
    }
    async fn default_channels(&self, w: i64) -> Result<Vec<(i64, Option<String>)>, StoreError> {
        let channels = self.channels.lock().unwrap();
        Ok(self
            .defaults
            .lock()
            .unwrap()
            .iter()
            .filter(|(ws, _, _)| *ws == w)
            .map(|(_, c, _)| {
                (
                    *c,
                    channels
                        .iter()
                        .find(|(ch, _)| ch.id == *c)
                        .and_then(|(ch, _)| ch.name.clone()),
                )
            })
            .collect())
    }
    async fn add_default_channel(
        &self,
        w: i64,
        channel_id: i64,
        added_by: i64,
    ) -> Result<(), StoreError> {
        let mut defaults = self.defaults.lock().unwrap();
        if !defaults
            .iter()
            .any(|(ws, c, _)| *ws == w && *c == channel_id)
        {
            defaults.push((w, channel_id, added_by));
        }
        Ok(())
    }
    async fn remove_default_channel(&self, w: i64, channel_id: i64) -> Result<bool, StoreError> {
        let mut defaults = self.defaults.lock().unwrap();
        let before = defaults.len();
        defaults.retain(|(ws, c, _)| !(*ws == w && *c == channel_id));
        Ok(defaults.len() < before)
    }
    async fn onboarded_members(&self, w: i64) -> Result<Vec<i64>, StoreError> {
        let mut members: Vec<i64> = self
            .onboarded
            .lock()
            .unwrap()
            .iter()
            .filter(|(ws, _)| *ws == w)
            .map(|(_, u)| *u)
            .collect();
        members.sort_unstable();
        Ok(members)
    }
    async fn defaults_joined(&self, w: i64, user_id: i64) -> Result<bool, StoreError> {
        Ok(self.onboarded.lock().unwrap().contains(&(w, user_id)))
    }
    async fn mark_defaults_joined(&self, w: i64, user_id: i64) -> Result<(), StoreError> {
        self.onboarded.lock().unwrap().insert((w, user_id));
        Ok(())
    }
    async fn outside_workspace(&self, w: i64, users: &[i64]) -> Result<Vec<i64>, StoreError> {
        let members = self.workspaces.get(&w).cloned().unwrap_or_default();
        Ok(users
            .iter()
            .copied()
            .filter(|u| !members.contains(u))
            .collect())
    }
    async fn shared_workspace(&self, users: &[i64]) -> Result<Option<i64>, StoreError> {
        let mut ids: Vec<i64> = self.workspaces.keys().copied().collect();
        ids.sort_unstable();
        Ok(ids
            .into_iter()
            .find(|w| users.iter().all(|u| self.workspaces[w].contains(u))))
    }
    async fn channel_workspace(&self, channel_id: i64) -> Result<Option<i64>, StoreError> {
        Ok(self
            .channels
            .lock()
            .unwrap()
            .iter()
            .find(|(c, _)| c.id == channel_id)
            .map(|(c, _)| c.workspace_id))
    }
    async fn names(&self, ids: &[i64]) -> Result<HashMap<i64, String>, StoreError> {
        Ok(self
            .channels
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, _)| ids.contains(&c.id))
            .filter_map(|(c, _)| c.name.clone().map(|n| (c.id, n)))
            .collect())
    }
    async fn create_channel(
        &self,
        c: &NewChannel,
        members: &[i64],
        event: &InboundEvent,
    ) -> Result<(), StoreError> {
        if !self.workspaces.contains_key(&c.workspace_id) {
            return Err(StoreError::UnknownWorkspace);
        }
        if members.iter().any(|m| !self.users.contains(m)) {
            return Err(StoreError::UnknownUser);
        }
        self.channels
            .lock()
            .unwrap()
            .push((c.clone(), members.to_vec()));
        self.outbox.lock().unwrap().push(event.clone());
        Ok(())
    }
    async fn outbox_workspace_event(
        &self,
        workspace_id: i64,
        event_type: &str,
        payload: &[u8],
    ) -> Result<(), StoreError> {
        self.workspace_events.lock().unwrap().push((
            workspace_id,
            event_type.to_string(),
            payload.to_vec(),
        ));
        Ok(())
    }
    async fn create_dm(
        &self,
        c: &NewChannel,
        other: i64,
        event: &InboundEvent,
    ) -> Result<Dm, StoreError> {
        let pair = (c.created_by.min(other), c.created_by.max(other));
        if let Some(id) = self.dms.lock().unwrap().get(&pair) {
            return Ok(Dm::Existing(*id));
        }
        self.create_channel(c, &[c.created_by, other], event)
            .await?;
        self.dms.lock().unwrap().insert(pair, c.id);
        Ok(Dm::Created)
    }
}

/// What the persister would have written to `channels_by_user` + `channel_heads`.
#[derive(Default)]
struct FakeMemberships(
    Mutex<HashMap<i64, Vec<MyChannel>>>,
    /// `member_settings` rows (what the settings-writer stored).
    Mutex<Vec<xmsg_proto::v1::MemberSettings>>,
);

impl Memberships for FakeMemberships {
    async fn settings_of(
        &self,
        user_id: i64,
    ) -> Result<Vec<xmsg_proto::v1::MemberSettings>, String> {
        Ok(self
            .1
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.user_id == user_id)
            .cloned()
            .collect())
    }
    async fn save_settings(&self, settings: &xmsg_proto::v1::MemberSettings) -> Result<(), String> {
        self.1.lock().unwrap().push(*settings);
        Ok(())
    }
    async fn channels_of(&self, user_id: i64) -> Result<Vec<MyChannel>, String> {
        Ok(self
            .0
            .lock()
            .unwrap()
            .get(&user_id)
            .cloned()
            .unwrap_or_default())
    }
}

/// Plays Kafka + sequencer: records events and answers commands with `verdict`.
struct FakeLog {
    pending: Arc<Pending>,
    sent: Mutex<Vec<InboundEvent>>,
    verdict: Mutex<Option<CommandResult>>,
    down: Mutex<bool>,
    /// What went to `member.settings`.
    settings: Mutex<Vec<xmsg_proto::v1::MemberSettings>>,
}

impl EventLog for FakeLog {
    async fn publish_settings(
        &self,
        settings: &xmsg_proto::v1::MemberSettings,
    ) -> Result<(), String> {
        self.settings.lock().unwrap().push(*settings);
        Ok(())
    }
    async fn append(&self, event: &InboundEvent) -> Result<(), String> {
        if *self.down.lock().unwrap() {
            return Err("broker down".into());
        }
        self.sent.lock().unwrap().push(event.clone());
        if !event.request_id.is_empty()
            && let Some(mut result) = self.verdict.lock().unwrap().clone()
        {
            result.request_id = event.request_id.clone();
            let pending = self.pending.clone();
            tokio::spawn(async move { pending.complete(result) });
        }
        Ok(())
    }
}

struct Harness {
    app: Router,
    state: Arc<App<Memory, FakeLog, FakeMemberships>>,
    keys: Arc<Keys>,
}

const ALICE: i64 = 101;
const BOB: i64 = 102;
const CAROL: i64 = 103;
/// Shares workspace 2 with Alice, and nothing with Bob or Carol.
const DAVE: i64 = 104;

async fn harness() -> Harness {
    let keys = Arc::new(Keys::generate());
    let jwks = keys.jwks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let server = Router::new().route(
        "/jwks",
        axum::routing::get(move || async move { axum::Json(jwks) }),
    );
    tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });

    let pending = Arc::new(Pending::default());
    let state = Arc::new(App {
        store: Memory {
            users: vec![ALICE, BOB, CAROL, DAVE],
            workspaces: HashMap::from([(1, vec![ALICE, BOB, CAROL]), (2, vec![ALICE, DAVE])]),
            roles: Mutex::from(HashMap::from([
                ((1, ALICE), "owner".to_string()),
                ((1, BOB), "member".to_string()),
                ((1, CAROL), "member".to_string()),
                ((2, ALICE), "owner".to_string()),
                ((2, DAVE), "member".to_string()),
            ])),
            ..Default::default()
        },
        log: FakeLog {
            pending: pending.clone(),
            sent: Mutex::default(),
            verdict: Mutex::default(),
            down: Mutex::default(),
            settings: Mutex::default(),
        },
        memberships: FakeMemberships::default(),
        pending,
        verifier: xmsg_authn::Verifier::new(url),
        command_timeout: Duration::from_millis(200),
    });
    Harness {
        app: router(state.clone()),
        state,
        keys,
    }
}

impl Harness {
    fn token(&self, user: i64) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        format!("Bearer {}", self.keys.issue(user, 1, now, 900))
    }

    async fn call(
        &self,
        method: &str,
        path: &str,
        as_user: Option<i64>,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(user) = as_user {
            request = request.header("authorization", self.token(user));
        }
        let request = match body {
            Some(b) => request
                .header("content-type", "application/json")
                .body(Body::from(b.to_string())),
            None => request.body(Body::empty()),
        }
        .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        )
    }

    fn sent(&self) -> Vec<InboundEvent> {
        self.state.log.sent.lock().unwrap().clone()
    }

    /// A channel the store knows, as if created earlier.
    fn seed_channel(&self, id: i64, workspace_id: i64) {
        let channel = NewChannel {
            id,
            workspace_id,
            kind: "private",
            name: Some("seeded".into()),
            topic: None,
            created_by: ALICE,
        };
        self.state
            .store
            .channels
            .lock()
            .unwrap()
            .push((channel, vec![ALICE]));
    }

    fn answer(&self, result: CommandResult) {
        *self.state.log.verdict.lock().unwrap() = Some(result);
    }
}

#[tokio::test]
async fn creating_a_channel_commits_the_outbox_and_takes_the_fast_path() {
    let h = harness().await;
    let body = serde_json::json!({"type": "private", "name": " design ", "member_ids": [BOB.to_string(), BOB.to_string()]});
    let (status, reply) = h
        .call("POST", "/v1/channels", Some(ALICE), Some(body))
        .await;
    assert_eq!(status, StatusCode::CREATED, "{reply}");
    assert_eq!(reply["status"], "created");
    let channel_id: i64 = reply["channel_id"].as_str().unwrap().parse().unwrap();

    let (channel, members) = h.state.store.channels.lock().unwrap()[0].clone();
    assert_eq!(
        (
            channel.id,
            channel.kind,
            channel.name.as_deref(),
            channel.created_by
        ),
        (channel_id, "private", Some("design"), ALICE)
    );
    assert_eq!(
        members,
        vec![ALICE, BOB],
        "creator added, duplicates removed"
    );

    // The same event went to the outbox and to messages.in.
    let outbox = h.state.store.outbox.lock().unwrap()[0].clone();
    assert_eq!(h.sent(), vec![outbox.clone()]);
    assert_eq!(outbox.dedupe_id, format!("create:{channel_id}"));
    match outbox.payload {
        Some(Payload::ChannelCreated(c)) => {
            assert_eq!(c.r#type, ChannelType::Private as i32);
            assert_eq!(c.initial_member_ids, vec![ALICE, BOB]);
            assert_eq!(c.name, "design", "the name travels with the event");
        }
        other => panic!("unexpected payload {other:?}"),
    }
}

#[tokio::test]
async fn a_failed_fast_path_still_creates_the_channel_as_pending() {
    let h = harness().await;
    *h.state.log.down.lock().unwrap() = true;
    let (status, reply) = h
        .call(
            "POST",
            "/v1/channels",
            Some(ALICE),
            Some(serde_json::json!({"type": "public", "name": "general"})),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(reply["status"], "pending");
    assert_eq!(
        h.state.store.outbox.lock().unwrap().len(),
        1,
        "the outbox row is the safety net"
    );
}

#[tokio::test]
async fn invalid_requests_are_refused() {
    let h = harness().await;
    let cases = [
        (
            serde_json::json!({"type": "secret", "name": "x"}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"type": "public", "name": "  "}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"type": "public", "name": "x", "member_ids": ["999"]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"type": "public", "name": "x", "member_ids": ["abc"]}),
            StatusCode::BAD_REQUEST,
        ),
        (
            serde_json::json!({"type": "public", "name": "x", "workspace_id": "7"}),
            StatusCode::FORBIDDEN,
        ),
    ];
    for (body, expected) in cases {
        let (status, reply) = h
            .call("POST", "/v1/channels", Some(ALICE), Some(body.clone()))
            .await;
        assert_eq!(status, expected, "{body} → {reply}");
    }
    assert!(h.sent().is_empty());
}

#[tokio::test]
async fn requests_without_a_valid_token_are_unauthorized() {
    let h = harness().await;
    let body = serde_json::json!({"type": "public", "name": "x"});
    assert_eq!(
        h.call("POST", "/v1/channels", None, Some(body.clone()))
            .await
            .0,
        StatusCode::UNAUTHORIZED
    );
    let request = Request::post("/v1/channels")
        .header("authorization", "Bearer nope")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    assert_eq!(
        h.app.clone().oneshot(request).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn a_dm_is_created_once_per_pair_and_group_dms_are_new_channels() {
    let h = harness().await;
    let (status, first) = h
        .call(
            "POST",
            "/v1/dms",
            Some(ALICE),
            Some(serde_json::json!({"user_ids": [BOB.to_string()]})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, again) = h
        .call(
            "POST",
            "/v1/dms",
            Some(BOB),
            Some(serde_json::json!({"user_ids": [ALICE.to_string()]})),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the other side opening it gets the same DM"
    );
    assert_eq!(
        (again["channel_id"].clone(), again["status"].as_str()),
        (first["channel_id"].clone(), Some("exists"))
    );

    let (status, group) = h
        .call(
            "POST",
            "/v1/dms",
            Some(ALICE),
            Some(serde_json::json!({"user_ids": [BOB.to_string(), CAROL.to_string()]})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_ne!(group["channel_id"], first["channel_id"]);
    let kinds: Vec<&str> = h
        .state
        .store
        .channels
        .lock()
        .unwrap()
        .iter()
        .map(|(c, _)| c.kind)
        .collect();
    assert_eq!(kinds, ["dm", "group_dm"]);

    let (status, _) = h
        .call(
            "POST",
            "/v1/dms",
            Some(ALICE),
            Some(serde_json::json!({"user_ids": [ALICE.to_string()]})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "a DM with yourself");
}

#[tokio::test]
async fn membership_commands_return_the_sequencers_verdict() {
    let h = harness().await;
    h.seed_channel(555, 1);
    h.answer(CommandResult {
        accepted: true,
        event_seq: 9,
        ..Default::default()
    });
    let (status, reply) = h
        .call("POST", "/v1/channels/555/members", Some(BOB), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(
        (reply["status"].as_str(), reply["event_seq"].as_str()),
        (Some("accepted"), Some("9"))
    );
    let join = h.sent().pop().unwrap();
    assert_eq!((join.channel_id, join.actor_id), (555, BOB));
    assert!(
        matches!(join.payload, Some(Payload::MemberJoinRequested(ref j)) if j.user_id == BOB),
        "no body = self-join"
    );
    assert!(!join.request_id.is_empty());
    assert_eq!(join.dedupe_id, format!("cmd:{}", join.request_id));

    h.answer(CommandResult {
        accepted: false,
        reason: RejectReason::Forbidden as i32,
        ..Default::default()
    });
    let (status, _) = h
        .call(
            "POST",
            "/v1/channels/555/members",
            Some(BOB),
            Some(serde_json::json!({"user_id": CAROL.to_string()})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(
        matches!(h.sent().pop().unwrap().payload, Some(Payload::MemberJoinRequested(ref j)) if j.user_id == CAROL)
    );

    h.answer(CommandResult {
        accepted: false,
        reason: RejectReason::UnknownChannel as i32,
        ..Default::default()
    });
    assert_eq!(
        h.call(
            "DELETE",
            &format!("/v1/channels/556/members/{CAROL}"),
            Some(ALICE),
            None
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
    assert!(
        matches!(h.sent().pop().unwrap().payload, Some(Payload::MemberKickRequested(ref k)) if k.user_id == CAROL)
    );
}

#[tokio::test]
async fn role_changes_go_to_the_sequencer_as_commands() {
    let h = harness().await;
    h.seed_channel(555, 1);
    h.answer(CommandResult {
        accepted: true,
        event_seq: 9,
        ..Default::default()
    });
    let (status, reply) = h
        .call(
            "PATCH",
            &format!("/v1/channels/555/members/{CAROL}"),
            Some(ALICE),
            Some(serde_json::json!({"role": "poster"})),
        )
        .await;
    assert_eq!(
        (status, reply["event_seq"].as_str()),
        (StatusCode::OK, Some("9"))
    );
    let sent = h.sent().pop().unwrap();
    assert_eq!(sent.actor_id, ALICE);
    assert!(matches!(
        sent.payload,
        Some(Payload::MemberRoleChangeRequested(ref r)) if r.user_id == CAROL && r.role == xmsg_proto::v1::MemberRole::Poster as i32
    ));

    // The sequencer refuses posters outside broadcast channels.
    h.answer(CommandResult {
        accepted: false,
        reason: RejectReason::Invalid as i32,
        ..Default::default()
    });
    let (status, _) = h
        .call(
            "PATCH",
            &format!("/v1/channels/555/members/{CAROL}"),
            Some(ALICE),
            Some(serde_json::json!({"role": "poster"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // Unknown roles never reach it.
    let before = h.sent().len();
    let (status, _) = h
        .call(
            "PATCH",
            &format!("/v1/channels/555/members/{CAROL}"),
            Some(ALICE),
            Some(serde_json::json!({"role": "owner"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(h.sent().len(), before);
}

#[tokio::test]
async fn an_undecided_command_answers_pending_and_cleans_up() {
    let h = harness().await;
    h.seed_channel(555, 1);
    let (status, reply) = h
        .call(
            "DELETE",
            &format!("/v1/channels/555/members/{BOB}"),
            Some(BOB),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(reply["status"], "pending");
    assert!(h.state.pending.is_empty());

    *h.state.log.down.lock().unwrap() = true;
    let (status, _) = h
        .call("POST", "/v1/channels/555/members", Some(BOB), None)
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        h.state.pending.is_empty(),
        "a command that was never sent leaves no waiter"
    );
}

#[tokio::test]
async fn my_channels_lists_heads_with_names_from_postgres() {
    let h = harness().await;
    let (_, created) = h
        .call(
            "POST",
            "/v1/channels",
            Some(ALICE),
            Some(serde_json::json!({"type": "public", "name": "general"})),
        )
        .await;
    let id: i64 = created["channel_id"].as_str().unwrap().parse().unwrap();
    h.state.memberships.0.lock().unwrap().insert(
        ALICE,
        vec![
            MyChannel {
                channel_id: id,
                workspace_id: 1,
                kind: "public".into(),
                event_seq: 7,
                msg_seq: 5,
                fanout_mode: "pull".into(),
            },
            MyChannel {
                channel_id: 42,
                workspace_id: 1,
                kind: "dm".into(),
                event_seq: 2,
                msg_seq: 1,
                fanout_mode: "push".into(),
            },
        ],
    );
    let (status, reply) = h.call("GET", "/v1/me/channels", Some(ALICE), None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(
        reply["channels"],
        serde_json::json!([
            {"channel_id": id.to_string(), "workspace_id": "1", "type": "public", "name": "general", "event_seq": "7", "msg_seq": "5", "fanout_mode": "pull"},
            {"channel_id": "42", "workspace_id": "1", "type": "dm", "name": "", "event_seq": "2", "msg_seq": "1", "fanout_mode": "push"},
        ])
    );
    // What a gateway asks for: just the pull-mode channels' IDs (ADR-0008).
    let (status, reply) = h
        .call("GET", "/v1/me/channels?mode=pull", Some(ALICE), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        reply,
        serde_json::json!({ "channels": [{"channel_id": id.to_string(), "workspace_id": "1"}] })
    );
    let (status, reply) = h.call("GET", "/v1/me/channels", Some(BOB), None).await;
    assert_eq!(
        (status, reply["channels"].clone()),
        (StatusCode::OK, serde_json::json!([]))
    );
    assert_eq!(
        h.call("GET", "/v1/me/channels", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn channels_and_dms_stay_inside_workspaces() {
    let h = harness().await;
    let create =
        |user, body: serde_json::Value| h.call("POST", "/v1/channels", Some(user), Some(body));
    // Bob isn't in workspace 2; Dave isn't in workspace 1.
    let (status, reply) = create(
        BOB,
        serde_json::json!({"type": "public", "name": "x", "workspace_id": "2"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{reply}");
    let (status, _) = create(
        ALICE,
        serde_json::json!({"type": "public", "name": "x", "member_ids": [DAVE.to_string()]}),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "an outsider as initial member"
    );
    let (status, reply) = create(ALICE, serde_json::json!({"type": "private", "name": "ops", "workspace_id": "2", "member_ids": [DAVE.to_string()]})).await;
    assert_eq!(status, StatusCode::CREATED, "{reply}");
    let ops: i64 = reply["channel_id"].as_str().unwrap().parse().unwrap();
    assert_eq!(h.state.store.channels.lock().unwrap()[0].0.workspace_id, 2);

    // Inviting or self-joining someone outside the channel's workspace never reaches the sequencer.
    let sent = h.sent().len();
    let (status, _) = h
        .call(
            "POST",
            &format!("/v1/channels/{ops}/members"),
            Some(ALICE),
            Some(serde_json::json!({"user_id": BOB.to_string()})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = h
        .call(
            "POST",
            &format!("/v1/channels/{ops}/members"),
            Some(CAROL),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = h
        .call("POST", "/v1/channels/999/members", Some(ALICE), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "unknown channel");
    assert_eq!(h.sent().len(), sent);

    // DMs need a shared workspace and land in it.
    let dm = |user, others: Vec<i64>| {
        let ids: Vec<String> = others.iter().map(i64::to_string).collect();
        h.call(
            "POST",
            "/v1/dms",
            Some(user),
            Some(serde_json::json!({"user_ids": ids})),
        )
    };
    assert_eq!(dm(BOB, vec![DAVE]).await.0, StatusCode::FORBIDDEN);
    assert_eq!(dm(ALICE, vec![BOB, DAVE]).await.0, StatusCode::FORBIDDEN);
    let (status, _) = dm(ALICE, vec![DAVE]).await;
    assert_eq!(status, StatusCode::CREATED);
    let workspaces: Vec<i64> = h
        .state
        .store
        .channels
        .lock()
        .unwrap()
        .iter()
        .map(|(c, _)| c.workspace_id)
        .collect();
    assert_eq!(workspaces, [2, 2]);
}

#[tokio::test]
async fn a_workspace_removal_leaves_that_workspaces_channels_and_records_completion() {
    use xmsg_proto::Message;
    use xmsg_proto::v1::{UsersEvent, users_event};
    let h = harness().await;
    // BOB was just removed from workspace 2 (he's outside it now), but the persister's
    // channels_by_user still lists two channels there, and one in workspace 1.
    h.state.memberships.0.lock().unwrap().insert(
        BOB,
        vec![
            MyChannel {
                channel_id: 21,
                workspace_id: 2,
                kind: "public".into(),
                event_seq: 3,
                msg_seq: 1,
                fanout_mode: "push".into(),
            },
            MyChannel {
                channel_id: 22,
                workspace_id: 2,
                kind: "private".into(),
                event_seq: 5,
                msg_seq: 2,
                fanout_mode: "push".into(),
            },
            MyChannel {
                channel_id: 11,
                workspace_id: 1,
                kind: "public".into(),
                event_seq: 9,
                msg_seq: 4,
                fanout_mode: "push".into(),
            },
        ],
    );
    h.answer(CommandResult {
        accepted: true,
        event_seq: 6,
        ..Default::default()
    });
    let ended = xmsg_channels::removals::remove_from_workspace(&h.state, 2, BOB, 1_700)
        .await
        .unwrap();
    assert_eq!(ended, 2);

    let leaves: Vec<(i64, i64, String)> = h
        .sent()
        .into_iter()
        .map(|e| match e.payload {
            Some(Payload::MemberKickRequested(k)) => (e.channel_id, k.user_id, e.dedupe_id),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(
        leaves,
        vec![
            (21, BOB, "wsremove:2:102:1700:21".to_string()),
            (22, BOB, "wsremove:2:102:1700:22".to_string())
        ],
        "only workspace 2's channels, as BOB leaving"
    );
    assert!(h.sent().iter().all(|e| e.actor_id == BOB));

    let events = h.state.store.workspace_events.lock().unwrap().clone();
    assert_eq!(events.len(), 1);
    assert_eq!(
        (events[0].0, events[0].1.as_str()),
        (2, "workspace.member_removal_completed")
    );
    match UsersEvent::decode(events[0].2.as_slice()).unwrap().event {
        Some(users_event::Event::RemovalCompleted(c)) => {
            assert_eq!((c.workspace_id, c.user_id, c.channels), (2, BOB, 2))
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn a_stale_removal_of_someone_back_in_the_workspace_does_nothing() {
    let h = harness().await;
    h.state.memberships.0.lock().unwrap().insert(
        ALICE,
        vec![MyChannel {
            channel_id: 21,
            workspace_id: 2,
            kind: "public".into(),
            event_seq: 3,
            msg_seq: 1,
            fanout_mode: "push".into(),
        }],
    );
    // ALICE is (again) a member of workspace 2.
    let ended = xmsg_channels::removals::remove_from_workspace(&h.state, 2, ALICE, 1)
        .await
        .unwrap();
    assert_eq!(ended, 0);
    assert!(h.sent().is_empty(), "no channel is left");
    assert_eq!(
        h.state.store.workspace_events.lock().unwrap().len(),
        1,
        "the completion is still recorded, so fan-out lifts its deny entry"
    );
}

#[tokio::test]
async fn channels_the_user_already_left_are_skipped() {
    let h = harness().await;
    h.state.memberships.0.lock().unwrap().insert(
        BOB,
        vec![MyChannel {
            channel_id: 21,
            workspace_id: 2,
            kind: "public".into(),
            event_seq: 3,
            msg_seq: 1,
            fanout_mode: "push".into(),
        }],
    );
    h.answer(CommandResult {
        accepted: false,
        reason: RejectReason::NotAMember as i32,
        ..Default::default()
    });
    let ended = xmsg_channels::removals::remove_from_workspace(&h.state, 2, BOB, 1)
        .await
        .unwrap();
    assert_eq!(ended, 0);
    assert_eq!(
        h.state.store.workspace_events.lock().unwrap().len(),
        1,
        "still completes"
    );
}

#[tokio::test]
async fn settings_are_validated_published_and_listed_for_member_channels_only() {
    use xmsg_proto::v1::{MemberSettings, NotifyLevel};
    let h = harness().await;
    h.state.memberships.0.lock().unwrap().insert(
        BOB,
        vec![MyChannel {
            channel_id: 21,
            workspace_id: 1,
            kind: "public".into(),
            event_seq: 3,
            msg_seq: 1,
            fanout_mode: "push".into(),
        }],
    );
    let put = |channel: i64, body: serde_json::Value| {
        let h = &h;
        async move {
            h.call(
                "PUT",
                &format!("/v1/channels/{channel}/settings"),
                Some(BOB),
                Some(body),
            )
            .await
            .0
        }
    };
    assert_eq!(
        put(
            21,
            serde_json::json!({"notify_level": "mentions", "muted": true})
        )
        .await,
        StatusCode::ACCEPTED
    );
    assert_eq!(
        put(
            21,
            serde_json::json!({"notify_level": "loud", "muted": true})
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        put(
            99,
            serde_json::json!({"notify_level": "all", "muted": false})
        )
        .await,
        StatusCode::NOT_FOUND,
        "not a member"
    );

    let published = h.state.log.settings.lock().unwrap().clone();
    assert_eq!(published.len(), 1);
    assert_eq!(
        (
            published[0].user_id,
            published[0].channel_id,
            published[0].notify_level,
            published[0].muted
        ),
        (BOB, 21, NotifyLevel::Mentions as i32, true)
    );

    // The settings-writer stored it, plus an orphan row of a channel BOB has left.
    h.state.memberships.1.lock().unwrap().extend([
        published[0],
        MemberSettings {
            user_id: BOB,
            channel_id: 99,
            notify_level: NotifyLevel::Nothing as i32,
            muted: true,
            updated_at_ms: 1,
        },
    ]);
    let (status, body) = h.call("GET", "/v1/me/settings", Some(BOB), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["settings"],
        serde_json::json!([{"channel_id": "21", "notify_level": "mentions", "muted": true}]),
        "orphans are left out"
    );
}

#[tokio::test]
async fn default_channels_are_managed_by_owners_and_public_only() {
    let h = harness().await;
    h.state.store.add_channel(31, 1, "public", "general");
    h.state.store.add_channel(32, 1, "private", "ops");
    h.state.store.add_channel(33, 2, "public", "elsewhere");
    let path = "/v1/workspaces/1/default-channels";

    // Any member lists them; outsiders can't tell the workspace exists.
    let (status, reply) = h.call("GET", path, Some(BOB), None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["channels"], serde_json::json!([]));
    assert_eq!(
        h.call("GET", path, Some(DAVE), None).await.0,
        StatusCode::NOT_FOUND
    );

    // Members don't manage them; only public channels of this workspace qualify.
    let add = |user, body: serde_json::Value| {
        let h = &h;
        async move { h.call("POST", path, Some(user), Some(body)).await }
    };
    assert_eq!(
        add(BOB, serde_json::json!({"channel_id": "31"})).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        add(ALICE, serde_json::json!({"channel_id": "32"})).await.0,
        StatusCode::BAD_REQUEST,
        "a private channel"
    );
    assert_eq!(
        add(ALICE, serde_json::json!({"channel_id": "33"})).await.0,
        StatusCode::BAD_REQUEST,
        "another workspace's channel"
    );
    assert_eq!(
        add(ALICE, serde_json::json!({"channel_id": "999"})).await.0,
        StatusCode::NOT_FOUND
    );

    let (status, _) = add(ALICE, serde_json::json!({"channel_id": "31"})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        add(ALICE, serde_json::json!({"channel_id": "31"})).await.0,
        StatusCode::NO_CONTENT,
        "adding twice is harmless"
    );
    let (_, reply) = h.call("GET", path, Some(BOB), None).await;
    assert_eq!(
        reply["channels"],
        serde_json::json!([{"channel_id": "31", "name": "general"}])
    );

    let del = format!("{path}/31");
    assert_eq!(
        h.call("DELETE", &del, Some(BOB), None).await.0,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        h.call("DELETE", &del, Some(ALICE), None).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        h.call("DELETE", &del, Some(ALICE), None).await.0,
        StatusCode::NO_CONTENT,
        "removing is idempotent and never touches members"
    );
    let (_, reply) = h.call("GET", path, Some(BOB), None).await;
    assert_eq!(reply["channels"], serde_json::json!([]));
}

#[tokio::test]
async fn joining_default_channels_is_idempotent_and_never_rejoins_leavers() {
    let h = harness().await;
    h.state.store.add_channel(31, 1, "public", "general");
    h.state.store.add_channel(32, 1, "public", "random");
    for channel in [31, 32] {
        h.state
            .store
            .defaults
            .lock()
            .unwrap()
            .push((1, channel, ALICE));
    }

    // Bob's first sign-in: both defaults join through the normal command path.
    let (status, reply) = h
        .call(
            "POST",
            "/v1/me/default-channels/join",
            Some(BOB),
            Some(serde_json::json!({"workspace_id": "1"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(reply["joined"], serde_json::json!(["31", "32"]));
    let joins: Vec<(i64, i64, i64)> = h
        .sent()
        .into_iter()
        .map(|e| match e.payload {
            Some(Payload::MemberJoinRequested(j)) => (e.channel_id, e.actor_id, j.user_id),
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    assert_eq!(joins, vec![(31, BOB, BOB), (32, BOB, BOB)]);

    // Idempotent: the second call joins nothing, even where membership is gone
    // (bob "left" channel 31: he is never pushed back in).
    assert!(h.state.store.onboarded.lock().unwrap().contains(&(1, BOB)));
    let (status, reply) = h
        .call(
            "POST",
            "/v1/me/default-channels/join",
            Some(BOB),
            Some(serde_json::json!({"workspace_id": "1"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply["joined"], serde_json::json!([]));
    let joins: Vec<i64> = h
        .sent()
        .into_iter()
        .filter_map(|e| match e.payload {
            Some(Payload::MemberJoinRequested(_)) => Some(e.channel_id),
            _ => None,
        })
        .collect();
    assert_eq!(joins, vec![31, 32], "no new join commands");

    // Members only.
    assert_eq!(
        h.call(
            "POST",
            "/v1/me/default-channels/join",
            Some(DAVE),
            Some(serde_json::json!({"workspace_id": "1"})),
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn adding_existing_members_reaches_onboarded_members_in_the_background() {
    let h = harness().await;
    h.state.store.add_channel(31, 1, "public", "general");
    h.state.store.defaults.lock().unwrap().push((1, 31, ALICE));
    // Carol onboarded long ago; bob did too, but he's already in the channel.
    h.state.store.onboarded.lock().unwrap().insert((1, CAROL));
    h.state.store.onboarded.lock().unwrap().insert((1, BOB));
    h.state.memberships.0.lock().unwrap().insert(
        BOB,
        vec![MyChannel {
            channel_id: 31,
            workspace_id: 1,
            kind: "public".into(),
            event_seq: 1,
            msg_seq: 0,
            fanout_mode: "push".into(),
        }],
    );

    let (status, _) = h
        .call(
            "POST",
            "/v1/workspaces/1/default-channels",
            Some(ALICE),
            Some(serde_json::json!({"channel_id": "31", "add_existing_members": true})),
        )
        .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    // The background task eventually sends carol's join, and skips bob.
    let mut carol_joined = false;
    for _ in 0..50 {
        let sent = h.sent();
        let carol = sent.iter().any(|e| {
            matches!(&e.payload, Some(Payload::MemberJoinRequested(j)) if *j == xmsg_proto::v1::MemberJoinRequested{user_id: CAROL})
        });
        let bob = sent.iter().any(
            |e| matches!(&e.payload, Some(Payload::MemberJoinRequested(j)) if j.user_id == BOB),
        );
        assert!(!bob, "a current member is not re-joined");
        if carol {
            carol_joined = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(carol_joined, "carol's join reached the event log");
    let event = h
        .sent()
        .into_iter()
        .find(|e| matches!(&e.payload, Some(Payload::MemberJoinRequested(j)) if j.user_id == CAROL))
        .unwrap();
    assert_eq!((event.channel_id, event.actor_id), (31, CAROL));
}
