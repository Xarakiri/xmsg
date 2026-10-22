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
    channels: Mutex<Vec<(NewChannel, Vec<i64>)>>,
    outbox: Mutex<Vec<InboundEvent>>,
    dms: Mutex<HashMap<(i64, i64), i64>>,
    /// Workspace outbox events: (workspace, event_type, payload).
    workspace_events: Mutex<Vec<(i64, String, Vec<u8>)>>,
}

impl Store for Memory {
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
struct FakeMemberships(Mutex<HashMap<i64, Vec<MyChannel>>>);

impl Memberships for FakeMemberships {
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
}

impl EventLog for FakeLog {
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
            ..Default::default()
        },
        log: FakeLog {
            pending: pending.clone(),
            sent: Mutex::default(),
            verdict: Mutex::default(),
            down: Mutex::default(),
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
            },
            MyChannel {
                channel_id: 42,
                workspace_id: 1,
                kind: "dm".into(),
                event_seq: 2,
                msg_seq: 1,
            },
        ],
    );
    let (status, reply) = h.call("GET", "/v1/me/channels", Some(ALICE), None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    assert_eq!(
        reply["channels"],
        serde_json::json!([
            {"channel_id": id.to_string(), "workspace_id": "1", "type": "public", "name": "general", "event_seq": "7", "msg_seq": "5"},
            {"channel_id": "42", "workspace_id": "1", "type": "dm", "name": "", "event_seq": "2", "msg_seq": "1"},
        ])
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
            },
            MyChannel {
                channel_id: 22,
                workspace_id: 2,
                kind: "private".into(),
                event_seq: 5,
                msg_seq: 2,
            },
            MyChannel {
                channel_id: 11,
                workspace_id: 1,
                kind: "public".into(),
                event_seq: 9,
                msg_seq: 4,
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
