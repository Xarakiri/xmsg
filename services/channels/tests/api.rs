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
use xmsg_channels::store::{Dm, NewChannel, Store, StoreError};
use xmsg_proto::v1::{
    ChannelType, CommandResult, InboundEvent, RejectReason, inbound_event::Payload,
};

#[derive(Default)]
struct Memory {
    users: Vec<i64>,
    channels: Mutex<Vec<(NewChannel, Vec<i64>)>>,
    outbox: Mutex<Vec<InboundEvent>>,
    dms: Mutex<HashMap<(i64, i64), i64>>,
}

impl Store for Memory {
    async fn create_channel(
        &self,
        c: &NewChannel,
        members: &[i64],
        event: &InboundEvent,
    ) -> Result<(), StoreError> {
        if c.workspace_id != 1 {
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
    state: Arc<App<Memory, FakeLog>>,
    keys: Arc<Keys>,
}

const ALICE: i64 = 101;
const BOB: i64 = 102;
const CAROL: i64 = 103;

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
            users: vec![ALICE, BOB, CAROL],
            ..Default::default()
        },
        log: FakeLog {
            pending: pending.clone(),
            sent: Mutex::default(),
            verdict: Mutex::default(),
            down: Mutex::default(),
        },
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
            StatusCode::BAD_REQUEST,
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
