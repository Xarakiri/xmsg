// SPDX-License-Identifier: AGPL-3.0-only
//! The calls API with an in-memory store and log, and real tokens from the auth service's
//! key code: joining, the room's token, membership, and webhook signatures.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use tower::ServiceExt;
use xmsg_auth::keys::Keys as AuthKeys;
use xmsg_calls::api::{App, router};
use xmsg_calls::livekit::Keys;
use xmsg_calls::{ActiveCall, Calls, EventLog};
use xmsg_proto::Message as _;
use xmsg_proto::v1::{CallChange, InboundEvent, inbound_event::Payload};

const DM: i64 = 7;
const CHANNEL: i64 = 8;
const ALICE: i64 = 1;
const BOB: i64 = 2;
const MALLORY: i64 = 3;
const SECRET: &str = "a-livekit-api-secret-of-32-chars!!";

#[derive(Default)]
struct Fake {
    calls: Mutex<HashMap<i64, ActiveCall>>,
    log: Mutex<Vec<InboundEvent>>,
}

#[derive(Clone)]
struct Shared(Arc<Fake>);

impl Calls for Shared {
    async fn is_member(&self, channel_id: i64, user_id: i64) -> Result<bool, String> {
        Ok([DM, CHANNEL].contains(&channel_id) && [ALICE, BOB].contains(&user_id))
    }
    async fn channel_type(&self, channel_id: i64) -> Result<Option<String>, String> {
        Ok(Some(if channel_id == DM { "dm" } else { "public" }.into()))
    }
    async fn start(&self, channel_id: i64, call: &ActiveCall) -> Result<ActiveCall, String> {
        Ok(self
            .0
            .calls
            .lock()
            .unwrap()
            .entry(channel_id)
            .or_insert_with(|| call.clone())
            .clone())
    }
    async fn active(&self, channel_id: i64) -> Result<Option<ActiveCall>, String> {
        Ok(self.0.calls.lock().unwrap().get(&channel_id).cloned())
    }
    async fn set_participant(
        &self,
        channel_id: i64,
        call_id: &str,
        user: i64,
        present: bool,
    ) -> Result<Option<ActiveCall>, String> {
        let mut calls = self.0.calls.lock().unwrap();
        let Some(call) = calls.get_mut(&channel_id).filter(|c| c.call_id == call_id) else {
            return Ok(None);
        };
        call.participants.retain(|p| *p != user);
        if present {
            call.participants.push(user);
        }
        Ok(Some(call.clone()))
    }
    async fn announce(&self, channel_id: i64, call_id: &str) -> Result<bool, String> {
        let mut calls = self.0.calls.lock().unwrap();
        Ok(
            match calls.get_mut(&channel_id).filter(|c| c.call_id == call_id) {
                Some(c) if !c.announced => {
                    c.announced = true;
                    true
                }
                _ => false,
            },
        )
    }
    async fn finish(&self, channel_id: i64, call_id: &str) -> Result<Option<ActiveCall>, String> {
        let mut calls = self.0.calls.lock().unwrap();
        Ok(
            if calls.get(&channel_id).is_some_and(|c| c.call_id == call_id) {
                calls.remove(&channel_id)
            } else {
                None
            },
        )
    }
}

impl EventLog for Shared {
    async fn append(&self, _: i64, event: Vec<u8>) -> Result<(), String> {
        self.0
            .log
            .lock()
            .unwrap()
            .push(InboundEvent::decode(event.as_slice()).unwrap());
        Ok(())
    }
}

struct Harness {
    app: Router,
    auth: Arc<AuthKeys>,
    fake: Arc<Fake>,
}

async fn harness() -> Harness {
    let auth = Arc::new(AuthKeys::generate());
    let jwks = auth.jwks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let server = Router::new().route(
        "/jwks",
        axum::routing::get(move || async move { axum::Json(jwks) }),
    );
    tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
    let fake = Arc::new(Fake::default());
    let app = router(Arc::new(App {
        calls: Shared(fake.clone()),
        log: Shared(fake.clone()),
        verifier: xmsg_authn::Verifier::new(url),
        keys: Keys {
            api_key: "APIxmsg".into(),
            api_secret: SECRET.into(),
        },
        livekit_url: "wss://chat.example.com/livekit".into(),
    }));
    Harness { app, auth, fake }
}

impl Harness {
    async fn call(&self, method: &str, user: i64, channel: i64) -> (StatusCode, serde_json::Value) {
        let now = chrono::Utc::now().timestamp();
        let request = Request::builder()
            .method(method)
            .uri(format!("/v1/channels/{channel}/call"))
            .header(
                "authorization",
                format!("Bearer {}", self.auth.issue(user, 1, now, 900)),
            )
            .body(Body::empty())
            .unwrap();
        let response = self.app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap_or_default())
    }

    async fn webhook(&self, body: &str, signed_with: &str) -> StatusCode {
        let now = chrono::Utc::now().timestamp();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = serde_json::json!({"iss": "APIxmsg", "nbf": now, "exp": now + 300, "sha256": STANDARD.encode(Sha256::digest(body.as_bytes()))});
        let signed = format!("{header}.{}", URL_SAFE_NO_PAD.encode(claims.to_string()));
        let mut mac = Hmac::<Sha256>::new_from_slice(signed_with.as_bytes()).unwrap();
        mac.update(signed.as_bytes());
        let jwt = format!(
            "{signed}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        );
        let request = Request::post("/webhooks/livekit")
            .header("authorization", jwt)
            .header("content-type", "application/webhook+json")
            .body(Body::from(body.to_string()))
            .unwrap();
        self.app.clone().oneshot(request).await.unwrap().status()
    }
}

#[tokio::test]
async fn members_get_a_token_for_the_same_room_and_others_nothing() {
    let h = harness().await;
    let (status, alice) = h.call("POST", ALICE, DM).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(alice["huddle"], false);
    assert_eq!(alice["url"], "wss://chat.example.com/livekit");
    let room = alice["call_id"].as_str().unwrap().to_string();
    let keys = Keys {
        api_key: "APIxmsg".into(),
        api_secret: SECRET.into(),
    };
    let claims = keys
        .verify(
            alice["token"].as_str().unwrap(),
            chrono::Utc::now().timestamp(),
        )
        .unwrap();
    assert_eq!(
        (claims["sub"].as_str(), claims["video"]["room"].as_str()),
        (Some("1"), Some(room.as_str()))
    );

    // Bob joins the same call, not a new one.
    let (_, bob) = h.call("POST", BOB, DM).await;
    assert_eq!(bob["call_id"], room.as_str());
    // A channel has its huddle.
    let (_, huddle) = h.call("POST", ALICE, CHANNEL).await;
    assert_eq!(
        (huddle["call_id"].as_str(), huddle["huddle"].as_bool()),
        (Some("h8"), Some(true))
    );
    // Not a member: nothing, as for an unknown channel.
    assert_eq!(h.call("POST", MALLORY, DM).await.0, StatusCode::NOT_FOUND);
    assert_eq!(h.call("GET", MALLORY, DM).await.0, StatusCode::NOT_FOUND);
    assert_eq!(h.call("GET", BOB, DM).await.1["call_id"], room.as_str());
}

#[tokio::test]
async fn signed_webhooks_become_call_events_and_unsigned_ones_are_refused() {
    let h = harness().await;
    let (_, call) = h.call("POST", ALICE, DM).await;
    let room = call["call_id"].as_str().unwrap();
    let joined = format!(
        r#"{{"event":"participant_joined","id":"EV_1","room":{{"name":"{room}"}},"participant":{{"identity":"1"}}}}"#
    );
    assert_eq!(
        h.webhook(&joined, "another-secret-of-32-characters!!!")
            .await,
        StatusCode::UNAUTHORIZED
    );
    assert!(h.fake.log.lock().unwrap().is_empty());
    assert_eq!(h.webhook(&joined, SECRET).await, StatusCode::OK);
    let finished = format!(r#"{{"event":"room_finished","id":"EV_2","room":{{"name":"{room}"}}}}"#);
    assert_eq!(h.webhook(&finished, SECRET).await, StatusCode::OK);
    let changes: Vec<_> = h
        .fake
        .log
        .lock()
        .unwrap()
        .iter()
        .map(|e| match &e.payload {
            Some(Payload::CallEvent(c)) => (
                e.channel_id,
                CallChange::try_from(c.change).unwrap(),
                e.dedupe_id.clone(),
            ),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        changes,
        vec![
            (DM, CallChange::Started, "lk:EV_1".into()),
            (DM, CallChange::Ended, "lk:EV_2".into())
        ]
    );
    // Over: the next join starts a new call.
    assert_ne!(h.call("POST", BOB, DM).await.1["call_id"], room);
}
