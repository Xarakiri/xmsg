// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API tests: an in-memory store and real tokens signed by the auth service's key code.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{DateTime, Utc};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use xmsg_auth::keys::Keys;
use xmsg_users::api::{App, router};
use xmsg_users::store::{
    Accepted, Member, NewInvite, Profile, ProfileChange, Store, StoreError, Workspace,
};

#[derive(Default)]
struct Memory {
    /// user → (email, profile)
    users: Mutex<HashMap<i64, (String, Profile)>>,
    /// id → (name, slug)
    workspaces: Mutex<HashMap<i64, (String, String)>>,
    /// (workspace, user) → role
    members: Mutex<HashMap<(i64, i64), String>>,
    invites: Mutex<Vec<NewInvite>>,
}

impl Memory {
    fn workspaces_of(&self, user: i64) -> Vec<i64> {
        let members = self.members.lock().unwrap();
        members
            .keys()
            .filter(|(_, u)| *u == user)
            .map(|(w, _)| *w)
            .collect()
    }
}

impl Store for Memory {
    async fn me(&self, user_id: i64) -> Result<Option<(String, Profile)>, StoreError> {
        Ok(self.users.lock().unwrap().get(&user_id).cloned())
    }
    async fn update_profile(
        &self,
        user_id: i64,
        change: &ProfileChange,
    ) -> Result<Option<Profile>, StoreError> {
        let mut users = self.users.lock().unwrap();
        let Some((_, profile)) = users.get_mut(&user_id) else {
            return Ok(None);
        };
        if let Some(name) = &change.name {
            profile.name = name.clone();
        }
        if let Some(url) = &change.avatar_url {
            profile.avatar_url = url.clone();
        }
        Ok(Some(profile.clone()))
    }
    async fn visible_profiles(&self, viewer: i64, ids: &[i64]) -> Result<Vec<Profile>, StoreError> {
        let mine = self.workspaces_of(viewer);
        let users = self.users.lock().unwrap();
        Ok(ids
            .iter()
            .filter(|id| {
                **id == viewer || self.workspaces_of(**id).iter().any(|w| mine.contains(w))
            })
            .filter_map(|id| users.get(id).map(|(_, p)| p.clone()))
            .collect())
    }
    async fn create_workspace(
        &self,
        id: i64,
        name: &str,
        slug: &str,
        owner: i64,
    ) -> Result<(), StoreError> {
        let mut workspaces = self.workspaces.lock().unwrap();
        if workspaces.values().any(|(_, s)| s == slug) {
            return Err(StoreError::SlugTaken);
        }
        workspaces.insert(id, (name.into(), slug.into()));
        self.members
            .lock()
            .unwrap()
            .insert((id, owner), "owner".into());
        Ok(())
    }
    async fn workspaces_of(&self, user_id: i64) -> Result<Vec<Workspace>, StoreError> {
        let workspaces = self.workspaces.lock().unwrap();
        let mut list: Vec<Workspace> = self
            .members
            .lock()
            .unwrap()
            .iter()
            .filter(|((_, u), _)| *u == user_id)
            .map(|((w, _), role)| Workspace {
                id: *w,
                name: workspaces[w].0.clone(),
                slug: workspaces[w].1.clone(),
                role: role.clone(),
            })
            .collect();
        list.sort_by_key(|w| w.id);
        Ok(list)
    }
    async fn role(&self, workspace_id: i64, user_id: i64) -> Result<Option<String>, StoreError> {
        Ok(self
            .members
            .lock()
            .unwrap()
            .get(&(workspace_id, user_id))
            .cloned())
    }
    async fn members(
        &self,
        workspace_id: i64,
        after: i64,
        limit: i64,
    ) -> Result<Vec<Member>, StoreError> {
        let users = self.users.lock().unwrap();
        let mut list: Vec<Member> = self
            .members
            .lock()
            .unwrap()
            .iter()
            .filter(|((w, u), _)| *w == workspace_id && *u > after)
            .map(|((_, u), role)| Member {
                user_id: *u,
                name: users[u].1.name.clone(),
                avatar_url: users[u].1.avatar_url.clone(),
                role: role.clone(),
            })
            .collect();
        list.sort_by_key(|m| m.user_id);
        list.truncate(limit as usize);
        Ok(list)
    }
    async fn create_invite(&self, invite: &NewInvite) -> Result<(), StoreError> {
        self.invites.lock().unwrap().push(invite.clone());
        Ok(())
    }
    async fn accept_invite(
        &self,
        code_hash: &str,
        user_id: i64,
        now: DateTime<Utc>,
    ) -> Result<Accepted, StoreError> {
        let Some(workspace_id) = self
            .invites
            .lock()
            .unwrap()
            .iter()
            .find(|i| i.code_hash == code_hash && i.expires_at > now)
            .map(|i| i.workspace_id)
        else {
            return Ok(Accepted::Invalid);
        };
        let mut members = self.members.lock().unwrap();
        if members.contains_key(&(workspace_id, user_id)) {
            return Ok(Accepted::AlreadyMember { workspace_id });
        }
        members.insert((workspace_id, user_id), "member".into());
        Ok(Accepted::Joined { workspace_id })
    }
}

const ALICE: i64 = 101;
const BOB: i64 = 102;
const CAROL: i64 = 103;
/// Signed up, but in no workspace the others share.
const OSCAR: i64 = 104;

struct Harness {
    app: Router,
    state: Arc<App<Memory>>,
    keys: Arc<Keys>,
}

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

    let store = Memory::default();
    for (id, name) in [
        (ALICE, "Alice"),
        (BOB, "Bob"),
        (CAROL, "Carol"),
        (OSCAR, "Oscar"),
    ] {
        store.users.lock().unwrap().insert(
            id,
            (
                format!("{}@example.org", name.to_lowercase()),
                Profile {
                    user_id: id,
                    name: name.into(),
                    avatar_url: None,
                },
            ),
        );
    }
    store
        .workspaces
        .lock()
        .unwrap()
        .insert(1, ("Default".into(), "default".into()));
    for user in [ALICE, BOB, CAROL] {
        store
            .members
            .lock()
            .unwrap()
            .insert((1, user), "member".into());
    }
    let state = Arc::new(App {
        store,
        verifier: xmsg_authn::Verifier::new(url),
    });
    Harness {
        app: router(state.clone()),
        state,
        keys,
    }
}

impl Harness {
    async fn call(
        &self,
        method: &str,
        path: &str,
        as_user: Option<i64>,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder().method(method).uri(path);
        if let Some(user) = as_user {
            let now = Utc::now().timestamp();
            request = request.header(
                "authorization",
                format!("Bearer {}", self.keys.issue(user, 1, now, 900)),
            );
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
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// A workspace owned by `owner`; returns its ID.
    async fn workspace(&self, owner: i64, slug: &str) -> String {
        let (status, reply) = self
            .call(
                "POST",
                "/v1/workspaces",
                Some(owner),
                Some(json!({"name": " Acme ", "slug": slug})),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "{reply}");
        reply["workspace_id"].as_str().unwrap().to_string()
    }
}

#[tokio::test]
async fn me_reads_and_updates_the_profile() {
    let h = harness().await;
    let (status, me) = h.call("GET", "/v1/me", Some(ALICE), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        me,
        json!({"user_id": "101", "name": "Alice", "avatar_url": null, "email": "alice@example.org"})
    );

    let (status, updated) = h
        .call(
            "PATCH",
            "/v1/me",
            Some(ALICE),
            Some(json!({"name": " Ada ", "avatar_url": "https://example.org/a.png"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(
        updated,
        json!({"user_id": "101", "name": "Ada", "avatar_url": "https://example.org/a.png"})
    );

    // Absent fields stay; null clears the avatar.
    let (_, kept) = h
        .call("PATCH", "/v1/me", Some(ALICE), Some(json!({})))
        .await;
    assert_eq!(kept["avatar_url"], "https://example.org/a.png");
    let (_, cleared) = h
        .call(
            "PATCH",
            "/v1/me",
            Some(ALICE),
            Some(json!({"avatar_url": null})),
        )
        .await;
    assert_eq!(
        (cleared["name"].as_str(), cleared["avatar_url"].clone()),
        (Some("Ada"), Value::Null)
    );

    for bad in [
        json!({"name": "  "}),
        json!({"name": "x".repeat(81)}),
        json!({"avatar_url": "javascript:alert(1)"}),
    ] {
        let (status, _) = h
            .call("PATCH", "/v1/me", Some(ALICE), Some(bad.clone()))
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad}");
    }
    assert_eq!(
        h.call("GET", "/v1/me", None, None).await.0,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn batch_profiles_leave_out_users_in_no_shared_workspace() {
    let h = harness().await;
    let path = format!("/v1/users?ids={ALICE},{BOB},{OSCAR},999,{BOB}");
    let (status, reply) = h.call("GET", &path, Some(ALICE), None).await;
    assert_eq!(status, StatusCode::OK, "{reply}");
    let names: Vec<&str> = reply["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| u["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["Alice", "Bob"],
        "outsiders and unknown IDs are omitted"
    );

    // Oscar sees only himself until he shares a workspace with someone.
    let (_, reply) = h.call("GET", &path, Some(OSCAR), None).await;
    assert_eq!(
        reply["users"],
        json!([{"user_id": "104", "name": "Oscar", "avatar_url": null}])
    );

    let too_many: Vec<String> = (1..=201).map(|i| i.to_string()).collect();
    let (status, _) = h
        .call(
            "GET",
            &format!("/v1/users?ids={}", too_many.join(",")),
            Some(ALICE),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = h.call("GET", "/v1/users?ids=1,x", Some(ALICE), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, reply) = h.call("GET", "/v1/users", Some(ALICE), None).await;
    assert_eq!((status, reply), (StatusCode::OK, json!({"users": []})));
}

#[tokio::test]
async fn workspaces_are_created_listed_and_closed_to_outsiders() {
    let h = harness().await;
    let id = h.workspace(ALICE, "Acme-Labs").await;
    let (_, mine) = h.call("GET", "/v1/me/workspaces", Some(ALICE), None).await;
    assert_eq!(
        mine["workspaces"],
        json!([
            {"workspace_id": "1", "name": "Default", "slug": "default", "role": "member"},
            {"workspace_id": id, "name": "Acme", "slug": "acme-labs", "role": "owner"},
        ])
    );

    let (status, _) = h
        .call(
            "POST",
            "/v1/workspaces",
            Some(BOB),
            Some(json!({"name": "Other", "slug": "acme-labs"})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT, "slugs are unique");
    for slug in ["a", "-acme", "acme labs", "ácme"] {
        let (status, _) = h
            .call(
                "POST",
                "/v1/workspaces",
                Some(BOB),
                Some(json!({"name": "X", "slug": slug})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{slug}");
    }

    let members = format!("/v1/workspaces/{id}/members");
    let (status, reply) = h.call("GET", &members, Some(ALICE), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        reply,
        json!({"members": [{"user_id": "101", "name": "Alice", "avatar_url": null, "role": "owner"}], "next": null})
    );
    let (status, _) = h.call("GET", &members, Some(BOB), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND, "members only");
}

#[tokio::test]
async fn member_lists_page_by_user_id() {
    let h = harness().await;
    let (_, first) = h
        .call("GET", "/v1/workspaces/1/members?limit=2", Some(BOB), None)
        .await;
    let ids: Vec<&str> = first["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["user_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        (ids, first["next"].as_str()),
        (vec!["101", "102"], Some("102"))
    );
    let (_, rest) = h
        .call(
            "GET",
            "/v1/workspaces/1/members?limit=2&after=102",
            Some(BOB),
            None,
        )
        .await;
    assert_eq!(rest["members"][0]["user_id"], "103");
    assert_eq!(rest["next"], Value::Null);
}

#[tokio::test]
async fn invites_bring_outsiders_in_and_need_an_owner_or_admin() {
    let h = harness().await;
    let id = h.workspace(ALICE, "acme").await;
    let invites = format!("/v1/workspaces/{id}/invites");

    let (status, invite) = h.call("POST", &invites, Some(ALICE), None).await;
    assert_eq!(status, StatusCode::CREATED, "{invite}");
    let code = invite["code"].as_str().unwrap().to_string();
    assert_eq!(invite["workspace_id"], id.as_str());
    let expires: DateTime<Utc> = invite["expires_at"].as_str().unwrap().parse().unwrap();
    assert!(expires > Utc::now() + chrono::Duration::days(6));
    assert_ne!(
        h.state.store.invites.lock().unwrap()[0].code_hash,
        code,
        "only the hash is stored"
    );

    // Before joining, Oscar and Alice can't see each other; after, they can.
    let path = format!("/v1/users?ids={ALICE}");
    assert_eq!(
        h.call("GET", &path, Some(OSCAR), None).await.1["users"],
        json!([])
    );
    let accept = format!("/v1/invites/{code}/accept");
    let (status, reply) = h.call("POST", &accept, Some(OSCAR), None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reply, json!({"workspace_id": id, "status": "joined"}));
    assert_eq!(
        h.call("GET", &path, Some(OSCAR), None).await.1["users"][0]["name"],
        "Alice"
    );
    let (_, again) = h.call("POST", &accept, Some(OSCAR), None).await;
    assert_eq!(again["status"], "already_member");

    // Oscar is a plain member: he can't invite.
    let (status, _) = h.call("POST", &invites, Some(OSCAR), None).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    // Bob isn't a member at all.
    let (status, _) = h.call("POST", &invites, Some(BOB), None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = h
        .call("POST", &invites, Some(ALICE), Some(json!({"ttl_hours": 0})))
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let (status, reply) = h
        .call("POST", "/v1/invites/not-a-code/accept", Some(BOB), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{reply}");
}

#[tokio::test]
async fn expired_invites_are_refused() {
    let h = harness().await;
    let id: i64 = h.workspace(ALICE, "acme").await.parse().unwrap();
    h.state.store.invites.lock().unwrap().push(NewInvite {
        code_hash: xmsg_users::code_hash("old"),
        workspace_id: id,
        created_by: ALICE,
        expires_at: Utc::now() - chrono::Duration::seconds(1),
    });
    let (status, _) = h
        .call("POST", "/v1/invites/old/accept", Some(BOB), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
