// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! The SCIM API end to end against a stand-in for the users service's provisioning API
//! (same paths, statuses and rules: owner can't be deactivated, only members renamed).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use serde_json::{Value, json};

const SCIM_TOKEN: &str = "scim-token-for-tests-0123456789abcdef";
const SERVICE_TOKEN: &str = "provisioning-token";
const OWNER: u64 = 1;

#[derive(Clone)]
struct Person {
    email: String,
    name: String,
    active: bool,
}

#[derive(Default)]
struct Users {
    people: Mutex<BTreeMap<u64, Person>>,
}

type Shared = Arc<Users>;

fn check(headers: &HeaderMap, ws: &str) -> Result<(), StatusCode> {
    let ok = headers.get("authorization").and_then(|v| v.to_str().ok())
        == Some(&format!("Bearer {SERVICE_TOKEN}"));
    match (ok, ws) {
        (false, _) => Err(StatusCode::UNAUTHORIZED),
        (true, "7") => Ok(()),
        (true, _) => Err(StatusCode::NOT_FOUND),
    }
}

fn user(id: u64, p: &Person) -> Value {
    json!({ "user_id": id.to_string(), "email": p.email, "name": p.name, "active": p.active })
}

async fn list(
    State(users): State<Shared>,
    headers: HeaderMap,
    Path(ws): Path<String>,
    Query(q): Query<BTreeMap<String, String>>,
) -> Result<Json<Value>, StatusCode> {
    check(&headers, &ws)?;
    let people = users.people.lock().unwrap();
    if let Some(email) = q.get("email") {
        let (id, p) = people
            .iter()
            .find(|(_, p)| &p.email == email)
            .ok_or(StatusCode::NOT_FOUND)?;
        return Ok(Json(json!({ "users": [user(*id, p)] })));
    }
    let after: u64 = q.get("after").and_then(|a| a.parse().ok()).unwrap_or(0);
    let limit: usize = q.get("limit").and_then(|l| l.parse().ok()).unwrap_or(100);
    let page: Vec<Value> = people
        .range(after + 1..)
        .filter(|(_, p)| p.active)
        .take(limit)
        .map(|(id, p)| user(*id, p))
        .collect();
    let next = (page.len() == limit).then(|| page.last().unwrap()["user_id"].clone());
    Ok(Json(json!({ "users": page, "next": next })))
}

async fn provision(
    State(users): State<Shared>,
    headers: HeaderMap,
    Path(ws): Path<String>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), StatusCode> {
    check(&headers, &ws)?;
    let email = body["email"].as_str().unwrap().to_lowercase();
    let mut people = users.people.lock().unwrap();
    if let Some((id, p)) = people.iter_mut().find(|(_, p)| p.email == email) {
        p.active = true;
        return Ok((StatusCode::OK, Json(user(*id, p))));
    }
    let id = people.keys().last().copied().unwrap_or(0) + 1;
    let name = match body["name"].as_str() {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => email.split('@').next().unwrap().to_string(),
    };
    let p = Person {
        email,
        name,
        active: true,
    };
    let created = user(id, &p);
    people.insert(id, p);
    Ok((StatusCode::CREATED, Json(created)))
}

async fn one(
    State(users): State<Shared>,
    headers: HeaderMap,
    Path((ws, id)): Path<(String, u64)>,
) -> Result<Json<Value>, StatusCode> {
    check(&headers, &ws)?;
    let people = users.people.lock().unwrap();
    let p = people.get(&id).ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(user(id, p)))
}

async fn rename(
    State(users): State<Shared>,
    headers: HeaderMap,
    Path((ws, id)): Path<(String, u64)>,
    Json(body): Json<Value>,
) -> StatusCode {
    if let Err(s) = check(&headers, &ws) {
        return s;
    }
    match users.people.lock().unwrap().get_mut(&id) {
        Some(p) if p.active => {
            p.name = body["name"].as_str().unwrap().to_string();
            StatusCode::NO_CONTENT
        }
        _ => StatusCode::NOT_FOUND,
    }
}

async fn deactivate(
    State(users): State<Shared>,
    headers: HeaderMap,
    Path((ws, id)): Path<(String, u64)>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    check(&headers, &ws).map_err(|s| (s, Json(json!({}))))?;
    if id == OWNER {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({ "error": "the workspace owner can't be deactivated" })),
        ));
    }
    if let Some(p) = users.people.lock().unwrap().get_mut(&id) {
        p.active = false;
    }
    Ok(StatusCode::NO_CONTENT)
}

struct Harness {
    users: Shared,
    base: String,
    http: reqwest::Client,
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn harness() -> Harness {
    let users: Shared = Arc::default();
    users.people.lock().unwrap().insert(
        OWNER,
        Person {
            email: "owner@example.org".into(),
            name: "Owner".into(),
            active: true,
        },
    );
    let users_url = serve(
        Router::new()
            .route(
                "/internal/v1/workspaces/{ws}/users",
                get(list).post(provision),
            )
            .route(
                "/internal/v1/workspaces/{ws}/users/{id}",
                get(one).patch(rename).delete(deactivate),
            )
            .with_state(users.clone()),
    )
    .await;
    let base = serve(xmsg_scim_ee::router(xmsg_scim_ee::Config {
        token: SCIM_TOKEN.into(),
        workspace_id: "7".into(),
        users_url,
        provisioning_token: SERVICE_TOKEN.into(),
        public_url: "https://chat.example.org/scim/v2".into(),
    }))
    .await;
    Harness {
        users,
        base,
        http: reqwest::Client::new(),
    }
}

impl Harness {
    async fn call(&self, method: &str, path: &str, body: Option<Value>) -> (u16, Value) {
        self.call_as(SCIM_TOKEN, method, path, body).await
    }

    async fn call_as(
        &self,
        token: &str,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (u16, Value) {
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        let mut request = self
            .http
            .request(method, format!("{}/scim/v2{path}", self.base))
            .bearer_auth(token);
        if let Some(body) = body {
            request = request
                .header("content-type", "application/scim+json")
                .body(body.to_string());
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        if status != 204 {
            assert_eq!(
                response.headers()["content-type"],
                "application/scim+json",
                "{path}"
            );
        }
        let text = response.text().await.unwrap();
        (status, serde_json::from_str(&text).unwrap_or(Value::Null))
    }
}

#[tokio::test]
async fn a_provider_creates_finds_updates_and_deactivates_people() {
    let h = harness().await;
    let (status, created) = h
        .call(
            "POST",
            "/Users",
            Some(json!({
                "schemas": [xmsg_scim_ee::USER_SCHEMA],
                "userName": "Ann@Example.org",
                "name": {"givenName": "Ann", "familyName": "Brown"},
                "emails": [{"value": "ann@example.org", "primary": true}],
                "active": true,
            })),
        )
        .await;
    assert_eq!(status, 201, "{created}");
    let id = created["id"].as_str().unwrap().to_string();
    assert_eq!(created["userName"], "ann@example.org");
    assert_eq!(created["displayName"], "Ann Brown");
    assert_eq!(created["active"], true);
    assert_eq!(
        created["meta"]["location"],
        format!("https://chat.example.org/scim/v2/Users/{id}")
    );

    // Creating the same person again is a uniqueness conflict.
    let (status, err) = h
        .call(
            "POST",
            "/Users",
            Some(json!({"userName": "ann@example.org"})),
        )
        .await;
    assert_eq!(
        (status, err["scimType"].as_str()),
        (409, Some("uniqueness"))
    );

    // The provider's lookup before it creates.
    let (status, found) = h
        .call(
            "GET",
            "/Users?filter=userName%20eq%20%22ANN%40example.org%22",
            None,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(found["totalResults"], 1);
    assert_eq!(found["Resources"][0]["id"], id.as_str());
    let (_, none) = h
        .call(
            "GET",
            "/Users?filter=userName%20eq%20%22nobody%40example.org%22",
            None,
        )
        .await;
    assert_eq!(none["totalResults"], 0);
    let (status, err) = h
        .call("GET", "/Users?filter=title%20eq%20%22CTO%22", None)
        .await;
    assert_eq!(
        (status, err["scimType"].as_str()),
        (400, Some("invalidFilter"))
    );

    // Okta-style rename, then Entra-style deactivation.
    let (status, renamed) = h
        .call(
            "PATCH",
            &format!("/Users/{id}"),
            Some(
                json!({"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
                "Operations": [{"op": "replace", "value": {"displayName": "Ann B."}}]}),
            ),
        )
        .await;
    assert_eq!(
        (status, renamed["displayName"].as_str()),
        (200, Some("Ann B."))
    );
    let (status, off) = h
        .call(
            "PATCH",
            &format!("/Users/{id}"),
            Some(json!({"Operations": [{"op": "Replace", "path": "active", "value": "False"}]})),
        )
        .await;
    assert_eq!((status, off["active"].as_bool()), (200, Some(false)));

    // Deactivated people aren't listed, but the filter still finds them...
    let (_, all) = h.call("GET", "/Users", None).await;
    assert_eq!(all["totalResults"], 1, "only the owner: {all}");
    let (_, found) = h
        .call(
            "GET",
            "/Users?filter=userName%20eq%20%22ann%40example.org%22",
            None,
        )
        .await;
    assert_eq!(found["Resources"][0]["active"], false);

    // ...and a PUT with active and a new name brings them back renamed.
    let (status, back) = h
        .call(
            "PUT",
            &format!("/Users/{id}"),
            Some(json!({"userName": "ann@example.org", "displayName": "Ann Brown-Smith", "active": true})),
        )
        .await;
    assert_eq!(status, 200, "{back}");
    assert_eq!(
        (back["active"].as_bool(), back["displayName"].as_str()),
        (Some(true), Some("Ann Brown-Smith"))
    );
    let (status, err) = h
        .call(
            "PUT",
            &format!("/Users/{id}"),
            Some(json!({"userName": "someone.else@example.org"})),
        )
        .await;
    assert_eq!(
        (status, err["scimType"].as_str()),
        (400, Some("mutability"))
    );

    // DELETE deactivates; the user stays visible as inactive.
    let (status, _) = h.call("DELETE", &format!("/Users/{id}"), None).await;
    assert_eq!(status, 204);
    let (status, gone) = h.call("GET", &format!("/Users/{id}"), None).await;
    assert_eq!((status, gone["active"].as_bool()), (200, Some(false)));
    assert!(!h.users.people.lock().unwrap()[&2].active);

    // Re-creating a deactivated person reactivates them (201, same id).
    let (status, again) = h
        .call(
            "POST",
            "/Users",
            Some(json!({"userName": "ann@example.org", "displayName": "Ann"})),
        )
        .await;
    assert_eq!(
        (status, again["id"].as_str(), again["displayName"].as_str()),
        (201, Some(id.as_str()), Some("Ann"))
    );
}

#[tokio::test]
async fn paging_auth_and_the_rules_of_the_workspace() {
    let h = harness().await;
    for i in 0..3 {
        let (status, _) = h
            .call(
                "POST",
                "/Users",
                Some(json!({"userName": format!("p{i}@example.org")})),
            )
            .await;
        assert_eq!(status, 201);
    }
    let (_, page) = h.call("GET", "/Users?startIndex=2&count=2", None).await;
    assert_eq!(page["totalResults"], 4);
    assert_eq!(page["startIndex"], 2);
    assert_eq!(page["itemsPerPage"], 2);
    assert_eq!(page["Resources"][0]["userName"], "p0@example.org");

    let health = h
        .http
        .get(format!("{}/healthz", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), 200, "probes need no token");
    let (status, err) = h.call_as("wrong", "GET", "/Users", None).await;
    assert_eq!((status, err["status"].as_str()), (401, Some("401")));
    let (status, _) = h
        .call_as("wrong", "GET", "/ServiceProviderConfig", None)
        .await;
    assert_eq!(status, 401);
    let (status, config) = h.call("GET", "/ServiceProviderConfig", None).await;
    assert_eq!(
        (status, config["patch"]["supported"].as_bool()),
        (200, Some(true))
    );
    let (_, types) = h.call("GET", "/ResourceTypes", None).await;
    assert_eq!(types["Resources"][0]["endpoint"], "/Users");

    // The owner can't be deprovisioned; the users service's reason comes through.
    let (status, err) = h.call("DELETE", &format!("/Users/{OWNER}"), None).await;
    assert_eq!(status, 409);
    assert_eq!(err["detail"], "the workspace owner can't be deactivated");

    for path in ["/Users/999", "/Users/not-a-number"] {
        let (status, _) = h.call("GET", path, None).await;
        assert_eq!(status, 404, "{path}");
    }
    let (status, err) = h
        .call("POST", "/Users", Some(json!({"userName": "no-email"})))
        .await;
    assert_eq!(
        (status, err["scimType"].as_str()),
        (400, Some("invalidValue"))
    );
}
