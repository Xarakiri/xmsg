// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! The admin API over HTTP, with real access tokens checked against a JWKS and an
//! in-memory store.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use axum::Router;
use chrono::{Duration, Utc};
use serde_json::Value;
use xmsg_admin_ee::{App, Store, Usage, router};
use xmsg_auth::keys::Keys;
use xmsg_ee_license::{Feature, Licence};

const OWNER: i64 = 1;
const MEMBER: i64 = 2;
const OUTSIDER: i64 = 3;

#[derive(Default)]
struct Memory {
    /// (workspace, user) → role.
    roles: HashMap<(i64, i64), String>,
}

impl Store for Memory {
    async fn role(&self, ws: i64, user: i64) -> Result<Option<String>, String> {
        Ok(self.roles.get(&(ws, user)).cloned())
    }
    async fn admin_anywhere(&self, user: i64) -> Result<bool, String> {
        Ok(self
            .roles
            .iter()
            .any(|((_, u), r)| *u == user && (r == "owner" || r == "admin")))
    }
    async fn seats_used(&self) -> Result<i64, String> {
        let mut people: Vec<i64> = self.roles.keys().map(|(_, u)| *u).collect();
        people.sort();
        people.dedup();
        Ok(people.len() as i64)
    }
    async fn usage(&self, ws: i64) -> Result<Usage, String> {
        let mut members_by_role = BTreeMap::new();
        for ((w, _), role) in &self.roles {
            if *w == ws {
                *members_by_role.entry(role.clone()).or_insert(0) += 1;
            }
        }
        Ok(Usage {
            members_by_role,
            channels_by_type: BTreeMap::from([("public".into(), 3)]),
            active_sessions: 4,
            sso_enforced: true,
            ..Usage::default()
        })
    }
}

fn licence(features: Vec<Feature>, expires_in_days: i64) -> Licence {
    Licence {
        licence_id: "lic-1".into(),
        customer: "Acme".into(),
        issued_at: Utc::now() - Duration::days(30),
        expires_at: Utc::now() + Duration::days(expires_in_days),
        users: 50,
        features,
    }
}

/// A stand-in for the built console.
fn console_dir() -> &'static std::path::Path {
    static DIR: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("xmsg-admin-console-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("assets")).unwrap();
        std::fs::write(dir.join("index.html"), "<title>xmsg admin</title>").unwrap();
        std::fs::write(dir.join("assets/app.js"), "console.log(1)").unwrap();
        dir
    })
}

struct Harness {
    base: String,
    keys: Arc<Keys>,
    http: reqwest::Client,
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

async fn harness(licence: Option<Licence>) -> Harness {
    let keys = Arc::new(Keys::generate());
    let jwks = keys.jwks();
    let jwks_url = serve(Router::new().route(
        "/jwks",
        axum::routing::get(move || async move { axum::Json(jwks) }),
    ))
    .await;
    let mut store = Memory::default();
    store.roles.insert((1, OWNER), "owner".into());
    store.roles.insert((1, MEMBER), "member".into());
    store.roles.insert((2, OUTSIDER), "owner".into());
    let app = Arc::new(App {
        store,
        verifier: xmsg_authn::Verifier::new(format!("{jwks_url}/jwks")),
        licence,
    });
    Harness {
        base: serve(router(app, Some(console_dir()))).await,
        keys,
        http: reqwest::Client::new(),
    }
}

impl Harness {
    async fn get(&self, path: &str, user: Option<i64>) -> (u16, Value) {
        let mut request = self.http.get(format!("{}{path}", self.base));
        if let Some(user) = user {
            let token = self.keys.issue(user, 1, Utc::now().timestamp(), 900);
            request = request.bearer_auth(token);
        }
        let response = request.send().await.unwrap();
        let status = response.status().as_u16();
        (status, response.json().await.unwrap_or(Value::Null))
    }
}

#[tokio::test]
async fn admins_see_the_licence_and_their_workspaces_usage() {
    let h = harness(Some(licence(vec![Feature::Sso, Feature::Admin], 90))).await;

    let (status, l) = h.get("/admin/api/v1/licence", Some(OWNER)).await;
    assert_eq!(status, 200, "{l}");
    assert_eq!(l["customer"], "Acme");
    assert_eq!(l["status"], "active");
    assert_eq!(l["seats"], 50);
    assert_eq!(l["seats_used"], 3);
    assert_eq!(l["features"], serde_json::json!(["sso", "admin"]));

    let (status, u) = h.get("/admin/api/v1/workspaces/1/usage", Some(OWNER)).await;
    assert_eq!(status, 200, "{u}");
    assert_eq!(u["members"], 2);
    assert_eq!(u["members_by_role"]["owner"], 1);
    assert_eq!(u["channels_by_type"]["public"], 3);
    assert_eq!(u["sso_enforced"], true);

    // Members, owners of other workspaces and anonymous callers see nothing.
    assert_eq!(h.get("/admin/api/v1/licence", Some(MEMBER)).await.0, 403);
    assert_eq!(
        h.get("/admin/api/v1/workspaces/1/usage", Some(MEMBER))
            .await
            .0,
        403
    );
    assert_eq!(
        h.get("/admin/api/v1/workspaces/1/usage", Some(OUTSIDER))
            .await
            .0,
        403
    );
    assert_eq!(h.get("/admin/api/v1/licence", None).await.0, 401);
    assert_eq!(
        h.get("/admin/api/v1/workspaces/x/usage", Some(OWNER))
            .await
            .0,
        400
    );
    assert_eq!(h.get("/healthz", None).await.0, 200);
}

#[tokio::test]
async fn the_console_is_served_with_its_own_links_working() {
    let h = harness(None).await;
    let page = |path: &'static str| {
        let h = &h;
        async move {
            let r = h
                .http
                .get(format!("{}{path}", h.base))
                .send()
                .await
                .unwrap();
            (r.status().as_u16(), r.text().await.unwrap())
        }
    };
    assert!(page("/admin/").await.1.contains("xmsg admin"));
    assert_eq!(page("/admin/assets/app.js").await.1, "console.log(1)");
    assert!(
        page("/admin/members").await.1.contains("xmsg admin"),
        "unknown paths get the app"
    );
    assert!(
        page("/admin").await.1.contains("xmsg admin"),
        "redirected to /admin/"
    );
    assert_eq!(
        page("/admin/api/v1/nope").await.0,
        404,
        "the API doesn't fall back to the page"
    );
}

#[tokio::test]
async fn analytics_follow_the_licence() {
    // Without a licence: the licence page says so, analytics are off.
    let h = harness(None).await;
    let (status, l) = h.get("/admin/api/v1/licence", Some(OWNER)).await;
    assert_eq!((status, l["licensed"].as_bool()), (200, Some(false)));
    assert_eq!(
        h.get("/admin/api/v1/workspaces/1/usage", Some(OWNER))
            .await
            .0,
        403
    );

    // A licence without `admin`: no analytics either.
    let h = harness(Some(licence(vec![Feature::Sso], 90))).await;
    assert_eq!(
        h.get("/admin/api/v1/workspaces/1/usage", Some(OWNER))
            .await
            .0,
        403
    );

    // Long expired: still readable (ADR-0022), and the status says so.
    let h = harness(Some(licence(vec![Feature::Admin], -400))).await;
    let (_, l) = h.get("/admin/api/v1/licence", Some(OWNER)).await;
    assert_eq!(l["status"], "expired");
    assert_eq!(
        h.get("/admin/api/v1/workspaces/1/usage", Some(OWNER))
            .await
            .0,
        200
    );
}
