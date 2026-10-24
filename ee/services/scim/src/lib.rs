// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! SCIM 2.0 user provisioning (RFC 7643/7644) for one workspace.
//!
//! An identity provider (Okta, Entra ID, …) creates, updates and deactivates the
//! workspace's people here; every change goes through the core users service's
//! provisioning API (`/internal/v1/workspaces/{ws}/users`), so a deactivation is the
//! ordinary workspace removal.
//!
//! | Method | Path | |
//! |---|---|---|
//! | GET | `/scim/v2/ServiceProviderConfig`, `/scim/v2/ResourceTypes` | discovery |
//! | GET | `/scim/v2/Users?filter=userName eq "…"&startIndex=&count=` | find or list |
//! | POST | `/scim/v2/Users` | create, or reactivate a deactivated user |
//! | GET | `/scim/v2/Users/{id}` | one user |
//! | PUT | `/scim/v2/Users/{id}` | replace: name and `active` |
//! | PATCH | `/scim/v2/Users/{id}` | `active` and name, either PatchOp style |
//! | DELETE | `/scim/v2/Users/{id}` | deactivate |
//!
//! The user's `userName` is their email address. Deactivated users stay visible with
//! `active: false` (also after DELETE), so a provider can reactivate them.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{Map, Value, json};

pub const USER_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:User";
const LIST_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:ListResponse";
const ERROR_SCHEMA: &str = "urn:ietf:params:scim:api:messages:2.0:Error";
const PAGE: usize = 500;

pub struct Config {
    /// The bearer token the identity provider sends.
    pub token: String,
    pub workspace_id: String,
    /// The users service, e.g. `http://users:8084`.
    pub users_url: String,
    /// The users service's `XMSG_PROVISIONING_TOKEN`.
    pub provisioning_token: String,
    /// This API as the provider reaches it, e.g. `https://chat.example.com/scim/v2`, for
    /// `meta.location`.
    pub public_url: String,
}

struct Scim {
    config: Config,
    http: reqwest::Client,
}

pub fn router(config: Config) -> Router {
    let scim = Arc::new(Scim {
        config,
        http: reqwest::Client::new(),
    });
    Router::new()
        .route(
            "/scim/v2/ServiceProviderConfig",
            get(service_provider_config),
        )
        .route("/scim/v2/ResourceTypes", get(resource_types))
        .route("/scim/v2/Users", get(list).post(create))
        .route(
            "/scim/v2/Users/{id}",
            get(one).put(replace).patch(patch).delete(delete),
        )
        .route_layer(middleware::from_fn_with_state(scim.clone(), authorize))
        .with_state(scim)
}

// ---- errors -------------------------------------------------------------------------

#[derive(Debug)]
pub struct ScimError {
    status: StatusCode,
    scim_type: Option<&'static str>,
    detail: String,
}

impl ScimError {
    fn new(status: StatusCode, scim_type: Option<&'static str>, detail: impl Into<String>) -> Self {
        ScimError {
            status,
            scim_type,
            detail: detail.into(),
        }
    }
    fn bad(scim_type: &'static str, detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, Some(scim_type), detail)
    }
    fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, None, "no such user")
    }
    fn upstream() -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, None, "try again later")
    }
}

impl IntoResponse for ScimError {
    fn into_response(self) -> Response {
        let mut body = json!({
            "schemas": [ERROR_SCHEMA],
            "status": self.status.as_u16().to_string(),
            "detail": self.detail,
        });
        if let Some(t) = self.scim_type {
            body["scimType"] = json!(t);
        }
        scim_json(self.status, body)
    }
}

fn scim_json(status: StatusCode, body: Value) -> Response {
    let mut response = (status, Json(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/scim+json"),
    );
    response
}

// ---- auth ---------------------------------------------------------------------------

/// Equal without leaking where they differ through timing.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn authorize(State(scim): State<Arc<Scim>>, request: Request, next: Next) -> Response {
    let given = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if same(given.as_bytes(), scim.config.token.as_bytes()) {
        next.run(request).await
    } else {
        ScimError::new(StatusCode::UNAUTHORIZED, None, "invalid or missing token").into_response()
    }
}

// ---- the users service --------------------------------------------------------------

#[derive(Deserialize, Clone, Debug)]
struct User {
    user_id: String,
    email: String,
    name: String,
    active: bool,
}

#[derive(Deserialize)]
struct Page {
    users: Vec<User>,
    next: Option<String>,
}

impl Scim {
    fn url(&self, rest: &str) -> String {
        format!(
            "{}/internal/v1/workspaces/{}/users{rest}",
            self.config.users_url.trim_end_matches('/'),
            self.config.workspace_id
        )
    }

    /// Sends and maps the users service's statuses: `Ok(None)` for 404.
    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<Option<reqwest::Response>, ScimError> {
        let response = request
            .bearer_auth(&self.config.provisioning_token)
            .send()
            .await
            .map_err(|e| {
                tracing::warn!(error = %e, "users service unreachable");
                ScimError::upstream()
            })?;
        let status = response.status();
        if status.is_success() {
            return Ok(Some(response));
        }
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let detail = response
            .json::<Value>()
            .await
            .ok()
            .and_then(|b| b["error"].as_str().map(str::to_string))
            .unwrap_or_default();
        Err(match status {
            reqwest::StatusCode::BAD_REQUEST => ScimError::bad("invalidValue", detail),
            reqwest::StatusCode::CONFLICT => ScimError::new(StatusCode::CONFLICT, None, detail),
            _ => {
                tracing::warn!(%status, detail, "users service failed");
                ScimError::upstream()
            }
        })
    }

    async fn read<T: serde::de::DeserializeOwned>(
        &self,
        response: reqwest::Response,
    ) -> Result<T, ScimError> {
        response.json().await.map_err(|e| {
            tracing::warn!(error = %e, "users service answered something unexpected");
            ScimError::upstream()
        })
    }

    async fn by_email(&self, email: &str) -> Result<Option<User>, ScimError> {
        let request = self.http.get(self.url("")).query(&[("email", email)]);
        match self.send(request).await? {
            Some(r) => Ok(self.read::<Page>(r).await?.users.into_iter().next()),
            None => Ok(None),
        }
    }

    async fn user(&self, id: &str) -> Result<User, ScimError> {
        // IDs are numbers; anything else can't name a user.
        if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ScimError::not_found());
        }
        let response = self
            .send(self.http.get(self.url(&format!("/{id}"))))
            .await?;
        self.read(response.ok_or_else(ScimError::not_found)?).await
    }

    /// Every active member, page by page.
    async fn members(&self) -> Result<Vec<User>, ScimError> {
        let mut all = Vec::new();
        let mut after = String::new();
        loop {
            let request = self
                .http
                .get(self.url(""))
                .query(&[("after", after.as_str()), ("limit", &PAGE.to_string())]);
            let response = self.send(request).await?.ok_or_else(ScimError::upstream)?;
            let page: Page = self.read(response).await?;
            all.extend(page.users);
            match page.next {
                Some(next) => after = next,
                None => return Ok(all),
            }
        }
    }

    /// Creates the user or reactivates them in the workspace.
    async fn provision(&self, email: &str, name: &str) -> Result<User, ScimError> {
        let request = self
            .http
            .post(self.url(""))
            .json(&json!({ "email": email, "name": name }));
        let response = self.send(request).await?.ok_or_else(ScimError::upstream)?;
        self.read(response).await
    }

    async fn rename(&self, id: &str, name: &str) -> Result<(), ScimError> {
        let request = self
            .http
            .patch(self.url(&format!("/{id}")))
            .json(&json!({ "name": name }));
        self.send(request)
            .await?
            .map(|_| ())
            .ok_or_else(ScimError::not_found)
    }

    async fn deactivate(&self, id: &str) -> Result<(), ScimError> {
        self.send(self.http.delete(self.url(&format!("/{id}"))))
            .await?
            .map(|_| ())
            .ok_or_else(ScimError::not_found)
    }

    fn location(&self, path: &str) -> String {
        format!("{}/{path}", self.config.public_url.trim_end_matches('/'))
    }

    fn resource(&self, user: &User) -> Value {
        json!({
            "schemas": [USER_SCHEMA],
            "id": user.user_id,
            "userName": user.email,
            "displayName": user.name,
            "name": { "formatted": user.name },
            "emails": [{ "value": user.email, "type": "work", "primary": true }],
            "active": user.active,
            "meta": {
                "resourceType": "User",
                "location": self.location(&format!("Users/{}", user.user_id)),
            },
        })
    }

    /// Applies a name and an `active` state, in the order that keeps the users service's
    /// rules: reactivate before renaming, rename before deactivating.
    async fn apply(&self, user: User, changes: Changes) -> Result<User, ScimError> {
        let mut active = user.active;
        if changes.active == Some(true) && !active {
            let name = changes.name.as_deref().unwrap_or(&user.name);
            self.provision(&user.email, name).await?;
            active = true;
        }
        if let Some(name) = changes.name.as_deref().map(str::trim)
            && !name.is_empty()
            && name != user.name
        {
            if active {
                self.rename(&user.user_id, name).await?;
            } else {
                // Only members can be renamed; the name follows on reactivation.
                tracing::debug!(user_id = user.user_id, "not renaming a deactivated user");
            }
        }
        if changes.active == Some(false) && active {
            self.deactivate(&user.user_id).await?;
        }
        self.user(&user.user_id).await
    }
}

// ---- reading SCIM bodies -------------------------------------------------------------

fn parse(body: &Bytes) -> Result<Value, ScimError> {
    serde_json::from_slice(body).map_err(|_| ScimError::bad("invalidSyntax", "not JSON"))
}

fn bool_of(v: &Value) -> Result<bool, ScimError> {
    match v {
        Value::Bool(b) => Ok(*b),
        // Entra ID sends "True"/"False".
        Value::String(s) if s.eq_ignore_ascii_case("true") => Ok(true),
        Value::String(s) if s.eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(ScimError::bad("invalidValue", "active must be a boolean")),
    }
}

/// The display name: `displayName`, else `name.formatted`, else given and family name.
fn name_of(user: &Map<String, Value>) -> Option<String> {
    let text = |v: Option<&Value>| {
        v.and_then(Value::as_str)
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let name = user.get("name").and_then(Value::as_object);
    text(user.get("displayName"))
        .or_else(|| name.and_then(|n| text(n.get("formatted"))))
        .or_else(|| {
            let n = name?;
            let parts: Vec<String> = [text(n.get("givenName")), text(n.get("familyName"))]
                .into_iter()
                .flatten()
                .collect();
            (!parts.is_empty()).then(|| parts.join(" "))
        })
}

/// The email: `userName` when it's an address, else the primary (or first) email.
fn email_of(user: &Map<String, Value>) -> Option<String> {
    let user_name = user.get("userName").and_then(Value::as_str).map(str::trim);
    if let Some(u) = user_name.filter(|u| u.contains('@')) {
        return Some(u.to_lowercase());
    }
    let emails = user.get("emails").and_then(Value::as_array)?;
    emails
        .iter()
        .find(|e| e["primary"] == json!(true))
        .or_else(|| emails.first())
        .and_then(|e| e["value"].as_str())
        .map(|e| e.trim().to_lowercase())
}

#[derive(Default, Debug, PartialEq)]
struct Changes {
    active: Option<bool>,
    name: Option<String>,
}

/// Reads a PatchOp: `add`/`replace` of `active` and the name, with a path (Entra ID) or
/// as a value object without one (Okta). Attributes xmsg doesn't keep are ignored.
fn patch_changes(body: &Value) -> Result<Changes, ScimError> {
    let operations = body["Operations"]
        .as_array()
        .ok_or_else(|| ScimError::bad("invalidSyntax", "Operations is required"))?;
    let mut changes = Changes::default();
    let (mut given, mut family) = (None, None);
    for op in operations {
        let kind = op["op"].as_str().unwrap_or("").to_ascii_lowercase();
        if kind == "remove" {
            continue;
        }
        if kind != "add" && kind != "replace" {
            return Err(ScimError::bad(
                "invalidSyntax",
                "op must be add, replace or remove",
            ));
        }
        let mut attributes = Map::new();
        match op["path"].as_str() {
            Some(path) => {
                attributes.insert(path.to_string(), op["value"].clone());
            }
            None => match op["value"].as_object() {
                Some(value) => attributes.extend(value.clone()),
                None => return Err(ScimError::bad("invalidValue", "value must be an object")),
            },
        }
        for (key, value) in &attributes {
            match key.to_ascii_lowercase().as_str() {
                "active" => changes.active = Some(bool_of(value)?),
                "displayname" | "name.formatted" => {
                    changes.name = value.as_str().map(str::to_string);
                }
                "name" => {
                    if let Some(n) = value.as_object() {
                        let mut wrapped = Map::new();
                        wrapped.insert("name".into(), Value::Object(n.clone()));
                        changes.name = name_of(&wrapped).or(changes.name);
                    }
                }
                "name.givenname" => given = value.as_str().map(str::to_string),
                "name.familyname" => family = value.as_str().map(str::to_string),
                _ => {}
            }
        }
    }
    if changes.name.is_none() && (given.is_some() || family.is_some()) {
        let parts: Vec<String> = [given, family].into_iter().flatten().collect();
        changes.name = Some(parts.join(" "));
    }
    Ok(changes)
}

/// The one filter providers need: `userName eq "…"` (or `emails.value eq "…"`).
fn filter_email(filter: &str) -> Result<String, ScimError> {
    let unsupported = || ScimError::bad("invalidFilter", "only userName eq \"…\" is supported");
    let (attribute, rest) = filter.trim().split_once(' ').ok_or_else(unsupported)?;
    let (op, value) = rest.trim_start().split_once(' ').ok_or_else(unsupported)?;
    let known = attribute.eq_ignore_ascii_case("userName")
        || attribute.eq_ignore_ascii_case("emails.value");
    if !known || !op.eq_ignore_ascii_case("eq") {
        return Err(unsupported());
    }
    let value: String = serde_json::from_str(value.trim())
        .map_err(|_| ScimError::bad("invalidFilter", "the value must be a quoted string"))?;
    Ok(value.trim().to_lowercase())
}

// ---- handlers -----------------------------------------------------------------------

async fn service_provider_config() -> Response {
    scim_json(
        StatusCode::OK,
        json!({
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServiceProviderConfig"],
            "patch": { "supported": true },
            "bulk": { "supported": false, "maxOperations": 0, "maxPayloadSize": 0 },
            "filter": { "supported": true, "maxResults": PAGE },
            "changePassword": { "supported": false },
            "sort": { "supported": false },
            "etag": { "supported": false },
            "authenticationSchemes": [{
                "type": "oauthbearertoken",
                "name": "Bearer token",
                "description": "The token configured as XMSG_EE_SCIM_TOKEN",
            }],
        }),
    )
}

async fn resource_types(State(scim): State<Arc<Scim>>) -> Response {
    let users = json!({
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ResourceType"],
        "id": "User",
        "name": "User",
        "endpoint": "/Users",
        "schema": USER_SCHEMA,
        "meta": {
            "resourceType": "ResourceType",
            "location": scim.location("ResourceTypes/User"),
        },
    });
    scim_json(
        StatusCode::OK,
        json!({
            "schemas": [LIST_SCHEMA],
            "totalResults": 1,
            "startIndex": 1,
            "itemsPerPage": 1,
            "Resources": [users],
        }),
    )
}

#[derive(Deserialize)]
struct ListQuery {
    filter: Option<String>,
    #[serde(rename = "startIndex")]
    start_index: Option<usize>,
    count: Option<usize>,
}

async fn list(
    State(scim): State<Arc<Scim>>,
    Query(q): Query<ListQuery>,
) -> Result<Response, ScimError> {
    let users = match q.filter.as_deref() {
        // Deactivated users are found too, so the provider reactivates instead of creating.
        Some(f) => scim
            .by_email(&filter_email(f)?)
            .await?
            .into_iter()
            .collect(),
        None => scim.members().await?,
    };
    let start = q.start_index.unwrap_or(1).max(1);
    let count = q.count.unwrap_or(100).min(PAGE);
    let page: Vec<Value> = users
        .iter()
        .skip(start - 1)
        .take(count)
        .map(|u| scim.resource(u))
        .collect();
    Ok(scim_json(
        StatusCode::OK,
        json!({
            "schemas": [LIST_SCHEMA],
            "totalResults": users.len(),
            "startIndex": start,
            "itemsPerPage": page.len(),
            "Resources": page,
        }),
    ))
}

async fn create(State(scim): State<Arc<Scim>>, body: Bytes) -> Result<Response, ScimError> {
    let body = parse(&body)?;
    let user = body
        .as_object()
        .ok_or_else(|| ScimError::bad("invalidSyntax", "a User object is required"))?;
    let email = email_of(user)
        .ok_or_else(|| ScimError::bad("invalidValue", "userName must be an email address"))?;
    let active = match user.get("active") {
        Some(v) => bool_of(v)?,
        None => true,
    };
    if let Some(existing) = scim.by_email(&email).await?
        && existing.active
    {
        return Err(ScimError::new(
            StatusCode::CONFLICT,
            Some("uniqueness"),
            "a user with this userName already exists",
        ));
    }
    let name = name_of(user).unwrap_or_default();
    let mut created = scim.provision(&email, &name).await?;
    if !name.is_empty() && created.name != name {
        // A deactivated user coming back keeps their old name until renamed.
        scim.rename(&created.user_id, &name).await?;
        created.name = name;
    }
    if !active {
        scim.deactivate(&created.user_id).await?;
        created.active = false;
    }
    tracing::info!(user_id = created.user_id, "SCIM user created");
    let resource = scim.resource(&created);
    let mut response = scim_json(StatusCode::CREATED, resource);
    if let Ok(location) =
        HeaderValue::from_str(&scim.location(&format!("Users/{}", created.user_id)))
    {
        response.headers_mut().insert(header::LOCATION, location);
    }
    Ok(response)
}

async fn one(State(scim): State<Arc<Scim>>, Path(id): Path<String>) -> Result<Response, ScimError> {
    let user = scim.user(&id).await?;
    Ok(scim_json(StatusCode::OK, scim.resource(&user)))
}

async fn replace(
    State(scim): State<Arc<Scim>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Response, ScimError> {
    let body = parse(&body)?;
    let new = body
        .as_object()
        .ok_or_else(|| ScimError::bad("invalidSyntax", "a User object is required"))?;
    let user = scim.user(&id).await?;
    if let Some(email) = email_of(new)
        && email != user.email
    {
        return Err(ScimError::bad("mutability", "userName can't change"));
    }
    let changes = Changes {
        active: new.get("active").map(bool_of).transpose()?,
        name: name_of(new),
    };
    let user = scim.apply(user, changes).await?;
    Ok(scim_json(StatusCode::OK, scim.resource(&user)))
}

async fn patch(
    State(scim): State<Arc<Scim>>,
    Path(id): Path<String>,
    body: Bytes,
) -> Result<Response, ScimError> {
    let changes = patch_changes(&parse(&body)?)?;
    let user = scim.user(&id).await?;
    let user = scim.apply(user, changes).await?;
    Ok(scim_json(StatusCode::OK, scim.resource(&user)))
}

async fn delete(
    State(scim): State<Arc<Scim>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ScimError> {
    let user = scim.user(&id).await?;
    scim.deactivate(&user.user_id).await?;
    tracing::info!(user_id = user.user_id, "SCIM user deactivated");
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_compare_whole() {
        assert!(same(b"secret", b"secret"));
        assert!(!same(b"secret", b"secret2"));
        assert!(!same(b"", b"secret"));
    }

    #[test]
    fn filters_find_one_email() {
        assert_eq!(
            filter_email(r#"userName eq "Ann@Example.org""#).unwrap(),
            "ann@example.org"
        );
        assert_eq!(
            filter_email(r#"emails.value EQ "a@b.co""#).unwrap(),
            "a@b.co"
        );
        for bad in [
            r#"userName sw "a""#,
            r#"displayName eq "a""#,
            "userName eq a",
            "userName",
        ] {
            assert_eq!(
                filter_email(bad).unwrap_err().scim_type,
                Some("invalidFilter"),
                "{bad}"
            );
        }
    }

    #[test]
    fn patches_read_both_provider_styles() {
        // Entra ID: a path per attribute, string booleans, capitalised ops.
        let entra = json!({"Operations": [
            {"op": "Replace", "path": "active", "value": "False"},
            {"op": "Add", "path": "displayName", "value": "Ann B."},
        ]});
        assert_eq!(
            patch_changes(&entra).unwrap(),
            Changes {
                active: Some(false),
                name: Some("Ann B.".into())
            }
        );
        // Okta: one value object, no path.
        let okta = json!({"Operations": [{"op": "replace", "value": {"active": true}}]});
        assert_eq!(
            patch_changes(&okta).unwrap(),
            Changes {
                active: Some(true),
                name: None
            }
        );
        let parts = json!({"Operations": [
            {"op": "replace", "path": "name.givenName", "value": "Ann"},
            {"op": "replace", "path": "name.familyName", "value": "Brown"},
            {"op": "remove", "path": "title"},
            {"op": "replace", "path": "title", "value": "CTO"},
        ]});
        assert_eq!(
            patch_changes(&parts).unwrap().name.as_deref(),
            Some("Ann Brown")
        );
        assert!(patch_changes(&json!({"Operations": [{"op": "move"}]})).is_err());
        assert!(patch_changes(&json!({})).is_err());
    }

    #[test]
    fn names_and_emails_come_from_the_usual_places() {
        let user = |v: Value| v.as_object().unwrap().clone();
        assert_eq!(
            name_of(&user(
                json!({"name": {"givenName": "Ann", "familyName": "Brown"}})
            ))
            .as_deref(),
            Some("Ann Brown")
        );
        assert_eq!(
            name_of(&user(
                json!({"displayName": "A", "name": {"formatted": "B"}})
            ))
            .as_deref(),
            Some("A")
        );
        assert_eq!(
            email_of(&user(json!({"userName": "ann", "emails": [
                {"value": "x@y.co"}, {"value": "Ann@Y.co", "primary": true}
            ]})))
            .as_deref(),
            Some("ann@y.co")
        );
        assert_eq!(email_of(&user(json!({"userName": "ann"}))), None);
    }
}
