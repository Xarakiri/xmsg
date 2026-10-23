// SPDX-License-Identifier: AGPL-3.0-only
//! Search (plan §5.12): `GET /v1/search?q=&limit=` over the documents the search-indexer
//! keeps in OpenSearch, **only in the caller's channels**: the query is filtered by the
//! channels they're in now (`channels_by_user`, up to 1,000), so a result can never come
//! from a channel they can't read. Search is optional on the Small tier; without
//! `XMSG_SEARCH_URL` the answer is 503.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::history::{App, Store, caller, error};

/// The index the search-indexer writes (`xmsg-search-indexer`).
pub const INDEX: &str = "xmsg-messages-v1";
pub const MAX_QUERY_CHARS: usize = 200;
pub const DEFAULT_LIMIT: usize = 20;
pub const MAX_LIMIT: usize = 50;

pub struct Search {
    url: String,
    http: reqwest::Client,
}

impl Search {
    pub fn new(url: String) -> Self {
        Search {
            url: url.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }
}

#[derive(Deserialize)]
pub struct Params {
    q: String,
    limit: Option<usize>,
}

/// The OpenSearch request: all words must match, only in `channels`, best first.
pub fn query_body(q: &str, channels: &[i64], limit: usize) -> Value {
    json!({
        "size": limit,
        "query": { "bool": {
            "must": [{ "simple_query_string": { "query": q, "fields": ["body"], "default_operator": "and" } }],
            "filter": [{ "terms": { "channel_id": channels } }]
        }},
        "sort": ["_score", { "at_ms": "desc" }]
    })
}

/// Hits as the API returns them (ids as strings, as everywhere).
pub fn results(response: &Value) -> Vec<Value> {
    response["hits"]["hits"]
        .as_array()
        .map(|hits| {
            hits.iter()
                .map(|h| {
                    let s = &h["_source"];
                    let id = |k: &str| s[k].as_i64().map(|v| v.to_string()).unwrap_or_default();
                    json!({
                        "channel_id": id("channel_id"),
                        "msg_seq": id("msg_seq"),
                        "message_id": id("message_id"),
                        "author_id": id("author_id"),
                        "kind": s["kind"],
                        "root_msg_seq": id("root_msg_seq"),
                        "body": s["body"],
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub async fn search<S: Store, L>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
) -> Response {
    let user = match caller(&app.verifier, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    let Some(search) = &app.search else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "search is not enabled");
    };
    let q = params.q.trim();
    if q.is_empty() || q.chars().count() > MAX_QUERY_CHARS {
        return error(StatusCode::BAD_REQUEST, "q must be 1-200 characters");
    }
    let channels = match app.store.my_channels(user).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "channel list read failed");
            return error(StatusCode::SERVICE_UNAVAILABLE, "try again later");
        }
    };
    if channels.is_empty() {
        return Json(json!({ "results": [] })).into_response();
    }
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let response = search
        .http
        .post(format!("{}/{INDEX}/_search", search.url))
        .header("content-type", "application/json")
        .body(query_body(q, &channels, limit).to_string())
        .send()
        .await;
    let body = match response {
        Ok(r) if r.status().is_success() => r.text().await.unwrap_or_default(),
        Ok(r) => {
            tracing::warn!(status = %r.status(), "search query failed");
            return error(StatusCode::SERVICE_UNAVAILABLE, "try again later");
        }
        Err(e) => {
            tracing::warn!(error = %e, "search unreachable");
            return error(StatusCode::SERVICE_UNAVAILABLE, "try again later");
        }
    };
    let parsed: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
    Json(json!({ "results": results(&parsed) })).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_scoped_to_the_callers_channels() {
        let body = query_body("lunch plans", &[7, 9], 20);
        assert_eq!(
            body["query"]["bool"]["filter"][0]["terms"]["channel_id"],
            json!([7, 9])
        );
        assert_eq!(
            body["query"]["bool"]["must"][0]["simple_query_string"]["default_operator"],
            "and"
        );
        assert_eq!(body["size"], 20);
    }

    #[test]
    fn hits_come_back_with_string_ids() {
        let response = json!({"hits": {"hits": [{"_source": {
            "channel_id": 7, "msg_seq": 3, "message_id": 500, "author_id": 42,
            "kind": "message", "root_msg_seq": 0, "body": "lunch?", "workspace_id": 1, "at_ms": 1
        }}]}});
        assert_eq!(
            results(&response),
            vec![json!({
                "channel_id": "7", "msg_seq": "3", "message_id": "500", "author_id": "42",
                "kind": "message", "root_msg_seq": "0", "body": "lunch?"
            })]
        );
        assert!(results(&json!({})).is_empty());
    }
}
