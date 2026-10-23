// SPDX-License-Identifier: AGPL-3.0-only
//! Thread reads (ADR-0005): `GET /v1/channels/{id}/threads/{root_msg_seq}?after=&limit=`
//! returns `{replies: [{reply_seq, message_id, author_id, body}], has_more}` from
//! `thread_messages`, in reply order. Members only; anyone else gets 404, as for history.
//! Live replies arrive as `thread.reply` events; this is for opening a thread.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::history::{App, Store, caller, error};

pub const DEFAULT_LIMIT: usize = 200;
pub const MAX_LIMIT: usize = 500;

#[derive(Deserialize)]
pub struct Page {
    #[serde(default)]
    after: i64,
    limit: Option<usize>,
}

pub async fn replies<S: Store, L>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path((channel_id, root_msg_seq)): Path<(i64, i64)>,
    Query(page): Query<Page>,
) -> Response {
    let user = match caller(&app.verifier, &headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    match app.store.is_member(channel_id, user).await {
        Ok(true) => {}
        Ok(false) => return error(StatusCode::NOT_FOUND, "unknown channel"),
        Err(e) => {
            tracing::error!(error = %e, "membership read failed");
            return error(StatusCode::SERVICE_UNAVAILABLE, "try again later");
        }
    }
    let limit = page.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    // One more than asked: tells whether there's another page.
    match app
        .store
        .thread(channel_id, root_msg_seq, page.after.max(0), limit + 1)
        .await
    {
        Ok(mut rows) => {
            let has_more = rows.len() > limit;
            rows.truncate(limit);
            let replies: Vec<serde_json::Value> = rows
                .iter()
                .map(|r| {
                    serde_json::json!({
                        "reply_seq": r.reply_seq.to_string(),
                        "message_id": r.message_id.to_string(),
                        "author_id": r.author_id.to_string(),
                        "body": r.body,
                    })
                })
                .collect();
            Json(serde_json::json!({ "replies": replies, "has_more": has_more })).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, channel_id, root_msg_seq, "thread read failed");
            error(StatusCode::SERVICE_UNAVAILABLE, "try again later")
        }
    }
}
