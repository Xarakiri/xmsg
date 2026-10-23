// SPDX-License-Identifier: AGPL-3.0-only
//! Reactions (plan §6.8, ADR-0015).
//!
//! `PUT` / `DELETE /v1/channels/{id}/messages/{msg_seq}/reactions/{emoji}?counter=N` checks
//! that the caller is a member and the message exists (and isn't deleted), then produces
//! a `ReactionCommand` to `reactions.in`, keyed by channel. The reaction-batcher turns a
//! second's worth into one batch per (message, emoji); the sequencer applies each user's
//! latest operation by `counter`, so a retried or late request can't undo a newer one.
//! The answer is 202; the result reaches every member as `reactions.changed`.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use xmsg_proto::Message;
use xmsg_proto::v1::ReactionCommand;

use crate::EventLog;
use crate::edits::{Refused, member_target};
use crate::history::{App, Store, caller, error};

/// An emoji: a short name like `thumbsup` or the character itself.
pub const MAX_EMOJI_BYTES: usize = 64;

#[derive(Deserialize)]
pub struct Counter {
    /// Client counter per (user, message, emoji), increasing (a timestamp will do).
    counter: i64,
}

pub fn valid_emoji(emoji: &str) -> bool {
    !emoji.is_empty()
        && emoji.len() <= MAX_EMOJI_BYTES
        && !emoji.chars().any(|c| c.is_whitespace() || c.is_control())
}

/// Checks a reaction and builds its command.
pub async fn plan(
    store: &impl Store,
    user: i64,
    channel_id: i64,
    msg_seq: i64,
    emoji: &str,
    add: bool,
    counter: i64,
) -> Result<ReactionCommand, Refused> {
    if !valid_emoji(emoji) {
        return Err(Refused::Invalid("emoji must be 1-64 bytes without spaces"));
    }
    if counter <= 0 {
        return Err(Refused::Invalid("counter must be positive"));
    }
    let row = member_target(store, user, channel_id, msg_seq).await?;
    if row.deleted {
        return Err(Refused::NotFound);
    }
    Ok(ReactionCommand {
        channel_id,
        message_id: row.message_id,
        msg_seq,
        user_id: user,
        emoji: emoji.to_string(),
        add,
        counter,
    })
}

async fn handle<S: Store, L: EventLog>(
    app: &App<S, L>,
    headers: &HeaderMap,
    (channel_id, msg_seq, emoji): (i64, i64, String),
    counter: i64,
    add: bool,
) -> Response {
    let user = match caller(&app.verifier, headers).await {
        Ok(u) => u,
        Err((status, message)) => return error(status, message),
    };
    let command = match plan(&app.store, user, channel_id, msg_seq, &emoji, add, counter).await {
        Ok(c) => c,
        Err(refused) => return refused.into_response(),
    };
    match app
        .reactions
        .append(channel_id, command.encode_to_vec())
        .await
    {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(serde_json::json!({ "status": "accepted" })),
        )
            .into_response(),
        Err(e) => {
            tracing::warn!(error = %e, channel_id, "append to reactions.in failed");
            Refused::Unavailable.into_response()
        }
    }
}

pub async fn add<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path(target): Path<(i64, i64, String)>,
    Query(q): Query<Counter>,
) -> Response {
    handle(&app, &headers, target, q.counter, true).await
}

pub async fn remove<S: Store, L: EventLog>(
    State(app): State<Arc<App<S, L>>>,
    headers: HeaderMap,
    Path(target): Path<(i64, i64, String)>,
    Query(q): Query<Counter>,
) -> Response {
    handle(&app, &headers, target, q.counter, false).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emoji_are_short_names_or_characters() {
        assert!(valid_emoji("👍"));
        assert!(valid_emoji("thumbsup"));
        assert!(valid_emoji("👨‍👩‍👧‍👦"));
        assert!(!valid_emoji(""));
        assert!(!valid_emoji("two words"));
        assert!(!valid_emoji(&"x".repeat(65)));
    }
}
