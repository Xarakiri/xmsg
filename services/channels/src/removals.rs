// SPDX-License-Identifier: AGPL-3.0-only
//! The workspace-removal job (plan §6.5). When the users service removes someone from a
//! workspace (`users.events`: `member_removed`), this takes them out of each of the
//! workspace's channels they're in, through the same sequencer commands as a user
//! leaving (ADR-0006), then records `workspace.member_removal_completed` in the outbox so
//! fan-out can drop its deny-list entry. Everything is idempotent: a replayed removal
//! re-sends leaves the sequencer dedupes or rejects (not a member), harmlessly.

use std::time::Duration;

use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message as _};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    InboundEvent, MemberKickRequested, RejectReason, UsersEvent, WorkspaceMemberRemovalCompleted,
    inbound_event, users_event,
};

use crate::api::App;
use crate::kafka::EventLog;
use crate::memberships::Memberships;
use crate::request_id;
use crate::store::Store;

pub const TOPIC_USERS_EVENTS: &str = "users.events";
/// Attempts per channel when the sequencer hasn't answered in time.
const ATTEMPTS: usize = 3;

/// Takes `user_id` out of `workspace_id`'s channels and records the completion; returns
/// how many channel memberships it ended. A user who is back in the workspace (invited
/// again after the removal) is left alone: the event is stale.
pub async fn remove_from_workspace<S: Store, L: EventLog, M: Memberships>(
    app: &App<S, L, M>,
    workspace_id: i64,
    user_id: i64,
    removed_at_ms: i64,
) -> Result<i32, String> {
    let outside = app
        .store
        .outside_workspace(workspace_id, &[user_id])
        .await
        .map_err(|e| e.to_string())?;
    // Back in the workspace (invited again): leave their channels alone, but still record
    // the completion, so fan-out stops withholding the workspace from them.
    let channels: Vec<i64> = if outside.is_empty() {
        tracing::info!(
            workspace_id,
            user_id,
            "member is back in the workspace; no channels to leave"
        );
        Vec::new()
    } else {
        app.memberships
            .channels_of(user_id)
            .await?
            .into_iter()
            .filter(|c| c.workspace_id == workspace_id)
            .map(|c| c.channel_id)
            .collect()
    };
    let mut ended = 0;
    for channel_id in channels {
        if leave(app, workspace_id, user_id, removed_at_ms, channel_id).await? {
            ended += 1;
        }
    }
    let completed = UsersEvent {
        event: Some(users_event::Event::RemovalCompleted(
            WorkspaceMemberRemovalCompleted {
                workspace_id,
                user_id,
                channels: ended,
                completed_at_ms: chrono_now_ms(),
            },
        )),
    };
    app.store
        .outbox_workspace_event(
            workspace_id,
            "workspace.member_removal_completed",
            &completed.encode_to_vec(),
        )
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        workspace_id,
        user_id,
        channels = ended,
        "workspace removal completed"
    );
    Ok(ended)
}

/// One channel: the user leaves (actor = the user, which the sequencer always allows for
/// a member). `true` if this ended a membership, `false` if they weren't a member.
async fn leave<S, L: EventLog, M>(
    app: &App<S, L, M>,
    workspace_id: i64,
    user_id: i64,
    removed_at_ms: i64,
    channel_id: i64,
) -> Result<bool, String> {
    for attempt in 1..=ATTEMPTS {
        let request_id = request_id();
        let event = InboundEvent {
            channel_id,
            actor_id: user_id,
            // The same for every attempt and replay of this removal (the sequencer drops
            // repeats), but not for a later removal after the user was invited back.
            dedupe_id: format!("wsremove:{workspace_id}:{user_id}:{removed_at_ms}:{channel_id}"),
            request_id: request_id.clone(),
            payload: Some(inbound_event::Payload::MemberKickRequested(
                MemberKickRequested { user_id },
            )),
            ..Default::default()
        };
        let waiter = app.pending.register(&request_id);
        app.log.append(&event).await?;
        match waiter.wait(app.command_timeout).await {
            Some(r) if r.accepted => return Ok(true),
            Some(r)
                if r.reason == RejectReason::NotAMember as i32
                    || r.reason == RejectReason::UnknownChannel as i32 =>
            {
                return Ok(false);
            }
            Some(r) => {
                return Err(format!(
                    "leave of channel {channel_id} rejected: {:?}",
                    r.reason
                ));
            }
            // A repeat within the dedupe window gets no verdict at all: the first attempt
            // was sequenced already (or will be). Asking again can't tell us more.
            None if attempt == ATTEMPTS => {
                tracing::warn!(
                    channel_id,
                    user_id,
                    "no verdict for the removal leave; assuming it was applied"
                );
                return Ok(true);
            }
            None => {}
        }
    }
    unreachable!("the loop returns on its last attempt")
}

fn chrono_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Consumes `users.events` and runs the job for every removal. Offsets are committed only
/// after a removal completed, so a crash re-runs it (idempotently).
pub async fn run<S: Store, L: EventLog, M: Memberships>(
    app: std::sync::Arc<App<S, L, M>>,
    bootstrap: String,
) {
    let consumer: StreamConsumer = match ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "xmsg-channels-removals")
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
    {
        Ok(c) => c,
        Err(e) => return tracing::error!(error = %e, "removal job: Kafka consumer"),
    };
    if let Err(e) = consumer.subscribe(&[TOPIC_USERS_EVENTS]) {
        return tracing::error!(error = %e, "removal job: subscribing to users.events");
    }
    tracing::info!("workspace removal job ready");
    loop {
        let message = match consumer.recv().await {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "removal job: receive error");
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if let Ok(UsersEvent {
            event: Some(users_event::Event::MemberRemoved(r)),
        }) = UsersEvent::decode(message.payload().unwrap_or_default())
        {
            let mut backoff = Duration::from_millis(500);
            while let Err(e) =
                remove_from_workspace(&app, r.workspace_id, r.user_id, r.removed_at_ms).await
            {
                tracing::warn!(error = %e, workspace_id = r.workspace_id, user_id = r.user_id, "workspace removal failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        }
        if let Err(e) = consumer.commit_message(&message, CommitMode::Async) {
            tracing::warn!(error = %e, "removal job: offset commit failed (a removal may re-run, harmlessly)");
        }
    }
}
