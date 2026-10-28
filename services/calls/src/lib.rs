// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-calls (ADR-0025): voice and video calls through LiveKit.
//!
//! - Members ask for a call in a channel (`POST /v1/channels/{id}/call`) and get a LiveKit
//!   access token for its room: the channel's active call, or a new one. DMs and group DMs
//!   get a new room per call (the others are rung); other channels have one huddle room.
//! - LiveKit's webhooks say who joined and left. Each change becomes a `CallEvent` on
//!   `messages.in`, sequenced like any channel event, so every member's client learns who
//!   is in the call (and history shows it).

pub mod api;
pub mod cassandra;
pub mod livekit;

use serde::Deserialize;
use xmsg_proto::Message as _;
use xmsg_proto::v1::{CallChange, CallEvent, InboundEvent, inbound_event::Payload};

/// A room name: `c{channel}-{random}` for a DM call, `h{channel}` for a channel's huddle.
pub fn call_room(channel_id: i64) -> String {
    format!("c{channel_id}-{:016x}", rand::random::<u64>())
}

pub fn huddle_room(channel_id: i64) -> String {
    format!("h{channel_id}")
}

/// The channel a room belongs to, and whether it's a huddle. Rooms not named by us: None.
pub fn parse_room(name: &str) -> Option<(i64, bool)> {
    if let Some(rest) = name.strip_prefix('h') {
        return rest
            .parse()
            .ok()
            .filter(|c: &i64| *c > 0)
            .map(|c| (c, true));
    }
    let (channel, suffix) = name.strip_prefix('c')?.split_once('-')?;
    (!suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_hexdigit()))
        .then(|| channel.parse().ok().filter(|c: &i64| *c > 0))
        .flatten()
        .map(|c| (c, false))
}

/// A channel's call while it lasts.
#[derive(Clone, Debug, PartialEq)]
pub struct ActiveCall {
    pub call_id: String,
    pub huddle: bool,
    pub started_at_ms: i64,
    pub participants: Vec<i64>,
    /// STARTED has been announced (the first join).
    pub announced: bool,
}

/// Call state (Cassandra in production).
pub trait Calls: Send + Sync + 'static {
    fn is_member(
        &self,
        channel_id: i64,
        user_id: i64,
    ) -> impl Future<Output = Result<bool, String>> + Send;
    /// `dm`, `group_dm`, `public`, `private`, `broadcast`; None if unknown.
    fn channel_type(
        &self,
        channel_id: i64,
    ) -> impl Future<Output = Result<Option<String>, String>> + Send;
    /// Creates the channel's call unless one is active; returns whichever is.
    fn start(
        &self,
        channel_id: i64,
        call: &ActiveCall,
    ) -> impl Future<Output = Result<ActiveCall, String>> + Send;
    fn active(
        &self,
        channel_id: i64,
    ) -> impl Future<Output = Result<Option<ActiveCall>, String>> + Send;
    /// Adds or removes a participant of the call `call_id`; the call after the change, or
    /// None if that call isn't the channel's active one any more.
    fn set_participant(
        &self,
        channel_id: i64,
        call_id: &str,
        user_id: i64,
        present: bool,
    ) -> impl Future<Output = Result<Option<ActiveCall>, String>> + Send;
    /// Marks STARTED as announced; true if this call did it (first).
    fn announce(
        &self,
        channel_id: i64,
        call_id: &str,
    ) -> impl Future<Output = Result<bool, String>> + Send;
    /// Ends the call `call_id`; the call as it was, or None if it wasn't active.
    fn finish(
        &self,
        channel_id: i64,
        call_id: &str,
    ) -> impl Future<Output = Result<Option<ActiveCall>, String>> + Send;
}

/// Where call events go: `messages.in`, keyed by channel.
pub trait EventLog: Send + Sync + 'static {
    fn append(
        &self,
        channel_id: i64,
        event: Vec<u8>,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// The parts of a LiveKit webhook we use.
#[derive(Debug, Deserialize)]
pub struct Webhook {
    pub event: String,
    #[serde(default)]
    pub id: String,
    pub room: Option<Room>,
    pub participant: Option<Participant>,
}

#[derive(Debug, Deserialize)]
pub struct Room {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct Participant {
    pub identity: String,
}

fn event(
    channel_id: i64,
    actor_id: i64,
    dedupe_id: String,
    call: &ActiveCall,
    change: CallChange,
    now_ms: i64,
) -> InboundEvent {
    InboundEvent {
        channel_id,
        actor_id,
        dedupe_id,
        payload: Some(Payload::CallEvent(CallEvent {
            call_id: call.call_id.clone(),
            change: change as i32,
            huddle: call.huddle,
            participant_ids: call.participants.clone(),
            started_at_ms: call.started_at_ms,
            duration_ms: if change == CallChange::Ended {
                (now_ms - call.started_at_ms).max(0)
            } else {
                0
            },
        })),
        ..Default::default()
    }
}

/// Applies one webhook: updates the call and emits what changed. Returns how many events
/// were emitted. Webhooks for rooms we didn't name, or other kinds, change nothing.
pub async fn on_webhook(
    calls: &impl Calls,
    log: &impl EventLog,
    hook: &Webhook,
    now_ms: i64,
) -> Result<usize, String> {
    let Some(room) = &hook.room else { return Ok(0) };
    let Some((channel_id, _)) = parse_room(&room.name) else {
        return Ok(0);
    };
    let user = hook
        .participant
        .as_ref()
        .and_then(|p| p.identity.parse::<i64>().ok())
        .unwrap_or(0);
    // LiveKit retries webhooks: the sequencer drops repeats of the same id.
    let dedupe = |suffix: &str| {
        format!(
            "lk:{}{suffix}",
            if hook.id.is_empty() {
                &room.name
            } else {
                &hook.id
            }
        )
    };
    let mut emitted = Vec::new();
    match hook.event.as_str() {
        "participant_joined" | "participant_left" if user > 0 => {
            let joined = hook.event == "participant_joined";
            let Some(call) = calls
                .set_participant(channel_id, &room.name, user, joined)
                .await?
            else {
                return Ok(0);
            };
            if joined && calls.announce(channel_id, &room.name).await? {
                emitted.push(event(
                    channel_id,
                    user,
                    dedupe(""),
                    &call,
                    CallChange::Started,
                    now_ms,
                ));
            } else {
                emitted.push(event(
                    channel_id,
                    user,
                    dedupe(""),
                    &call,
                    CallChange::Participants,
                    now_ms,
                ));
            }
        }
        "room_finished" => {
            if let Some(mut call) = calls.finish(channel_id, &room.name).await? {
                call.participants.clear();
                emitted.push(event(
                    channel_id,
                    0,
                    dedupe(""),
                    &call,
                    CallChange::Ended,
                    now_ms,
                ));
            }
        }
        _ => {}
    }
    for e in &emitted {
        log.append(channel_id, e.encode_to_vec()).await?;
    }
    Ok(emitted.len())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use xmsg_proto::v1::InboundEvent;

    use super::*;

    #[test]
    fn rooms_name_their_channel() {
        let room = call_room(77);
        assert_eq!(parse_room(&room), Some((77, false)));
        assert_eq!(parse_room(&huddle_room(77)), Some((77, true)));
        for other in [
            "", "h", "h-1", "hx", "c77", "c77-", "c77-zz", "cx-ab", "room", "c0-ab",
        ] {
            assert_eq!(parse_room(other), None, "{other:?}");
        }
    }

    #[derive(Default)]
    pub struct Fake {
        pub calls: Mutex<HashMap<i64, ActiveCall>>,
        pub log: Mutex<Vec<InboundEvent>>,
    }

    impl Calls for Fake {
        async fn is_member(&self, _: i64, _: i64) -> Result<bool, String> {
            Ok(true)
        }
        async fn channel_type(&self, _: i64) -> Result<Option<String>, String> {
            Ok(Some("dm".into()))
        }
        async fn start(&self, channel_id: i64, call: &ActiveCall) -> Result<ActiveCall, String> {
            Ok(self
                .calls
                .lock()
                .unwrap()
                .entry(channel_id)
                .or_insert_with(|| call.clone())
                .clone())
        }
        async fn active(&self, channel_id: i64) -> Result<Option<ActiveCall>, String> {
            Ok(self.calls.lock().unwrap().get(&channel_id).cloned())
        }
        async fn set_participant(
            &self,
            channel_id: i64,
            call_id: &str,
            user: i64,
            present: bool,
        ) -> Result<Option<ActiveCall>, String> {
            let mut calls = self.calls.lock().unwrap();
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
            let mut calls = self.calls.lock().unwrap();
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
        async fn finish(
            &self,
            channel_id: i64,
            call_id: &str,
        ) -> Result<Option<ActiveCall>, String> {
            let mut calls = self.calls.lock().unwrap();
            if calls.get(&channel_id).is_some_and(|c| c.call_id == call_id) {
                return Ok(calls.remove(&channel_id));
            }
            Ok(None)
        }
    }

    impl EventLog for Fake {
        async fn append(&self, _: i64, event: Vec<u8>) -> Result<(), String> {
            self.log
                .lock()
                .unwrap()
                .push(InboundEvent::decode(event.as_slice()).unwrap());
            Ok(())
        }
    }

    fn hook(event: &str, id: &str, room: &str, identity: Option<&str>) -> Webhook {
        Webhook {
            event: event.into(),
            id: id.into(),
            room: Some(Room { name: room.into() }),
            participant: identity.map(|i| Participant { identity: i.into() }),
        }
    }

    fn change(e: &InboundEvent) -> (CallChange, Vec<i64>, i64) {
        match &e.payload {
            Some(Payload::CallEvent(c)) => (
                CallChange::try_from(c.change).unwrap(),
                c.participant_ids.clone(),
                c.duration_ms,
            ),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_call_starts_with_the_first_join_and_ends_when_the_room_does() {
        let f = Fake::default();
        let room = call_room(7);
        f.start(
            7,
            &ActiveCall {
                call_id: room.clone(),
                huddle: false,
                started_at_ms: 1_000,
                participants: vec![],
                announced: false,
            },
        )
        .await
        .unwrap();
        on_webhook(
            &f,
            &f,
            &hook("participant_joined", "e1", &room, Some("1")),
            1_100,
        )
        .await
        .unwrap();
        on_webhook(
            &f,
            &f,
            &hook("participant_joined", "e2", &room, Some("2")),
            1_200,
        )
        .await
        .unwrap();
        on_webhook(
            &f,
            &f,
            &hook("participant_left", "e3", &room, Some("1")),
            5_000,
        )
        .await
        .unwrap();
        on_webhook(&f, &f, &hook("room_finished", "e4", &room, None), 61_000)
            .await
            .unwrap();
        let log = f.log.lock().unwrap();
        let changes: Vec<_> = log.iter().map(change).collect();
        assert_eq!(
            changes,
            vec![
                (CallChange::Started, vec![1], 0),
                (CallChange::Participants, vec![1, 2], 0),
                (CallChange::Participants, vec![2], 0),
                (CallChange::Ended, vec![], 60_000),
            ]
        );
        assert_eq!(log[0].dedupe_id, "lk:e1");
        assert_eq!(log[0].actor_id, 1);
        assert!(f.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn stale_and_foreign_webhooks_change_nothing() {
        let f = Fake::default();
        // No active call for this room (ended already, or never ours).
        assert_eq!(
            on_webhook(
                &f,
                &f,
                &hook("participant_joined", "e1", &call_room(7), Some("1")),
                0
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            on_webhook(&f, &f, &hook("room_finished", "e2", &call_room(7), None), 0)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            on_webhook(
                &f,
                &f,
                &hook("participant_joined", "e3", "someone-elses-room", Some("1")),
                0
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            on_webhook(
                &f,
                &f,
                &hook("track_published", "e4", &huddle_room(7), Some("1")),
                0
            )
            .await
            .unwrap(),
            0
        );
        assert!(f.log.lock().unwrap().is_empty());
    }
}
