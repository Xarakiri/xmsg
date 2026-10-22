// SPDX-License-Identifier: AGPL-3.0-only
//! Session revocations (plan §5.2, §6.4). A revocation commits with an outbox row
//! (`session.revoked`, the safety net) and is then published straight to NATS
//! `auth.revocations` (fast path, ADR-0010), so gateways close the session's sockets
//! within about a second instead of when its access tokens expire.

use std::sync::Mutex;

use chrono::{DateTime, Duration, Utc};
use tokio::sync::mpsc::UnboundedSender;
use xmsg_proto::Message;
use xmsg_proto::v1::{AuthEvent, SessionRevoked, auth_event};

pub const SUBJECT: &str = "auth.revocations";

/// Access tokens are checked with this much leeway on `exp` (the gateway's and
/// `xmsg-authn`'s), so a revoked session's tokens are refused this much longer.
pub const VERIFY_LEEWAY_SECS: i64 = 30;

/// The encoded `AuthEvent` for a revoked session.
pub fn session_revoked(
    user_id: i64,
    session_id: i64,
    now: DateTime<Utc>,
    access_ttl_secs: i64,
    reason: &str,
) -> Vec<u8> {
    let refuse_until = now + Duration::seconds(access_ttl_secs + VERIFY_LEEWAY_SECS);
    AuthEvent {
        event: Some(auth_event::Event::SessionRevoked(SessionRevoked {
            user_id,
            session_id,
            revoked_at_ms: now.timestamp_millis(),
            refuse_until_ms: refuse_until.timestamp_millis(),
            reason: reason.to_string(),
        })),
    }
    .encode_to_vec()
}

/// Encodes revocations for the store (which writes them to the outbox in its
/// transaction) and remembers them, so they can be published once committed.
pub struct Recorder<'a> {
    now: DateTime<Utc>,
    access_ttl_secs: i64,
    reason: &'a str,
    encoded: Mutex<Vec<Vec<u8>>>,
}

impl<'a> Recorder<'a> {
    pub fn new(now: DateTime<Utc>, access_ttl_secs: i64, reason: &'a str) -> Self {
        Recorder {
            now,
            access_ttl_secs,
            reason,
            encoded: Mutex::new(Vec::new()),
        }
    }

    /// The outbox payload for `(user_id, session_id)`.
    pub fn encode(&self, user_id: i64, session_id: i64) -> Vec<u8> {
        let event = session_revoked(
            user_id,
            session_id,
            self.now,
            self.access_ttl_secs,
            self.reason,
        );
        self.encoded.lock().unwrap().push(event.clone());
        event
    }

    /// After commit: hand the events to the publisher (a failure here only costs
    /// latency; the outbox copy still reaches the gateways through meta-relay).
    pub fn publish(self, to: Option<&UnboundedSender<Vec<u8>>>) {
        if let Some(tx) = to {
            for event in self.encoded.into_inner().unwrap() {
                let _ = tx.send(event);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revocations_are_refused_until_every_token_has_expired() {
        let now = DateTime::from_timestamp(1_000, 0).unwrap();
        let bytes = session_revoked(7, 9, now, 900, "logout");
        let Some(auth_event::Event::SessionRevoked(r)) =
            AuthEvent::decode(bytes.as_slice()).unwrap().event
        else {
            panic!("not a session revocation");
        };
        assert_eq!(
            (r.user_id, r.session_id, r.reason.as_str()),
            (7, 9, "logout")
        );
        assert_eq!(r.revoked_at_ms, 1_000_000);
        assert_eq!(r.refuse_until_ms, (1_000 + 900 + VERIFY_LEEWAY_SECS) * 1000);
    }

    #[test]
    fn the_recorder_publishes_exactly_what_was_written() {
        let now = Utc::now();
        let recorder = Recorder::new(now, 900, "logout");
        let written = recorder.encode(1, 2);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        recorder.publish(Some(&tx));
        assert_eq!(rx.try_recv().unwrap(), written);
        assert!(rx.try_recv().is_err());
    }
}
