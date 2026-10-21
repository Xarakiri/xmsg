// SPDX-License-Identifier: AGPL-3.0-only
//! Messages service (plan §5.5): receives sends from the gateway over NATS
//! request/reply, makes early, best-effort checks, and appends accepted
//! events to `messages.in`, keyed by channel. The sequencer makes the
//! authoritative checks (membership, ordering) later (ADR-0006).

pub mod cassandra;
pub mod config;
pub mod history;
pub mod kafka;

use xmsg_proto::Message;
use xmsg_proto::v1::{InboundEvent, RejectReason, SendResult, inbound_event::Payload};

/// NATS subject the gateway sends to, and the queue group shared by all instances.
pub const SEND_SUBJECT: &str = "xmsg.svc.messages.send";
pub const QUEUE_GROUP: &str = "xmsg-messages";

/// Slack's limit; also keeps one event well under Kafka's default message size.
pub const MAX_BODY_CHARS: usize = 40_000;
pub const MAX_DEDUPE_ID_LEN: usize = 128;

/// Where accepted events go: `messages.in` in production, a fake in tests.
pub trait EventLog {
    /// Durably appends `event` keyed by its channel. Returns once it's acknowledged.
    fn append(
        &self,
        channel_id: i64,
        event: Vec<u8>,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Early checks. Anything that needs channel state is left to the sequencer.
pub fn validate(event: &InboundEvent) -> Result<(), (RejectReason, String)> {
    let invalid = |detail: &str| Err((RejectReason::Invalid, detail.to_string()));
    let Some(Payload::SendMessage(send)) = &event.payload else {
        return invalid("only send_message is accepted on this subject");
    };
    if event.channel_id <= 0 {
        return invalid("channel_id must be positive");
    }
    if event.actor_id <= 0 {
        return invalid("actor_id must be positive");
    }
    if event.dedupe_id.is_empty() || event.dedupe_id.len() > MAX_DEDUPE_ID_LEN {
        return invalid("client_msg_id must be 1-128 bytes");
    }
    if !event.request_id.is_empty() {
        return invalid("sends are not commands; request_id must be empty");
    }
    if send.body.trim().is_empty() {
        return invalid("message body is empty");
    }
    if send.body.chars().count() > MAX_BODY_CHARS {
        return invalid("message body is longer than 40,000 characters");
    }
    Ok(())
}

/// Handles one encoded request from the gateway and returns the encoded reply.
pub async fn handle(log: &impl EventLog, request: &[u8]) -> Vec<u8> {
    let result = match InboundEvent::decode(request) {
        Err(_) => rejected(RejectReason::Invalid, "malformed InboundEvent".into()),
        Ok(event) => match validate(&event) {
            Err((reason, detail)) => rejected(reason, detail),
            Ok(()) => match log.append(event.channel_id, event.encode_to_vec()).await {
                Ok(()) => SendResult {
                    accepted: true,
                    ..Default::default()
                },
                Err(error) => {
                    tracing::warn!(%error, channel_id = event.channel_id, "append to messages.in failed");
                    rejected(
                        RejectReason::Unavailable,
                        "temporarily unavailable, retry".into(),
                    )
                }
            },
        },
    };
    result.encode_to_vec()
}

fn rejected(reason: RejectReason, detail: String) -> SendResult {
    SendResult {
        accepted: false,
        reason: reason as i32,
        detail,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use xmsg_proto::v1::{ChannelCreated, SendMessage};

    #[derive(Default)]
    struct FakeLog {
        appended: Mutex<Vec<(i64, InboundEvent)>>,
        fail: bool,
    }

    impl EventLog for FakeLog {
        async fn append(&self, channel_id: i64, event: Vec<u8>) -> Result<(), String> {
            if self.fail {
                return Err("broker down".into());
            }
            self.appended
                .lock()
                .unwrap()
                .push((channel_id, InboundEvent::decode(event.as_slice()).unwrap()));
            Ok(())
        }
    }

    fn send(body: &str) -> InboundEvent {
        InboundEvent {
            channel_id: 100,
            actor_id: 7,
            dedupe_id: "c-1".into(),
            payload: Some(Payload::SendMessage(SendMessage { body: body.into() })),
            ..Default::default()
        }
    }

    fn reply(bytes: Vec<u8>) -> SendResult {
        SendResult::decode(bytes.as_slice()).unwrap()
    }

    #[tokio::test]
    async fn valid_send_is_appended_keyed_by_channel() {
        let log = FakeLog::default();
        let result = reply(handle(&log, &send("hello").encode_to_vec()).await);
        assert!(result.accepted);
        let appended = log.appended.lock().unwrap();
        assert_eq!(appended.len(), 1);
        assert_eq!(appended[0].0, 100);
        assert_eq!(appended[0].1, send("hello"));
    }

    #[tokio::test]
    async fn invalid_sends_are_rejected_and_not_appended() {
        let log = FakeLog::default();
        let mut no_channel = send("x");
        no_channel.channel_id = 0;
        let mut no_actor = send("x");
        no_actor.actor_id = 0;
        let mut no_dedupe = send("x");
        no_dedupe.dedupe_id.clear();
        let mut with_request = send("x");
        with_request.request_id = "r".into();
        let not_a_send = InboundEvent {
            payload: Some(Payload::ChannelCreated(ChannelCreated::default())),
            ..send("x")
        };
        let too_long = send(&"x".repeat(MAX_BODY_CHARS + 1));
        let cases = [
            no_channel,
            no_actor,
            no_dedupe,
            with_request,
            not_a_send,
            send("   "),
            too_long,
        ];

        for case in cases {
            let result = reply(handle(&log, &case.encode_to_vec()).await);
            assert!(!result.accepted, "{case:?} should be rejected");
            assert_eq!(result.reason, RejectReason::Invalid as i32);
        }
        assert_eq!(
            handle(&log, b"\xff\xff").await,
            rejected(RejectReason::Invalid, "malformed InboundEvent".into()).encode_to_vec()
        );
        assert!(log.appended.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn the_longest_allowed_body_is_accepted() {
        let log = FakeLog::default();
        let body = "é".repeat(MAX_BODY_CHARS); // characters, not bytes
        assert!(reply(handle(&log, &send(&body).encode_to_vec()).await).accepted);
    }

    #[tokio::test]
    async fn broker_failures_become_retryable_unavailable() {
        let log = FakeLog {
            fail: true,
            ..Default::default()
        };
        let result = reply(handle(&log, &send("hello").encode_to_vec()).await);
        assert!(!result.accepted);
        assert_eq!(result.reason, RejectReason::Unavailable as i32);
    }
}
