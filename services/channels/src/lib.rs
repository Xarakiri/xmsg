// SPDX-License-Identifier: AGPL-3.0-only
//! Channels service (plan §5.4): creates channels and DMs, and turns membership
//! actions into commands for the sequencer.
//!
//! - **Create** (§6.2): the Postgres row and an outbox `channel.created` commit in one
//!   transaction, then the same event goes straight to `messages.in` (fast path,
//!   ADR-0010). The sequencer dedupes the two copies.
//! - **Membership** (§6.3, ADR-0006): this service never writes membership. It sends
//!   `member.join_requested` / `member.kick_requested` and waits up to 2 s for the
//!   sequencer's verdict on `commands.results`.

pub mod api;
pub mod commands;
pub mod config;
pub mod kafka;
pub mod store;

use std::time::{SystemTime, UNIX_EPOCH};

use xmsg_proto::v1::{ChannelCreated, ChannelType, InboundEvent, inbound_event};

/// Time-ordered 63-bit IDs, the same layout as the auth service's: milliseconds since
/// 2026-01-01 above 22 random bits.
pub fn new_id() -> i64 {
    const EPOCH_MS: u64 = 1_767_225_600_000;
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut random = [0u8; 4];
    getrandom::fill(&mut random).expect("OS random");
    let low = u32::from_le_bytes(random) as u64 & ((1 << 22) - 1);
    (((ms - EPOCH_MS) << 22) | low) as i64
}

/// Random hex, for command request IDs.
pub fn request_id() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("OS random");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The `channel.created` event committed to the outbox and sent on the fast path.
/// Its dedupe ID is derived from the channel, so both copies (and any retry) collapse
/// into one creation in the sequencer.
pub fn channel_created(
    channel_id: i64,
    workspace_id: i64,
    creator: i64,
    channel_type: ChannelType,
    name: &str,
    members: &[i64],
) -> InboundEvent {
    InboundEvent {
        channel_id,
        workspace_id,
        actor_id: creator,
        dedupe_id: format!("create:{channel_id}"),
        payload: Some(inbound_event::Payload::ChannelCreated(ChannelCreated {
            r#type: channel_type as i32,
            initial_member_ids: members.to_vec(),
            name: name.to_string(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creation_events_dedupe_by_channel() {
        let a = channel_created(5, 1, 7, ChannelType::Private, "x", &[8]);
        let b = channel_created(5, 1, 7, ChannelType::Private, "x", &[8]);
        assert_eq!(a, b);
        assert_eq!(a.dedupe_id, "create:5");
    }

    #[test]
    fn request_ids_are_unique() {
        assert_ne!(request_id(), request_id());
        assert_eq!(request_id().len(), 32);
    }
}
