# ADR 0006: Membership changes are commands the sequencer validates

**Status:** Accepted (collects decisions from reviews #2–#6)

## Context
Membership decides who receives private messages. Earlier designs wrote membership outside the message stream, which allowed: lost updates (Scylla written, event not published), races (a message sequenced after a removal still delivered), validation against stale data, stale snapshots in caches, and finally (review #6) a settings write recreating a deleted membership row.

## Decision
- **Commands, not writes.** `channels` sends `member.join_requested` / `invite_requested` / `kick_requested` / `role_change_requested` (and `channel.created{initial_members}`) to `messages.in`. The sequencer validates them against its `members` store (actor has permission, channel not archived, under the member limit), then emits `member.joined` / `member.left` / `member.role_changed` or a rejection. Results go to `commands.results`; every `channels` instance reads that small topic.
- **The sequencer enforces membership** on every post: posts from non-members are rejected with `message.rejected`.
- **One writer for membership tables:** the persister writes `members_by_channel` and `channels_by_user` from `member.*` events. Nothing else writes those tables.
- **Settings are separate.** Mute and notification level live in `member_settings`, with a single writer of their own (originally `channels` through its outbox; now Kafka-first per [0016](0016-member-settings-kafka-first.md)). A settings row without a membership row means nothing, and a cleanup job deletes orphans.
- **`membership_version`:** every event carries the `event_seq` of the channel's latest membership change. The persister writes membership rows first, then the version, both at `LOCAL_QUORUM`. A reader may use a Scylla snapshot only if its version is at least the event's; otherwise it asks the sequencer's Interactive Queries endpoint ([0003](0003-kafka-streams-sequencer.md)). Only if both fail does fan-out hold that channel.
- Fan-out and gateways apply `member.*` events from the same ordered stream as messages. In pull-mode channels, membership events go to gateway indexes only; viewers get a batched member count.

## Consequences
- No message sequenced after a removal can reach the removed member, and new members get everything from their join onward.
- Membership changes take a few ms longer (caller waits for `commands.results`).
- The sequencer holds a second full copy of membership (1–4B keys, ~100–400 GB changelog).

## Alternatives considered
- **`channels` writes Scylla and publishes an event:** dual write; rejected in review #2.
- **Validation in `channels` against Scylla:** races with the pipeline; rejected in review #3.
- **Settings as columns on the membership row:** two writers on one row; rejected in review #6.
