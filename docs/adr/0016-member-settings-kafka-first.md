# ADR 0016: Member settings are written Kafka-first to ScyllaDB

**Status:** Accepted. Amends [0006](0006-membership-as-sequenced-commands.md) (settings "through the outbox") and extends [0007](0007-membership-in-scylla.md).

## Context
Review #6 split personal settings (mute, notification level) out of the membership rows into `member_settings`, written by `channels` in PostgreSQL through the outbox. Review #7 found two problems:
- The table has the same key as membership, `(user_id, channel_id)`. Muting is common, so it can reach hundreds of millions of rows, in the same single Postgres primary that [0007](0007-membership-in-scylla.md) moved membership out of because of its size.
- Settings don't need relational transactions with anything else in Postgres, so the outbox buys nothing here.

## Decision
- `channels` validates a settings change (the user is changing their own settings, values are allowed) and produces **`member.settings{user_id, channel_id, notify_level, muted, ...}`** with **absolute values** to the Kafka topic `member.settings`.
  - **Key:** `user_id:channel_id`, so log compaction keeps the latest settings per membership.
  - **Partition:** chosen explicitly by `user_id` (same murmur2 function, [0004](0004-fixed-partitions-and-partition-function.md)), so all of one user's settings land in one partition.
- A **`settings-writer`** consumer (part of `channels`, its own consumer group) owns each partition and is the **only writer** of `member_settings` in ScyllaDB. It applies one user's events in partition order, so a replay after a crash ends in the same state. If two devices change the same setting at the same moment, the change that reaches Kafka last wins, which is the behaviour users expect from a settings toggle.
- The compacted topic is itself a durable record: `member_settings` can be rebuilt from it.
- `push` keeps reading `member.settings` to maintain `push_subscribers`; it still checks membership before sending.
- A settings row without a membership row is ignored by every reader; a cleanup job removes orphans.

## Consequences
- Postgres stays small: users, workspaces, channel metadata, devices, tokens, `dm_pairs`, outbox.
- One more consumer group and one compacted topic. Settings changes take one Kafka hop (~10–20 ms) before they're stored; clients show them optimistically.
- The pattern matches read state ([0012](0012-unread-counts-and-read-state.md)): per-user data goes through a user-keyed topic with one writer per partition.

## Alternatives considered
- **Keep `member_settings` in Postgres:** simplest, but works against the reason for [0007](0007-membership-in-scylla.md).
- **`channels` writes Scylla directly and then publishes:** a dual write; `push_subscribers` could drift.
- **Settings as sequenced channel events:** ordered with membership, but adds per-user settings traffic to channel partitions and the sequencer for data that doesn't need channel ordering.
