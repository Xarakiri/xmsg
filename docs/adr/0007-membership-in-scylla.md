# ADR 0007: Membership lives in ScyllaDB

**Status:** Accepted

## Context
At 10–20M users with 50–200 channels each, membership is 1–4 billion rows with steady writes. Fan-out needs two reads: "who is in this channel" and "which channels is this user in".

## Decision
- Two tables written as a pair by the persister: `members_by_channel (channel_id, user_id)` and `channels_by_user (user_id, workspace_id, channel_id)`, each a single-partition read.
- PostgreSQL keeps users, workspaces, channel metadata, devices, tokens and `dm_pairs`; it can later be sharded by `workspace_id`.

## Consequences
- No relational joins on membership; anything needing them goes through these two tables or the derived `channels.membership` feed.
- Membership's durability now depends on Scylla plus the sequencer changelog, not a synchronous Postgres replica ([0014](0014-disaster-recovery-reconciliation.md)).

## Alternatives considered
- **Single Postgres primary:** vacuum, index bloat and failover time get worse at billions of rows.
- **Citus sharded by workspace:** viable, but every query must carry `workspace_id` and the fan-out read pattern fits Scylla better.
