# ADR 0012: Unread counts from sequence math, mentions as rows, one writer per user

**Status:** Accepted

## Context
Unread counts and badges must be exact, cheap to compute for users in hundreds of channels, and safe under replays and out-of-order updates from several devices.

## Decision
- **Unread = `chan_head.msg_seq − last_read_msg_seq`.** No per-message counters. New members start at the channel's `msg_seq` at join; sending a message marks the channel read up to it (as Slack does).
- **Mentions as rows:** `user_mentions(user_id, channel_id, msg_seq)` and `channel_mentions(channel_id, msg_seq)` (one row per `@channel`/`@here`, so a mention in a 500k channel is one write). Counts are rows after `last_read_msg_seq`; `last_mention_seq` fields let sync skip channels with nothing new.
- **One writer per user:** every read-position update (mark-read from any device, own sends, mark-unread, joins, mentions) goes through `readstate.events`, keyed by `user_id`. Only the owning `readstate` instance writes `read_state`, merging with max; "mark as unread" is an explicit operation in the same stream. The consumer of `messages.persisted` (partitioned by channel) never writes `read_state`; it only produces `readstate.events` (clarified in review #7).
- **Read receipts** (DMs and group DMs only) go over NATS `receipts.{partition}` through fan-out, coalesced to 1/s per user per channel; they never take an `event_seq`.

## Consequences
- No counter can be double-counted by a replay, and a late update from one device can't undo another's.
- Read-position updates take one extra Kafka hop (~10–30 ms); clients show them optimistically.

## Alternatives considered
- **Counters incremented on each message or mention:** not idempotent under replays.
- **Last-write-wins with Scylla timestamps:** breaks "mark as unread".
