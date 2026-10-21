# ADR 0015: Reactions through a partitioned batcher

**Status:** Accepted

## Context
A popular post in a large channel can get hundreds of thousands of reactions in minutes, all on one partition. If each reaction is a channel event, it floods the sequencer task and pushes every viewer's `event_seq` forward. Batching in the `messages` service (review #5) doesn't keep one user's add and remove in order, because requests land on any instance.

## Decision
- Reaction commands go to **`reactions.in`**, keyed by `channel_id`.
- A **`reaction-batcher`** (Rust) owns those partitions, so one batcher handles each channel in order. Every second it emits one `reactions.batch{adds, removes}` per `(channel, message, emoji)` to `messages.in`.
- Each operation carries a client counter per `(user, message, emoji)`; older operations are ignored.
- The sequencer keeps `reaction_counts` and puts absolute counts in each batch event; the persister sets them.
- In pull-mode channels clients get counts plus their own reaction; "who reacted" loads on demand. Per-channel rate limits apply there.

## Consequences
- Reactions take one extra Kafka hop and up to ~1 s to appear for others; the reacting user sees theirs immediately.
- One more small service and topic.

## Alternatives considered
- **One event per reaction:** floods hot partitions.
- **Batching in `messages`:** loses per-user ordering.
