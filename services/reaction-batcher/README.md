<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Reaction batcher (Rust)

Phase 2 ([plan §6.8](../../docs/architecture-plan.md), [ADR-0015](../../docs/adr/0015-reaction-batching.md)). A popular message can get thousands of reactions a second, all on one channel's partition. One sequenced event per reaction would flood that sequencer task and push every viewer's `event_seq` forward, so reactions are batched first.

## How it works
- The messages service checks a reaction (member, message exists and isn't deleted, valid emoji) and produces a `ReactionCommand` to **`reactions.in`**, keyed by channel. One batcher instance (consumer group `xmsg-reaction-batcher`) owns each partition, so it sees each channel's commands in order.
- Every second (or at 10,000 operations) it emits one **`ReactionBatch`** per (message, emoji) to `messages.in`, keyed by channel, with **at most one operation per user**: the one with the highest client counter.
- The **sequencer** applies an operation only if its counter is above the last applied one for that (user, message, emoji), and only for current members. It keeps the count absolute (stores `reactions`, `reaction-counts`) and emits **`reactions.changed`** `{count, added, removed}` with an `event_seq` and no `msg_seq`; nothing if no reaction changed. The persister sets `reaction_counts` and adds or removes `reactions` rows.
- **At least once:** offsets are committed only after every batch holding them is in `messages.in`. After a crash the same commands are batched again, and the counters make the repeats no-ops.
- **Startup check:** exits with code 2 unless `reactions.in` and `messages.in` both have `XMSG_PARTITIONS` partitions (ADR-0021).

## Not yet
- Pull-mode channels: counts plus your own reaction, with "who reacted" loaded on demand, and per-channel rate limits.
- Reaction counts for messages older than the 30-day event log (today clients learn counts from events).

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_PARTITIONS` | *(required)* | The installation's partition count |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka |
| `XMSG_REACTION_BATCHER_GROUP` | `xmsg-reaction-batcher` | Consumer group |

## Tests

```bash
cargo test -p xmsg-reaction-batcher
```

The end-to-end smoke (`tools/e2e-smoke`) checks that a non-member is refused and that an add, a remove and another user's add end at a count of 1.
