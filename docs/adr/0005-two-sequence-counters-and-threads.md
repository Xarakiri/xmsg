# ADR 0005: Two counters per channel, and threads in their own table

**Status:** Accepted

## Context
One sequence per channel can't serve both purposes. If edits and reactions take a number, they show up as unread messages and leave holes in history pages; if they don't, clients can't detect missed edits.

## Decision
- **`event_seq`**: increases on every channel event. Used for gap detection and sync.
- **`msg_seq`**: increases only on new top-level messages (and thread replies sent "also to channel"). Used for history pages and unread counts.
- **Threads:** replies live in `thread_messages`, keyed by `(channel_id, thread_root)`, with a per-thread `reply_seq`. A reply takes an `event_seq` but not a `msg_seq`. Followers (root author, repliers, mentioned users, manual follows) get the reply and a Threads-view unread; channel viewers get a small reply-count update.
- **Message IDs** are Snowflake-style, assigned by the sequencer: `(ms timestamp, partition [9 bits], counter)`; a new partition owner continues from `max(last_id + 1, now)`.

## Consequences
- Every event carries both counters; clients track `event_seq` per channel for gaps and `msg_seq` for position.
- Thread state (`reply_seq`, `reply_count`) lives in the sequencer's `threads` store, never read back from Scylla.

## Alternatives considered
- **Replies in the `messages` partition with a `thread_root` column:** simpler schema, but thread reads scan channel history and unread rules get tangled.
