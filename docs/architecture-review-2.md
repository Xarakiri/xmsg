# xmsg: Architecture Review #2

**Reviewed:** [architecture-plan.md](architecture-plan.md) after the first review's fixes, the 512-partition change and the new §10 (performance).
**Date:** 2026-10-21
**Note:** section numbers in this review refer to the plan as it was then, now in [archive/architecture-plan-v1.md](archive/architecture-plan-v1.md). The current plan was rewritten on 2026-10-21; its reasoning is in [adr/](adr/README.md).
**Previous review:** [architecture-review.md](architecture-review.md) (all findings applied)
**Status:** ✅ All recommendations below (R1–R10 and the low items) were applied to the plan on 2026-10-21, using the recommended option for each decision.

**Verdict:** The message path is now correct under failure: fenced sequencer, idempotent persister, two counters, fixed partitions. The remaining problems are mostly **at the edges of that path**: data that changes *outside* the sequenced pipeline (membership, Postgres-backed state, Redis heads) and state the sequencer now has to hold (threads, dedupe). The biggest finding is that membership changes aren't ordered with messages, so a removed member of a private channel can keep receiving its messages.

Severity: 🔴 critical (data leak, loss or corruption) · 🟠 high (will hurt at scale) · 🟡 medium · ⚪ low.

---

## 🔴 Critical

### R1. Membership changes aren't ordered with messages, and can be lost entirely
**Where:** §4.4, §4.7, §5.3, Kafka topics

**Problem:** The channels service writes membership to Scylla and *separately* publishes to `channels.membership`, a 64-partition topic with no ordering relative to `messages.persisted`. This causes three failures:
1. **Lost change:** the Scylla write succeeds and the Kafka produce fails (or the pod dies in between). Fan-out and gateway caches never learn about it, so a **removed member keeps receiving a private channel's messages** until those caches restart.
2. **Race:** even when both writes succeed, fan-out can process a message sequenced *after* the removal before it sees the membership event. Joins have the mirror problem: a new member misses the first messages after joining.
3. **Fan-out cold start:** fan-out instances own `messages.persisted` partitions (512) but membership events come keyed into 64 different partitions, so every instance must read the whole topic and filter.

**Fix:** Send membership changes **through the channel's own pipeline**. §4.5 already lists `member.joined/left` as channel events, so make that the only path:
- `channels` validates the request and produces `member.joined` / `member.left` / `member.role_changed` to `messages.in` (keyed by channel). It does **not** write Scylla directly.
- The sequencer gives the event an `event_seq`. The **persister** writes `members_by_channel` and `channels_by_user`.
- Fan-out and gateways update membership from the same `messages.persisted` stream, **in order with messages**. Nothing sequenced after a removal can reach the removed member, and a new member gets everything from their join `event_seq` onward.
- The **sequencer becomes the authoritative membership check**: it keeps each channel's member set (or a membership version) in its state and rejects posts from non-members. The `messages` service check becomes an early, best-effort filter.
- Drop `channels.membership` as a separate topic, or keep it only as a derived, compacted feed for systems outside the pipeline (search ACL cache).

Read-after-write: a client that just joined sees the channel once its own `member.joined` arrives over the socket, a few ms later.

---

## 🟠 High

### R2. Postgres-backed services publish events with dual writes
**Where:** §4.2, §4.3, §4.4 (channel metadata), §4.11

`users`, `auth` and `channels` (metadata: rename, archive, public ↔ private) write Postgres and then publish to Kafka. If the publish fails, caches, search and gateways quietly drift. For example, a channel switched to private stays `is_private = false` in search, so its messages stay searchable workspace-wide.

**Fix:** a **transactional outbox**: write the event to an `outbox` table in the same Postgres transaction, and relay it to Kafka with **Debezium** (CDC) or a small Rust relay. Channel metadata events that clients need in order (`channel.updated`) are relayed into `messages.in` so they get an `event_seq`. A public → private switch also triggers an `update_by_query` in OpenSearch for that channel's documents.

### R3. Periodic client sync is a hidden constant load
**Where:** §4.1 ("clients also run that check every few minutes"), §5.2, §10.5

1M clients × a cheap sync every ~3 minutes ≈ **5.5k syncs/s, all the time**. Each one reads `chan_head` for every channel the user is in (~500), so that's **~2.8M Redis reads per second just for polling**. That's more than all message traffic.

**Fix:** detect delivery loss **at the gateway** instead of polling from every client:
- Fan-out stamps each batch it sends to `gw.{gateway_id}` with a **per-(fan-out partition, gateway) stream sequence**. A gap means NATS dropped something, and the gateway sends `resync` only to the users in the affected batch window.
- On `ch.{channel_id}` (large channels), `event_seq` already works as the sequence, so the gateway can spot gaps per channel.
- Client polling then becomes rare (e.g. every 30 min, and on app foreground) and covers only the open channel plus channels with unread mentions.

### R4. `chan_head` in Redis can go backwards or vanish
**Where:** §4.6 persister, §5.2, §6 Redis

Sync and unread counts trust `chan_head:{id}`. Redis replicates asynchronously, so after a failover the head can **move backwards** (recent writes lost). Then sync answers "nothing changed" and clients miss updates until the next event in that channel, which in a quiet channel could be days away. If Redis is lost, nothing is left to rebuild the heads from.

**Fix:**
- Store heads durably in a Scylla **`channel_heads(channel_id, event_seq, msg_seq, last_mention_seq)`** table, written by the persister (idempotent, monotonic values from the event). Redis becomes a read-through cache over it.
- Write Redis heads with a small Lua script that only ever raises the value (`max(old, new)`), so replays and reordered writes can't lower a head.
- Clients always take `max(local head, server head)`. A server head *below* the client's head means "server is behind", which triggers a direct `channel_events` fetch, not "no change".

### R5. Sequencer state for threads and dedupe is unbounded, and thread counters can repeat C1's bug
**Where:** §4.6, §5.4

- The sequencer now assigns a per-thread `reply_seq`, but the plan only describes per-channel state. Threads build up into the billions over time, so their counters can't all stay in memory. Reading `max(reply_seq)` from Scylla on a cache miss **brings back review #1's C1 bug**: the persister may be behind, so two replies get the same `reply_seq`.
- The persister "updates the root's `reply_count`". If that's a Scylla counter increment, it's **not idempotent**, and every replay double-counts.
- Restore time grows with state size, which is paid on every rebalance.

**Fix:**
- Keep sequencer state in an **embedded RocksDB store per partition** (`rust-rocksdb`), backed by the `sequencer.state` changelog (the Kafka Streams model). Memory stays bounded, and hot channels and threads are cached.
- Keep per-thread counters keyed by `(channel_id, thread_root)` in that store, never read back from Scylla.
- The sequencer puts the **absolute** `reply_count` in the event, and the persister **sets** it.
- Run **standby replicas** (instances that keep a warm copy of other partitions' state by tailing the changelog), so a rebalance doesn't wait for a full restore.
- The Phase 0 test should measure restore time with realistic state (millions of channels and threads per partition).

---

## 🟡 Medium

### R6. Dedupe window doesn't match how long clients retry
**Where:** §4.6, §10.2

The local-first client keeps unsent messages in an outbox and retries them. If a phone is offline for an hour and then resends, and the sequencer's dedupe window is shorter, the message shows up **twice**.

**Fix:** set a fixed contract, e.g. a **15-minute dedupe window** (in the RocksDB store from R5). Clients retry automatically for up to 10 minutes. After that they show "not sent, tap to retry", and a manual retry gets a new `client_msg_id`.

### R7. Fan-out's session map goes stale when a gateway crashes, and is slow to rebuild
**Where:** §4.7, `sessions.events`

- A gateway that crashes never sends `session.disconnected`, so fan-out keeps sending to `gw.{dead}` until those users reconnect elsewhere.
- Rebuilding the map on startup means replaying a day of `sessions.events`.
- Events from different gateways for the same user can arrive in any order.

**Fix:**
- Make `sessions.events` a **compacted topic keyed by `conn_id`** (tombstone on disconnect). Replaying it gives exactly the current connections.
- Gateways send a heartbeat on NATS (`gateway.alive.{id}`) every 2 s. When a gateway misses ~3 heartbeats, fan-out drops all of its entries.
- Track entries per `conn_id` (add/remove), not per user, so events arriving in any order can't remove a live connection.

### R8. Transaction overhead grows with commit frequency × partitions
**Where:** §4.6 (commits every 5–10 ms)

Every commit writes a marker to each partition it touched, across `messages.persisted`, `sequencer.state` and the offsets topic. At peak, with most of the 512 partitions active, that's roughly **100–200k marker writes per second**, all replicated ×3. Kafka can handle it, but it's a real cost that can push up p99.

**Fix:** give the Phase 0 test a pass/fail target: commit p99 < 10 ms at 100k events/s with 512 partitions. If it misses, raise the interval to 15–20 ms on less busy partitions, or compare Redpanda's transaction performance. This is also a reason not to go above 512 partitions.

### R9. Typing indicators have no defined route
**Where:** §4.7 vs §4.8

§4.7 says fan-out consumes typing events from Kafka. §4.8 says typing is NATS-only and never persisted. Typing needs a membership lookup to reach the right users, and the only service holding membership by channel is fan-out.

**Fix:** the gateway publishes typing to NATS `typing.{partition}`, where partition = `hash(channel_id) % 512`. The fan-out instance that owns that partition delivers it with its in-memory membership. No Kafka is involved, and it's dropped if anything is slow.

### R10. Presence subscriptions churn on NATS
**Where:** §4.8

Lazy presence means a subscription per visible user per viewer. Raw, that's millions of subscribe and unsubscribe calls on NATS as people scroll.

**Fix:** gateways **refcount** presence interest: one NATS subscription to `presence.{user_id}` per gateway, however many local viewers watch that user, released after a short delay so scrolling doesn't cause churn.

---

## ⚪ Low / consistency

- **Join initialization:** set `last_read_msg_seq` = the channel's `msg_seq` at the join event, otherwise new members see the whole history as unread.
- **Edit and delete targets:** edits, deletes and reactions refer to a message by `message_id`, but `messages` is keyed by `(channel_id, bucket, msg_seq)`. Events must carry `msg_seq` too (clients have it), or add a `message_locator(message_id → channel_id, msg_seq)` table.
- **`channel_events.bucket`** isn't defined. Use `event_seq / 10000`, like `messages`.
- **§10.5 wording:** "one pipelined Redis `HMGET`" should say a **pipeline of per-key reads** across cluster slots. `HMGET` reads fields of one key, not many keys.
- **§10.1:** "membership from in-memory caches" in the `messages` service. That service isn't partition-aligned, so its check is a best-effort Redis or LRU lookup. The sequencer is the authoritative check (R1).
- **Unauthenticated sockets:** close sockets that don't send a valid auth frame within 5 s, and cap pre-auth connections per IP.
- **WebSocket command rate limits:** Envoy's rate limiting doesn't see socket frames. Add per-connection command limits in the gateway (e.g. sends/s, typing/s, sync/min).
- **GDPR and Kafka:** `messages.in` / `messages.persisted` keep message bodies for 3–7 days. State this in the deletion policy (erasure completes within 7 days), alongside OpenSearch and client caches.
- **Sync token size:** for users in thousands of channels the token grows to tens of KB. Keep a server-side sync cursor once a user is above ~1,000 channels.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| R1 | How membership changes flow | Through `messages.in` as sequenced events; persister writes membership; sequencer enforces it |
| R2 | Postgres → Kafka events | Transactional outbox + Debezium |
| R3 | Delivery-loss detection | Stream sequence numbers checked by gateways; rare client polling |
| R4 | Source of truth for channel heads | Scylla `channel_heads`, Redis as monotonic cache |
| R5 | Sequencer state store | RocksDB + changelog + standby replicas |
| R6 | Dedupe window | 15 min, with a 10-min client retry limit |

## What improved since review #1
- Ordering is now correct under rebalances (fenced transactions, changelog state, idempotent persister).
- Unread counts are cheap and no longer counted wrong because of edits, reactions or thread replies.
- Socket revocation, admission control, TLS placement and recovery targets are all defined.
- The performance section puts priorities in the right order: client and network first, then server tuning.
