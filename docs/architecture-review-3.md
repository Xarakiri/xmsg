# xmsg: Architecture Review #3

**Reviewed:** [architecture-plan.md](architecture-plan.md) after the fixes from [review #2](architecture-review-2.md) (membership in the pipeline, outbox, RocksDB sequencer state, gateway gap detection).
**Date:** 2026-10-21
**Note:** section numbers in this review refer to the plan as it was then, now in [archive/architecture-plan-v1.md](archive/architecture-plan-v1.md). The current plan was rewritten on 2026-10-21; its reasoning is in [adr/](adr/README.md).
**Status:** ✅ All recommendations below (T1–T10 and the low items) were applied to the plan on 2026-10-21. For T2 the user chose **option A: Kafka Streams (Kotlin) for the sequencer only**.

**Verdict:** **No critical findings this time.** The core ordering and durability model is sound. What's left falls into three groups:
- **Caches loaded from Scylla can be older than the stream** they're used with. This is a narrow window, but it can leak private messages.
- **The sequencer has quietly become a stream-processing framework** that we'd have to build in Rust ourselves. This is the biggest delivery risk in the plan.
- A few **load and storage costs** introduced by the last round of fixes.

Severity: 🟠 high · 🟡 medium · ⚪ low.

---

## 🟠 High

### T1. Membership snapshots from Scylla can be older than the stream position
**Where:** §4.7 (fan-out "reloaded from Scylla on the next event"), §5.3 (gateway index "built from a membership snapshot")

Fan-out and the persister both read `messages.persisted` independently, so the persister (and Scylla) can be behind fan-out. If fan-out evicts a channel and reloads its members from Scylla, it can load a list that is **missing a `member.left` fan-out already passed**. The removed member then gets the next private message. The gateway's large-channel index has the same race between "read snapshot" and "start receiving `ch.{id}` events".

**Fix: give membership a version and only use snapshots that are at least that new.**
- The sequencer stamps every event with the channel's **`membership_version`**: the `event_seq` of the latest `member.*` event.
- The persister writes `membership_version` into `channel_heads` together with the membership rows.
- Fan-out may use a Scylla snapshot only if its `membership_version` ≥ the version on the event being delivered. Otherwise it retries for a few ms (the persister is normally only milliseconds behind) and delays that channel's delivery rather than guessing.
- **Gateways:** subscribe to `ch.{id}` first and buffer, then load the snapshot with its version, then apply the buffered events newer than that version.
- **Simpler gateway snapshot:** a gateway only needs *its own* users' membership. Read each connecting user's `channels_by_user` (one partition) to learn which large channels they're in, instead of every gateway reading a 500k-member list.

### T2. The sequencer is now a custom Kafka Streams, to be written in Rust
**Where:** §4.6, §12 Phase 0–1

The sequencer now needs:
- exactly-once transactions (KIP-447),
- an embedded RocksDB state store,
- a changelog topic and restore on assignment,
- standby replicas,
- cooperative rebalancing,
- membership enforcement and a dedupe window.

On the JVM, **Kafka Streams** provides all of this, battle-tested. Rust has no mature equivalent, so we'd be writing and debugging a stream-processing framework. Most of its bugs only show up under rebalances and crashes. Meanwhile the roadmap gives Phase 1, which also includes the gateway, outbox, membership pipeline and web client, 6–8 weeks.

**Options:**
- **A (lowest risk):** write **only the sequencer** with Kafka Streams in Kotlin/Java. Every other service stays in Rust. The sequencer is small (a few thousand lines) and is the one place where a proven framework matters most.
- **B:** keep it in Rust and plan it as framework work: a dedicated engineer, **+6–8 weeks**, and a test suite that kills the process, forces rebalances and partitions the network (Jepsen-style).
- **C:** reduce state. For example, drop standby replicas in v1 and accept slower failover (restore time from the Phase 0 test). This cuts the work, but not the hardest part (EOS + state + restore).

**Recommendation:** A, unless the team strongly prefers one language. Then B with an honest timeline. Either way, re-plan the roadmap (see T6).

### T3. Gap detection misses the last batch in a quiet stream, and resync is too broad
**Where:** §4.1 delivery gaps

- A gap is only seen when the **next** batch arrives. If the last batch on a (partition, gateway) stream is dropped and that stream then goes quiet, nobody notices for a long time. That's the same quiet-channel problem review #1 found, in a new place.
- When a gap *is* found, the gateway "resyncs users in channels on that partition". But a gateway doesn't know small-channel membership, so in practice it would resync **all ~100k users**. That's 100k syncs × ~500 Redis reads ≈ **50M reads** for one dropped NATS message.
- A fan-out rebalance restarts the stream on a new owner, which looks like a gap (or hides one) unless the stream has an epoch.

**Fix:**
- **Stream heartbeats:** each fan-out instance sends every gateway a small `stream_heads` message every 1 s, with the last sequence for each of its partitions. A missing last batch is then found within ~1 s.
- **Epochs:** the stream ID is `(partition, owner_epoch)`. A new epoch resets tracking instead of being counted as a gap.
- **Partition-scoped resync:** the `resync` hint says *which partition* had the gap. The client computes `partition(channel_id)` for its own channels (it knows them all) and syncs only those, about 1/512 of its channels, usually 0–2. This needs the shared partition function from T7.

### T4. `channel_events` would double message storage for 30 days
**Where:** §6 `channel_events`, §5.2 lazy step

If `channel_events` stores every event with its full payload, `message.created` bodies are stored twice: about **27 TB raw** over the 30-day window, before replication.

**Fix:** store `message.created` (and `thread.reply`) in `channel_events` as a **pointer** (`msg_seq` / `reply_seq`) only. Full payloads only for small events (edits, reactions, pins, member changes). The lazy sync step reads pointers from `channel_events` and fetches message bodies from `messages` by `msg_seq` range. That's one extra range read, only for the open channel.

---

## 🟡 Medium

### T5. Membership-change validation races with the pipeline
**Where:** §4.4

`channels` "validates a join, leave, invite, kick or role change" by reading membership from Scylla, which lags behind the sequencer. Two admins kicking each other at the same moment, an invite from someone who was just removed, or two joins racing a member limit can all pass validation and then be applied in an order that breaks the rules.

**Fix:** treat membership changes as **commands that the sequencer validates** with its authoritative state: the inviter is a member with permission, the channel isn't archived, it's under the member limit. `channels` keeps only stateless checks (auth, input format) and returns the sequencer's result to the caller. The caller waits for the sequenced outcome, a few ms.

### T6. The roadmap no longer matches the scope
**Where:** §12

Phase 1 now contains: the Elixir gateway with gap detection and resume, outbox + Debezium, the sequencer (see T2), the persister, the membership pipeline, two-step sync and a local-first web client. That's not 6–8 weeks of work for a typical team.

**Fix:** state a team size (e.g. 2 Rust backend + 1 Elixir + 1 platform + 2 client engineers) and re-estimate. A realistic split:
- **Phase 1a:** pipeline + gateway + minimal web client. 2 users chat, nothing durable beyond Scylla.
- **Phase 1b:** outbox, membership pipeline, two-step sync, resume.

That's about **3–4 months to an MVP**.

### T7. One partition function must be used everywhere
**Where:** §4.1 typing (`hash(channel_id) % 512`), T3 resync scope

The gateway (Elixir), the clients (TypeScript) and Rust services all compute a channel's partition. They must all match **Kafka's partitioner exactly** (murmur2 of the key bytes, made positive, mod 512), or typing and partition-scoped resync go to the wrong place.

**Fix:** define the key encoding (e.g. 8-byte big-endian `channel_id`) and the function once in `proto/`. Implement it in Rust, Elixir and TypeScript, and check all three against a shared **test-vector file in CI**. Producers set the partition explicitly with this function instead of relying on each client library's default partitioner.

### T8. Sequencer membership state is large, and the plan doesn't size it
**Where:** §4.6 ("the member set, or a compact membership version for large channels")

To reject posts from non-members, the sequencer needs a per-member lookup, so a "version" alone isn't enough. That makes the sequencer state a second full copy of membership: **1–4 billion entries**, about 2–8M per partition, and a changelog of **hundreds of GB** (×3 replicas). Restore and standby catch-up times depend on this.

**Fix:** store membership as RocksDB keys `(channel_id, user_id) → role`, not as set blobs, and size it explicitly: per-partition disk, changelog size, and restore time in the Phase 0 test. Account for it in the Kafka disk budget (§1). For the first deployment, bootstrap the changelog from Scylla once, offline.

### T9. Read position can move backwards
**Where:** §4.9

Two devices can mark-read out of order (the phone sends "read up to 40" after the desktop sent "read up to 55"), and readstate also moves the sender's read position on each send. With last-write-wins, the position can go backwards and unread counts reappear.

**Fix:** merge read positions with **max**. One way without LWT: write `last_read_msg_seq` with `USING TIMESTAMP` derived from the `msg_seq`, so the higher value always wins. "Mark as unread" (a Slack feature) is a separate explicit operation that sets a lower position on purpose, using a real timestamp.

### T10. Dropping a gateway after 3 missed heartbeats is too aggressive
**Where:** §4.1, §4.7

Three missed 2-second heartbeats is 6 s. A NATS reconnect or a brief network partition makes fan-out **drop all ~100k connections** of a gateway that is still alive, and they stop getting messages. There's also no defined way for the gateway to re-announce them.

**Fix:**
- After 3 misses, mark the gateway **suspect** but keep delivering. Delivering to a dead subject only wastes NATS messages.
- Drop its entries only after ~60 s.
- When a gateway's NATS connection comes back, it republishes `session.connected` for its live connections.
- A janitor job tombstones the `conn_id`s of gateways that are really gone (by `gateway_id`), so the compacted topic doesn't keep dead entries.

---

## ⚪ Low / consistency

- **`sess:*` in Redis is now redundant.** Fan-out uses the compacted `sessions.events` topic. Drop `sess:*` (saving ~50k Redis writes/s from heartbeats) and keep only `presence:*`.
- **`message.rejected` UX:** the sender already saw "accepted" and an optimistic message. Define the client behaviour: the message turns into an error with a reason ("You're no longer a member").
- **Push needs foreground state.** "No active foreground session" requires the push service to know about sessions and foreground status. Feed it from `sessions.events` plus a client `app_state` (foreground/background) event.
- **Channel metadata relay:** define who copies outbox `channels.meta` events into `messages.in` (a small relay), and that the sequencer dedupes them by outbox event ID. CDC replays after a long outage can exceed the 15-minute window, but metadata events carry absolute values, so applying one twice is harmless.
- **Revocation latency:** going through outbox → Debezium → relay → NATS adds ~0.1–2 s. That's fine for "within seconds", but put it in the SLOs (§9) as `revocation-to-disconnect p99 < 5 s`.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| T1 | Snapshot consistency | `membership_version` on events and in `channel_heads`; gateway uses per-user snapshots |
| T2 | How to build the sequencer | Kafka Streams (Kotlin) for the sequencer only, or Rust with +6–8 weeks |
| T3 | Gap detection | 1 s stream heartbeats, owner epochs, partition-scoped resync |
| T4 | `channel_events` contents | Pointers for created messages and replies, full payloads for small events |
| T5 | Membership-change validation | Validated by the sequencer as commands |
| T6 | Roadmap | Stated team size; split Phase 1 into 1a/1b; ~3–4 months to MVP |

## What improved since review #2
- Membership changes are ordered with messages and have a single write path.
- No dual writes: the outbox covers every Postgres-backed event.
- Channel heads are durable and can only move forward.
- Sequencer state is bounded, thread counters are safe, and dedupe matches the client's retry limit.
- Gateways detect delivery loss, so there's no constant client polling.
