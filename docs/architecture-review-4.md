# xmsg: Architecture Review #4

**Reviewed:** [architecture-plan.md](architecture-plan.md) after the fixes from [review #3](architecture-review-3.md) (Kafka Streams sequencer, membership versions, gap-detection heartbeats, single-writer read state, revised roadmap).
**Date:** 2026-10-21
**Note:** section numbers in this review refer to the plan as it was then, now in [archive/architecture-plan-v1.md](archive/architecture-plan-v1.md). The current plan was rewritten on 2026-10-21; its reasoning is in [adr/](adr/README.md).
**Status:** ✅ All recommendations below (F1–F7 and the low items) were applied to the plan on 2026-10-21.

**Verdict:** **No critical findings.** Two high findings, both about **transitions** the design doesn't cover yet:
- a channel switching between push and pull delivery,
- a Postgres failover under Debezium.

The rest are medium-sized consistency details plus a few stale lines.

**The findings are getting smaller with each pass.** The remaining big risks are things a document can't settle: Kafka Streams commit latency at 5–10 ms, restore times with billions of memberships, Elixir at 100k sockets per pod, and NATS behaviour under load. **After applying this review, I recommend moving to the Phase 0 spikes** instead of doing another paper review. Their results will matter more than a fifth pass.

Severity: 🟠 high · 🟡 medium · ⚪ low.

---

## 🟠 High

### F1. Switching a channel between push and pull delivery can lose messages
**Where:** §4.7, §5.3 (push model ≤ ~1,000 members, pull model above)

Channels grow and shrink. When a channel crosses the threshold, fan-out starts publishing to `ch.{id}`, but gateways haven't subscribed to it yet, because until now they got that channel through `gw.{gateway_id}`. Messages sent during the switch go nowhere, and gap detection doesn't help: there was no stream yet to have a gap in. The same happens in reverse when a channel shrinks.

**Fix:**
- **The sequencer decides the mode** and emits an ordered `channel.mode_changed{mode, event_seq}` event, so every consumer switches at the same point in the channel's order.
- **Hysteresis:** switch to pull at 1,000 members, and back to push only below 800, so a channel near the line doesn't flap.
- **Overlap window:** for ~10 s after a switch, fan-out publishes on **both** paths. Gateways subscribe on seeing `mode_changed` (or, for push → pull, as soon as it arrives via `gw.*`) and **dedupe by `event_seq`**, which they already track per channel.
- Store the mode in `channel_heads` (and in `channels_by_user` via the persister), so gateways know which channels are pull-mode when a user connects.

### F2. A Postgres failover can silently drop outbox events
**Where:** §4.3 (Debezium reads the outbox through logical replication)

Logical replication slots live on the primary. Before PostgreSQL 17 they were **not copied to standbys**. After a CloudNativePG failover, the new primary has no slot, so Debezium either fails, or is re-pointed and **skips events committed in between**. That's exactly the silent drift the outbox was meant to prevent: a revocation that never closes sockets, or a channel turned private that stays public in search.

**Fix:**
- Run **PostgreSQL 17+ with failover slots** (`sync_replication_slots` on standbys, slot `failover = true`). CloudNativePG supports synchronizing logical slots to replicas.
- Keep outbox rows for **7 days** (deleted by a cleanup job, not on publish). Run a **reconciler** that compares the outbox with what reached Kafka (by outbox event ID) after any failover, and republishes the gaps.
- All outbox consumers are already idempotent by event ID, so republishing is safe.
- Add a failover drill to the Phase 3 chaos tests: fail over Postgres under write load, then verify that no outbox event is missing in Kafka.

---

## 🟡 Medium

### F3. The `membership_version` snapshot rule depends on how Scylla is written and read
**Where:** §4.6 persister ("in the same batch"), §4.7 snapshot rule

The persister writes membership rows and `channel_heads.membership_version` "in the same batch". Those rows sit in **different partitions** (`members_by_channel` by channel, `channels_by_user` by user, `channel_heads` by channel). A Scylla logged batch guarantees they all get applied *eventually*, but **not that a reader sees them together**. A reader can see the new version before the new rows, and then trust a stale snapshot. That's the leak T1 was meant to close. Reads at `ONE` make it worse.

**Fix:**
- **Write in order, not as one batch:** membership rows first (wait for them), then `membership_version`.
- Write and read all of these at **`LOCAL_QUORUM`**. Then a reader that sees version X also sees the rows for X.

### F4. Waiting on a lagging persister stalls fan-out, so add a second snapshot source
**Where:** §4.7 ("holds that channel's deliveries")

If the persister falls minutes behind (a Scylla incident, a bad deploy), every evicted channel's next message waits for it. Holding single channels while continuing with others also means buffering per channel and managing offsets around them.

**Fix:** let fan-out load cold-channel membership from the **sequencer itself** through **Kafka Streams Interactive Queries** (a small gRPC endpoint on the sequencer that reads its `members` store). That store is always at least as new as anything fan-out has seen. Rules:
- use Scylla when it's new enough (the common case),
- fall back to the sequencer's endpoint when Scylla is behind,
- hold the channel only when both are unavailable.

Alert on persister lag, since it's now partly on the delivery path.

### F5. `mention_count` increments aren't idempotent
**Where:** §4.9, §6 `read_state.mention_count`

Readstate increments `mention_count` from `messages.persisted`. A replay after a crash or rebalance counts the same mention twice. It's the same bug class as `reply_count` in review #2. It also has to reset correctly on read and on mark-unread.

**Fix:** stop keeping a counter. Store mentions as rows, **`user_mentions(user_id, channel_id, msg_seq)`**, written idempotently, and compute the count as rows after `last_read_msg_seq`, like `channel_mentions`. These rows are few per user, and `chan_head`-style skip logic (a per-user `last_mention_seq`) keeps sync cheap.

### F6. Edit and delete authorization isn't defined, and races the persister
**Where:** §4.5

Only the author (or an admin) may edit or delete a message, but nothing in the pipeline knows message authors. If `messages` checks Scylla, an edit sent right after the send can fail because the persister hasn't written the row yet. If it trusts the client, anyone can edit anything.

**Fix:** `messages` reads the target row at `LOCAL_QUORUM`. If it isn't there and its `msg_seq` is at or below the channel head (so it was just sent), retry for up to ~200 ms before rejecting. Admin deletes check the role in the sequencer's membership state, so `messages` sends them as commands like membership changes (§4.4).

### F7. How `channels` gets command results isn't defined
**Where:** §4.4, `commands.results` (16 partitions, keyed by request ID)

Results are keyed by request ID, but the HTTP request is waiting on one specific `channels` instance, which won't own that partition.

**Fix:** keep it simple: the topic is small (membership commands are rare), so **every `channels` instance reads the whole topic** with `read_committed` and ignores request IDs it isn't waiting for. It also needs a request timeout (e.g. 2 s) with a "pending, check back" answer for slow cases.

---

## ⚪ Low / consistency

- **§4.1 revocation:** still lists `member.removed` on `auth.revocations`. Membership removal now arrives through the pipeline (`member.left`, on `ch.{id}` for large channels; fan-out simply stops delivering for small ones). Remove it from that list.
- **§4.1:** "Redis holds session/presence state" is stale. Sessions now live in `sessions.events`, and Redis only holds presence.
- **§4.6 partition function:** librdkafka can already produce Java-compatible partitions with `partitioner = murmur2_random`. Use that config rather than setting partitions by hand, and keep the test vectors. Also add **Kotlin** to the list of implementations (it's in CI, but missing from §4.6).
- **`channels.membership` derived feed:** name who produces it (e.g. the persister, after writing the membership rows).
- **Sync and read state:** the cheap sync reads `read_state` from Scylla, which can be a few ms behind the single writer. Clients already take the max of local and server values, so note that this applies to read positions too.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| F1 | Push ↔ pull switching | Sequencer-decided `channel.mode_changed`, 1,000/800 hysteresis, 10 s dual publish |
| F2 | Outbox safety across failover | PostgreSQL 17+ failover slots + 7-day outbox retention + reconciler |
| F4 | Fallback snapshot source | Kafka Streams Interactive Queries on the sequencer |
| — | What comes next | Apply this review, then start the Phase 0 spikes instead of another paper review |

## What improved since review #3
- No home-grown stream processor: the sequencer is on Kafka Streams with EOS, standbys and static membership.
- Stale membership snapshots can be detected (`membership_version`).
- Gap detection covers quiet streams, and a gap only resyncs one partition.
- Membership-change races are decided by the sequencer.
- One partition function for every language.
- Read positions can't move backwards.
- The roadmap reflects the real scope.
