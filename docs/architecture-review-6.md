# xmsg: Architecture Review #6

**Reviewed:** [architecture-plan.md](architecture-plan.md) after the fixes from [review #5](architecture-review-5.md) (channel creation fast path, workspace removal, reaction batching, read receipts, Snowflake IDs, push subscribers).
**Date:** 2026-10-21
**Note:** section numbers in this review refer to the plan as it was then, now in [archive/architecture-plan-v1.md](archive/architecture-plan-v1.md). The current plan was rewritten on 2026-10-21; its reasoning is in [adr/](adr/README.md).
**Status:** ✅ All recommendations below (K1–K4 and the stale lines) were applied, and the plan was rewritten as one consistent document with the reasoning moved into ADRs (2026-10-21).

**Verdict:** **No critical findings.** This pass read the whole document end to end, looking for places where the fixes from five reviews contradict each other. It found **two high issues created by the interaction of earlier fixes**:
- Two services write the same membership row, which can bring a removed member back.
- Disaster recovery breaks the "Kafka is the source of truth" rule.

There are also two medium issues and a set of stale lines.

**The document itself is now a risk.** At ~770 lines patched by five reviews, parts of it repeat or contradict each other: the diagram is out of date, and some tables and sections still describe older designs. Engineers will build from this. After applying this review, I recommend **one clean rewrite**: a short, consistent plan, with the reasoning moved into decision records (ADRs). After that, start Phase 0.

Severity: 🟠 high · 🟡 medium · ⚪ low.

---

## 🟠 High

### K1. Settings writes can bring a removed member back
**Where:** §4.4 ("personal settings ... written directly by `channels`"), §4.6 persister (sole writer of the membership tables), §4.10 `push_subscribers`

`members_by_channel` has two writers:
- the **persister** (membership rows, from `member.*` events),
- **`channels`** (`notify_level`, `muted` on the same row).

Scylla writes are upserts. If a user changes a notification setting while they're being kicked (or a client retries an old settings call), this can happen: the persister deletes the row on `member.left`, then the late settings write **creates a new row** holding only `notify_level`. Snapshot readers (fan-out cold loads, gateway indexes, search ACLs) see a row and treat the user as a member, so **private messages leak again**. The `membership_version` check doesn't catch it, because the version is correct; the row itself is wrong.

Also, `push_subscribers` is "maintained by the persister from membership and settings events", but the persister only reads `messages.persisted`. Settings come through the outbox, so the persister never sees them.

**Fix:**
- **Separate the tables:** move personal settings into `member_settings(user_id, channel_id, notify_level, muted, ...)`, written only by `channels` through its outbox. The membership tables have **exactly one writer**, the persister.
- **Membership decides what counts:** every reader treats a user as a member only from the membership tables. Settings without a membership row are ignored, and a cleanup job deletes orphans.
- **`push_subscribers`** gets its own small consumer that reads both `channels.membership` (derived) and the settings outbox feed. At send time, `push` also checks membership, so a stale subscriber row can't cause a push.

### K2. Disaster recovery can reuse sequence numbers and overwrite messages
**Where:** §4.6 ("restored from their changelogs, **never from Scylla**"), §8 backups (MirrorMaker 2 to a DR cluster), recovery targets

The design treats Kafka as the source of truth, and the sequencer's counters live only in Kafka changelogs. In a real disaster that assumption breaks:
- **Kafka is lost and restored from the MirrorMaker 2 copy**, which is asynchronous. The changelogs are slightly behind, so the sequencer restarts with `msg_seq` values **lower than ones already written to Scylla**. The persister upserts by `(channel_id, bucket, msg_seq)`, so new messages **overwrite real ones**.
- MirrorMaker 2 doesn't preserve transactions unless configured carefully. If it copies with `read_uncommitted`, cancelled transactions become visible in the DR cluster.
- **Scylla is restored from an older snapshot** while Kafka is current: this is recoverable by replaying `messages.persisted`, but only if the snapshot is younger than the topic's 7-day retention.

**Fix:**
- **DR bootstrap for the sequencer:** after any Kafka recovery, before accepting traffic, raise every channel's counters to `max(changelog value, Scylla channel_heads value) + safety gap` (e.g. +1,000), and take the last `message_id` per partition from the same maximum. A small, idempotent Kafka Streams "recovery mode" job does this. A gap in sequence numbers is harmless; a reused one isn't.
- **Persister guard:** a `message.created` whose `msg_seq` already exists with a **different** `message_id` is never overwritten. It goes to a quarantine topic and raises an alert.
- **MirrorMaker 2** with `isolation.level = read_committed`, and the DR cluster's changelogs treated as "recover then reconcile", never as authoritative.
- **Fix the recovery-targets table:** membership now lives in Scylla plus the sequencer changelog, not in Postgres. Its RPO is the Kafka/MM2 lag, made safe by the reconciliation step above. Scylla snapshots must be taken more often than every 7 days, so Kafka can always replay the gap.
- Add a **Kafka-loss DR drill** (restore into a scratch environment, then verify no `msg_seq` reuse) to the Phase 3 chaos tests.

---

## 🟡 Medium

### K3. Workspace removal doesn't stop delivery on shared sockets
**Where:** §4.3 workspace removal, open question 7 (one socket per workspace or per device)

Step 1 "closes the user's connections for that workspace". If a device uses **one socket for several workspaces** (still an open question), closing it kicks the user out of their other workspaces too, or, if it isn't closed, keeps delivering the removed workspace's channels until the per-channel kicks finish. And after a reconnect with a token for another workspace, fan-out still sees the user as a member of the old channels.

**Fix:** fan-out keeps a small **deny list** of `(user_id, workspace_id)` pairs from `workspace.member_removed` events and drops deliveries for that workspace's channels until the kick job finishes, after which the entry expires. This works whichever way open question 7 is decided. Channels need a `workspace_id` on every event (the sequencer already knows it from `channel.created`).

### K4. Reaction batching can reorder one user's add and remove
**Where:** §4.5 reaction batching

`messages` instances aren't partitioned: they sit behind a NATS queue group, so any instance can get any request. If a user adds and then removes a reaction quickly, the two requests can land on **different instances**, each with its own 1-second window. The sequencer may then apply the remove before the add, leaving a reaction the user removed. The counts are wrong too.

**Fix:** send reaction commands to a small **`reactions.in`** topic keyed by `channel_id`, and batch them in a **`reaction-batcher`** consumer that owns partitions. One batcher per channel keeps each user's operations in order. It emits `reactions.batch` to `messages.in` every second. Reactions aren't latency-critical, so the extra Kafka hop is fine. Also, each operation carries a client-side counter per (user, message, emoji), and the sequencer ignores older operations.

---

## ⚪ Low / consistency

The document has drifted in these places:

- **§2 diagram is out of date.** Missing:
  - `meta-relay` and the `channels` fast path into `messages.in` (shown only as `member.*`),
  - the sequencer's membership endpoint used by fan-out,
  - `readstate.events` and the per-user writer,
  - NATS `typing.*` / `receipts.*` / `presence.*`,
  - push reading `sessions.events`,
  - persister → Redis `chan_head`.
- **§3 table:** "Cache / sessions / presence → Redis". Sessions moved to `sessions.events`.
- **§4.5 event list:** still lists `reaction.added/removed`. It should list `reactions.batch`, `channel.created`, `channel.mode_changed`, `message.rejected`, and the command types.
- **§4.7:** says "with the snapshot rule below", but the rule is above that sentence.
- **§4.4 vs §4.6:** §4.4 says `channels.membership` is "produced from `messages.persisted`"; §4.6 says the persister produces it after writing rows. Keep §4.6's version.
- **§6 Kafka topics:** the sequencer changelog list is missing `reaction_counts`. `reactions.in` (K4) needs adding.
- **§1 storage:** doesn't include the sequencer's RocksDB state and changelogs (~0.3–1.2 TB on Kafka plus local NVMe), which §8 does include.
- **§8 recovery targets:** "Users, workspaces, membership: RPO 0 (synchronous replica)". Membership isn't in Postgres any more (see K2).

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| K1 | Settings vs membership rows | Separate `member_settings` table; persister is the only writer of membership tables |
| K2 | Disaster recovery for sequencer state | Recovery-mode reconciliation from Scylla (max + gap); persister refuses to overwrite a different message |
| K3 | Workspace removal delivery | Fan-out deny list by `(user, workspace)` |
| K4 | Reaction ordering | Partitioned `reaction-batcher` via `reactions.in` |
| — | The document | Apply this review, then one clean rewrite: a short, consistent plan plus ADRs; then Phase 0 |

## What improved since review #5
- New channels and DMs work instantly, and a first message can't be rejected.
- Workspace removal cuts access before cleanup starts.
- Reaction storms no longer flood a partition.
- Read receipts and Snowflake IDs are defined.
- Broadcast push no longer scans every member.
