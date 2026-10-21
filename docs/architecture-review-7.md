# xmsg: Architecture Review #7

**Reviewed:** the rewritten [architecture-plan.md](architecture-plan.md) and the [ADRs](adr/README.md) (first review of the clean version).
**Date:** 2026-10-21
**Status:** ✅ All recommendations below (L1–L4 and the low items) were applied on 2026-10-21. L3 became [ADR-0016](adr/0016-member-settings-kafka-first.md); ADR-0012 and ADR-0014 were clarified or amended in place with a note.

**Verdict:** **No critical or high findings.** The rewrite is consistent: I found no contradictions between sections or between the plan and the ADRs. Because the rewrite states its rules explicitly in §2, I checked every table and service against them. **Three medium findings break rule 3 ("each table or store has one writer")**, and one is a hidden per-message cost. The rest are small details lost in the rewrite.

This is the seventh pass, and the findings are now small. **I strongly recommend starting Phase 0 after this one.** The decisions that could still change the design (commit latency, restore time, sockets per pod) can only come from the spikes.

Severity: 🟡 medium · ⚪ low.

---

## 🟡 Medium

### L1. Read positions have two writers again
**Where:** §5.10 ("Also consumes `messages.persisted` for joins, own sends and mentions"), rule 3, [ADR-0012](adr/0012-unread-counts-and-read-state.md)

The plan says each user's read position has one writer: the `readstate` instance that owns that user's partition of `readstate.events`. But `readstate` also consumes `messages.persisted`, which is partitioned **by channel**, not by user. Whichever instance handles that channel partition would then write the user's row too. Example:
1. The desktop marks read up to 60 (owner writes 60).
2. The phone's earlier send at 55 is processed from `messages.persisted` by another instance, which writes 55.
3. The badge comes back.

That's exactly the regression ADR-0012 exists to prevent.

**Fix:** the `messages.persisted` consumer **never writes `read_state`**. It turns joins, own sends and mentions into events on `readstate.events` (keyed by `user_id`), and only the owner writes. Mention rows (`user_mentions`) are idempotent inserts, so either path may write those, but `last_mention_seq` on the `read_state` row goes through the owner too.

### L2. `thread_followers` has two writers, and replays undo unfollows
**Where:** §5.4/§6.9 threads, §7.2 `thread_followers`

The persister adds followers when people reply or get mentioned. Manual follow and unfollow (in v1 of the plan, dropped from the rewrite's text but still a Slack feature) would write the same table from somewhere else. When the persister replays `messages.persisted` after a crash or rebalance, it **re-adds users who had unfollowed**.

**Fix:** make follow and unfollow **channel events** (`thread.followed` / `thread.unfollowed`, sequenced like everything else). The persister applies all follower changes in channel order and stays the only writer, so a replay ends in the same state. Restore the manual follow/unfollow line in §6.9.

### L3. `member_settings` in Postgres contradicts ADR-0007's sizing
**Where:** §5.4, §7.1 `member_settings(user_id, channel_id, ...)`, [ADR-0007](adr/0007-membership-in-scylla.md)

ADR-0007 moved membership out of Postgres because of its size: 1–4B rows with steady writes. Per-membership settings have the same key and, since muting is common, could reach **hundreds of millions of rows**, in the same single Postgres primary the plan wanted to keep small.

**Fix:** store settings **Kafka-first**, the same way as read state:
- `channels` produces `member.settings` events (absolute values) to a topic keyed by `user_id`.
- A single consumer writes `member_settings` in **Scylla**. One writer, no dual write, and no outbox needed, because the event is the record.

Record this as a new ADR that amends ADR-0006/0007 rather than editing them.

### L4. The persister guard costs a read on every message
**Where:** §5.7 guard, [ADR-0014](adr/0014-disaster-recovery-reconciliation.md)

"A `message.created` whose `msg_seq` already exists with a different `message_id` is never overwritten" means **reading before every insert** on the hottest table (an extra ~20k–100k Scylla reads per second). It's also racy unless done with lightweight transactions (LWT), which ADR-0014 rejected as too slow.

**Fix:** make overwriting impossible by key design instead of checking:
- add `message_id` to the clustering key of `messages` (`PRIMARY KEY ((channel_id, bucket), msg_seq, message_id)`) and `thread_messages`. Two different messages with the same `msg_seq` then become **two rows**, so nothing is lost and no read is needed.
- history reads that find two rows for one `msg_seq` report it to `persister.quarantine` and alert. In normal operation this never happens.

---

## ⚪ Low / lost in the rewrite

- **Gateway ping interval:** v1 said "ping every 30 s, drop after 2 missed pings". The rewrite only mentions it indirectly (§9.2). Put it back in §5.1.
- **Presence unsubscribe delay:** v1 released a presence subscription ~30 s after the last local viewer left, so scrolling doesn't churn NATS. Put it back in §5.9.
- **Deny-list source and expiry (§5.8, §6.5):** say that fan-out rebuilds the deny list from `auth.events` (7-day retention) on restart, and that the kick job emits `workspace.member_removal_completed` to expire the entry.
- **Total Kafka partitions:** with three 512-partition topics plus five 512-partition changelogs, the cluster has ~4,200 partitions (~12,600 replicas at RF 3, ~1,400–2,100 per broker on 6–9 brokers). That's fine for KRaft, but add it to §11 for broker sizing. The Phase 0 commit-latency target already includes transaction markers on the changelogs.
- **Diagram (§2):** `readstate.events` (the per-user writer path) isn't shown.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| L1 | Read-state writes | Only the per-user owner writes `read_state`; everything else becomes `readstate.events` |
| L2 | Thread followers | Follow/unfollow as sequenced channel events; persister is the only writer |
| L3 | Where settings live | Kafka-first `member.settings` → Scylla, one writer; new ADR |
| L4 | Persister guard | `message_id` in the clustering key instead of read-before-write |
| — | Next step | Apply, then start Phase 0. No more paper reviews until spike results exist. |

## What the rewrite got right
- No contradictions between sections, or between the plan and the ADRs.
- Rules stated explicitly in §2 made these findings easy to spot: three of the four are violations of rule 3.
- Every table in §7 is labelled with its writer.
- Each fix from reviews #1–#6 appears once, in one place, with its reason in an ADR.
