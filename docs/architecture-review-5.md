# xmsg: Architecture Review #5

**Reviewed:** [architecture-plan.md](architecture-plan.md) after the fixes from [review #4](architecture-review-4.md) (push/pull switching, outbox failover safety, ordered snapshot writes, sequencer Interactive Queries, mention rows).
**Date:** 2026-10-21
**Note:** section numbers in this review refer to the plan as it was then, now in [archive/architecture-plan-v1.md](archive/architecture-plan-v1.md). The current plan was rewritten on 2026-10-21; its reasoning is in [adr/](adr/README.md).
**Status:** ✅ All recommendations below (G1–G6 and the low items) were applied to the plan on 2026-10-21. Multi-workspace connections and the edit time limit were added to the plan's open questions.

**Verdict:** **No critical findings, and the infrastructure core has nothing new to fix on paper.** This pass found **product flows the plan never walks through end to end**. Each one starts or ends outside the sequenced pipeline:
- creating a channel or DM,
- removing someone from a workspace,
- reaction storms,
- read receipts,
- message ID allocation.

One of them is high because it sits on the most common action in the app: opening a new DM and typing.

As in review #4, the remaining big unknowns need numbers, not more reading. Apply this review, then start Phase 0.

Severity: 🟠 high · 🟡 medium · ⚪ low.

---

## 🟠 High

### G1. Creating a channel or DM isn't defined, and the obvious path is slow
**Where:** §4.4, §4.6 (sequencer rejects posts from non-members), §4.3 outbox

The plan describes membership *changes*, but not how a channel starts. The sequencer only accepts posts from known members of a known channel. The natural path is:

> insert the Postgres row → outbox → Debezium → `meta-relay` → `messages.in` → sequencer

That adds **~0.1–2 s of CDC lag** before the channel exists in the sequencer. A user who opens a new DM and types right away (the most common action in a messenger) gets the first message **rejected**, or the UI has to wait for no visible reason. Channel renames and similar edits have the same lag.

**Fix: a fast path with the outbox as a safety net.**
- `channels` writes the Postgres row (and `dm_pairs`) plus an outbox event `channel.created{channel_id, type, initial_members}` in one transaction. Initial members are the creator, the DM pair or the group DM members.
- Right after the commit, `channels` **also produces the same event directly to `messages.in`** (same outbox event ID). That's the fast path, a few ms.
- Debezium → `meta-relay` delivers it again later as the safety net. The sequencer **dedupes by outbox event ID**, and `channel.created` is a no-op if the channel already exists, so a late or replayed copy is harmless even outside the 15-minute dedupe window.
- The sequencer creates the channel's state and its initial memberships **atomically in one event**, so the first message can never arrive before its members exist.
- **Client:** creating a DM returns the `channel_id` immediately; the first message waits locally (optimistic UI) until `channel.created` arrives over the socket (a few ms).
- Use the same fast-path-plus-outbox pattern for other interactive metadata changes (rename, topic, archive) and for revocations.

---

## 🟡 Medium

### G2. Removing someone from a workspace isn't defined
**Where:** §4.3, §4.4

Removing a user from a workspace (or deactivating them) has to remove them from **every channel** in it, possibly hundreds, each on a different partition. That can't be one atomic event. While it's in progress, the user still receives messages in channels that haven't been processed yet.

**Fix:**
1. **Cut access first:** `users` writes the removal plus an outbox event, and also sends it fast-path to `auth.revocations`. Gateways immediately close the user's connections for that workspace, and auth stops issuing tokens for it. Delivery stops within seconds.
2. **Then clean up:** a job reads the user's `channels_by_user` for that workspace and sends one `member.kick_requested` command per channel, at a controlled rate. These are idempotent, so the job can be retried.
3. Read paths (history, search) check **workspace membership** as well as channel membership, so the user can't read anything in between.

### G3. Reaction storms put all their load on one partition
**Where:** §5.3 ("reactions are aggregated"), §4.6

Aggregation happens only at *delivery*. Every single reaction still goes through the sequencer, takes an `event_seq`, writes a `reactions` row and a `channel_events` row. A popular post in a 500k-member broadcast channel can get **hundreds of thousands of reactions in minutes, all on one partition** (one stream task). That slows every other channel on that partition, and makes every viewer's `event_seq` race ahead.

**Fix:**
- **Batch before sequencing:** `messages` combines reactions per `(channel, message, emoji)` over a **1-second window** and produces one `reactions.batch{adds, removes}` event. One `event_seq` per batch, not per reaction.
- **Per-channel rate limit** on reaction events in pull-mode channels, with the excess queued in the batch.
- For pull-mode channels, store reaction **counts** per `(message, emoji)` plus each viewer's own reaction, instead of fanning out who reacted. The full "who reacted" list loads on demand.

### G4. Read receipts have no defined route
**Where:** §4.9 ("seen by" in DMs and group DMs)

Read positions are written by `readstate` (keyed by user). To show "seen by" in a DM or group DM, the *other* members need that update, which takes a membership lookup by channel. If receipts go through `messages.in`, every read takes an `event_seq` and adds churn to the channel. The plan doesn't say which way it goes.

**Fix:** route receipts like typing. `readstate` publishes `receipt{channel_id, user_id, msg_seq}` to NATS `receipts.{partition}`. The fan-out instance that owns that partition delivers it to the other members. Only for DMs and group DMs, and coalesced to at most 1 per user per channel per second. They're ephemeral: the current positions also come back in sync (§5.2), so a dropped receipt fixes itself.

### G5. How Snowflake IDs are allocated isn't defined
**Where:** §4.6 ("Snowflake-style, time + node + counter")

Snowflake IDs need a unique node component, and Kubernetes pods come and go. With failovers, a new owner can generate IDs in the same millisecond as the old one.

**Fix:** the **sequencer** assigns `message_id = (ms timestamp, partition [9 bits], counter)`. The partition is a natural, stable "node", since only one owner writes it at a time (fenced by EOS). The last issued ID per partition is kept in the `channel_heads` store, and a new owner continues from `max(last_id + 1, now)`. That covers clock skew between machines and IDs from transactions that were cancelled (those IDs were never visible).

### G6. Push for broadcast channels with notify-all scans every member
**Where:** §4.10

For a broadcast channel where members chose "notify me about all messages", each post makes `push` scan up to 500k membership rows to find who to notify, every time.

**Fix:** keep a separate index, `push_subscribers(channel_id, user_id)`, for members with `notify_level = all` (maintained by the persister from membership and settings events). `push` scans only that, in chunks, with the per-channel token bucket already in the plan. Mention-only users are found from the mention rows instead.

---

## ⚪ Low / consistency

- **§5.1 step 5:** "`readstate` updates mention counts" is stale. It now writes `user_mentions` rows (§4.9).
- **GDPR and retention deletes** write Scylla directly. Also emit `message.deleted` events (as commands, rate-limited) so clients drop the messages from their local caches and search removes them through the normal path.
- **Multi-workspace users:** decide whether a client opens one connection per workspace or one per device. This changes the 1.3 connections-per-user estimate and the gateway sizing. Add it to the open questions.
- **Edit window:** Slack lets workspaces limit how long after sending a message can be edited. If we want that, it's a check in `messages` against the row's timestamp. Decide it with the product side.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| G1 | Channel and DM creation | `channel.created` with initial members; fast-path produce + outbox safety net; sequencer dedupes |
| G2 | Workspace removal | Revoke first, then idempotent per-channel kicks; read paths check workspace membership |
| G3 | Reaction storms | 1 s batching before sequencing; counts for pull-mode channels |
| G4 | Read receipts | NATS `receipts.{partition}` via fan-out; DMs and group DMs only |
| — | What comes next | Apply this review, then start Phase 0. Further paper reviews have little left to find. |

## What improved since review #4
- Channels can switch between push and pull delivery without losing messages.
- The outbox survives Postgres failovers.
- Membership snapshots can't be read half-updated, and a lagging persister no longer stalls delivery.
- Mention counts can't double-count after replays.
- Edit, delete and command-result flows are defined.
