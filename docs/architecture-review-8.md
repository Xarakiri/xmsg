# xmsg: Architecture Review #8

**Reviewed:** [architecture-plan.md](architecture-plan.md) and the [ADRs](adr/README.md) after the review #7 fixes ([ADR-0016](adr/0016-member-settings-kafka-first.md), single-writer read state and thread followers, no-overwrite message keys).
**Date:** 2026-10-21
**Status:** ✅ All recommendations below (N1 and the low items) were applied on 2026-10-21. N1 became §4.3–§4.4 in the plan plus clarifications in ADR-0009 and ADR-0006.

**Verdict:** **No critical or high findings.** Every table in §7 now has exactly one writer, the review #7 fixes are consistent with the ADRs, and I found no contradictions. One medium finding is an ambiguity that would cause real problems if built as currently worded. The rest are three small consistency fixes.

**The design is ready to build.** Eight passes have taken the findings from "messages can be lost" down to wording. The next useful input is measurement: the Phase 0 spikes.

Severity: 🟡 medium · ⚪ low.

---

## 🟡 Medium

### N1. Clients must not treat `event_seq` gaps as message loss
**Where:** §4.3 (`event_seq` "used for gaps and sync"), §6.6, §6.7, [ADR-0009](adr/0009-nats-delivery-and-gap-detection.md), [ADR-0011](adr/0011-two-step-sync-and-local-first-client.md)

Several events with an `event_seq` are deliberately **not** sent to every client:
- In pull-mode channels, `member.*` events update the gateway's index, but a 500k-member channel's joins shouldn't go to every viewer.
- Members not viewing a pull-mode channel get only a batched "channel updated" bump.
- Thread replies reach non-followers only as a reply-count update.

If clients assume `event_seq` is **contiguous** and treat any jump as loss, these normal cases look like gaps. In a busy large channel, every viewer would resync continuously, which is the resync storm ADR-0009 was designed to avoid.

**Fix:** state the contract explicitly:
- For clients, `event_seq` is **monotonic, not contiguous**. They use it to order events, to build the sync token, and to ask for "events after X". A jump is never treated as loss.
- **Detecting loss is the gateway's job** (stream sequences, `stream_heads`, `event_seq` on `ch.*` subjects, which gateways do receive in full). Clients sync on `resync` hints, reconnect, foreground and the 30-minute background check.
- In pull-mode channels, `member.*` events go to gateway indexes only; viewers get a batched member-count update instead.

Add one line to ADR-0009 and §4.3; no design change.

---

## ⚪ Low / consistency

- **Where `workspace.member_removed` lives:** §6.5 says `users` publishes it (through its outbox, so it lands in `users.events`), but §5.8 rebuilds fan-out's deny list from `auth.events`. Pick one: have fan-out rebuild from `users.events`, or have `auth` (which already handles revocations) emit it. The simplest is to rebuild from `users.events`.
- **Root updates with the new message key:** `messages` is now keyed by `(msg_seq, message_id)`. Updates to a thread root (`reply_count`, `last_reply_at`) need the root's `msg_seq` as well as its `message_id`. The sequencer's `threads` store should keep the root's `msg_seq` and put it in each `thread.reply` event.
- **Who may follow a thread:** `thread.followed` should be validated by the sequencer like a post (the user must be a channel member). Say so in §5.6.

---

## Decisions needed

| # | Decision | Recommendation |
|---|---|---|
| N1 | Meaning of `event_seq` for clients | Monotonic, not contiguous; gateways detect loss; pull-mode member events go to gateways only |
| — | Next step | Apply these four small edits, then start Phase 0. Stop paper reviews until spike results exist. |

## What's now true of the design
- Each table and store has exactly one writer, and §7 says which.
- Every per-user data stream (read state, settings) goes through a user-keyed topic with one owner.
- No stored value is incremented; no message can be overwritten, even after a disaster recovery.
- Every decision has a written reason (16 ADRs) and a test in the roadmap that would show if it's wrong.
