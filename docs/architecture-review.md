# xmsg: Architecture Review

**Reviewed:** [architecture-plan.md](architecture-plan.md) (Elixir gateway, Rust services, Kafka → sequencer → Scylla → NATS fan-out).
**Date:** 2026-10-21
**Note:** section numbers in this review refer to the plan as it was then, now in [archive/architecture-plan-v1.md](archive/architecture-plan-v1.md). The current plan was rewritten on 2026-10-21; its reasoning is in [adr/](adr/README.md).
**Status:** ✅ All recommendations below were applied to the plan on 2026-10-21, using the recommended option wherever the review offered a choice:
- C1: transactional sequencer + separate persister
- C2: 1,024 partitions, never resized
- C3: `event_seq` + `msg_seq`
- C4: separate `thread_messages` table
- H4: membership in Scylla
- M2: TLS ends at the L4 load balancer or HAProxy
- M3: ClickHouse deferred, Redpanda listed as an option
- M5: NATS request/reply from the gateway

**Verdict:** The overall shape is sound and close to how Slack and Discord work: stateless socket gateways, a durable event log, one writer per channel, and a hybrid push/pull fan-out. Most of the risk sits in the **message pipeline's correctness under failure** (§1) and in a few **data-model gaps** (threads, event vs. message sequence numbers) that will be painful to change later. None of these needs a different architecture, but several must be decided **before Phase 1 code**.

Severity:
- 🔴 **Critical:** causes data loss, duplicates or reordering, or is very costly to change later. Fix before building.
- 🟠 **High:** will hurt at scale or in production. Fix before Phase 3 load tests.
- 🟡 **Medium:** a design gap or an ops cost to plan for.
- ⚪ **Low:** cleanup or inconsistency.

---

## 🔴 Critical

### C1. Sequencer can assign duplicate `seq` during a Kafka rebalance
**Where:** §4.6

**Problem:** During a rebalance, the old owner of a partition can still be finishing its last batch (it may be stuck in a GC-free but slow Scylla write, or partitioned from Kafka) while the new owner has already read `max(seq)` from Scylla and starts numbering. Both then write `seq = N` for different messages. Scylla upserts silently, so **one message is lost**, and fan-out may already have delivered both.

**Fix:** Make Kafka the source of truth and fence old owners:
1. Run the sequencer as a **transactional consume → produce** (Kafka exactly-once semantics) from `messages.in` to `messages.persisted`. The transactional producer ID is fenced on rebalance, so an old owner's commit fails.
2. Keep the per-channel `last_seq` state in a **compacted changelog topic**, written in the same transaction (Kafka-Streams style), and restore it from there instead of from Scylla.
3. Move the Scylla write into a separate **`persister`** consumer of `messages.persisted`. It does idempotent upserts keyed by `(channel_id, bucket, seq)`, so replays are harmless.

This also removes Scylla latency from the ordering-critical path.

### C2. Kafka partition count can never safely increase
**Where:** §6 Kafka topics (`messages.in`, 256 partitions)

**Problem:** Ordering and sequencer ownership depend on `hash(channel_id) % partitions`. Adding partitions later moves channels to new partitions mid-stream. That breaks ordering and the sequencer state for every moved channel.

**Fix:** Pick the final count up front (e.g. **1,024–2,048**; Kafka handles that fine on 6–9 brokers), or route by a fixed set of **virtual shards** (`shard = hash(channel_id) % 4096`, mapped to partitions through a migration-aware table). Write down the rule "never resize these topics".

### C3. One `seq` sequence is doing two jobs
**Where:** §4.5, §4.9, §5.3

**Problem:** Edits, deletes and reactions go through the same pipeline. If they take a channel `seq`:
- Unread count `last_seq − last_read_seq` counts reactions and edits as unread messages.
- History pagination returns gaps where non-message events sit.

If they *don't* take a `seq`, clients can't detect missed edits or reactions.

**Fix:** Use two counters per channel:
- **`event_seq`**: every event (message, edit, delete, reaction, pin, membership change). Clients use it for gap detection and sync.
- **`msg_seq`**: only new top-level messages. Used for unread counts and history.

Unread count = messages with `msg_seq > last_read` that weren't written by the user. Keep the user's own messages from inflating the count: mark-read on send moves `last_read` forward.

### C4. Threads are not designed
**Where:** §4.5 mentions "thread replies", but there is no data model and no fan-out or unread rules for them.

**Problem:** Slack-style threads change the message table key, unread counts ("threads" view), notifications (thread followers) and fan-out (only followers, plus "also send to channel"). Bolting them on later means a Scylla schema migration across billions of rows.

**Fix:** Decide now:
- Do replies live in the same partition with a `thread_root`, or in a separate `thread_messages` table keyed by `(channel_id, thread_root)`? The separate table is recommended.
- Add a `thread_followers` table, plus per-user thread read state.
- Replies take the channel's `event_seq` but not its `msg_seq` (unless "also send to channel").

---

## 🟠 High

### H1. NATS core is at-most-once, so dropped messages go unnoticed until the next one
**Where:** §2, §4.1, §4.7

If a gateway briefly loses its NATS connection, or a slow subscriber gets cut off, messages are lost silently. A client only notices the gap when the *next* message in that channel arrives, which may be hours later in a quiet channel.

**Fix:**
- When a gateway reconnects to NATS (or gets a slow-consumer error), push a **`resync` hint** to all its clients.
- Clients also run a cheap periodic check (every few minutes): send the channel's `event_seq` heads and get back which ones changed. This is a few hundred bytes from Redis.
- Optionally use **JetStream** for the `gw.*` subjects for a short replay window. Measure the latency cost first.

### H2. Reconnect sync doesn't scale for users in hundreds of channels
**Where:** §4.5 Sync API, §5.2

After an outage, 1M clients reconnect. Each one sends `{channel_id: last_seq}` for many channels, and the server reads gaps from Scylla, which means **tens of millions of Scylla reads in minutes**.

**Fix:** A two-step sync:
1. **Cheap step:** the client sends one sync token. The server returns *which* channels changed plus their new heads and unread and mention counts, all from Redis (`chan_head:{id}`) and `readstate`. No message bodies.
2. **Lazy step:** message bodies are fetched only for the open channel, plus maybe the top 5 by recency. Everything else loads on demand.

Also add **admission control** in the gateway (a token bucket on new sessions per pod), so a mass reconnect doesn't overload the backend.

### H3. Long-lived sockets ignore token expiry and revocation
**Where:** §4.1, §4.2

The JWT is checked once at connect, but a socket can stay open for days. Banning a user, removing them from a workspace, or logging out a device doesn't close their open sockets or stop deliveries.

**Fix:**
- Publish `session.revoked` / `user.banned` / `member.removed` events on NATS. Gateways kill the matching connections and drop channel subscriptions.
- Gateways also re-check token expiry and ask the client to refresh in-band before the 15-minute expiry. If the client doesn't refresh, the gateway disconnects it.
- Fan-out must re-check membership (from the event-driven cache), so a removed member stops receiving messages right away, not only after a cache TTL.

### H4. `channel_members` in a single Postgres will be the first relational bottleneck
**Where:** §6 PostgreSQL

At 10–20M users with ~50–200 channels each, `channel_members` holds **1–4 billion rows**, and every join, leave and notification-settings change writes to it. A single primary with replicas can hold this for a while, but VACUUM, index bloat and failover time get worse quickly.

**Fix:** Pick the scaling path now so the schema allows it:
- **Option A:** shard Postgres by `workspace_id` (Citus) from the start. All channel and membership queries must include `workspace_id`.
- **Option B:** move membership to **Scylla** as two tables, `members_by_channel` and `channels_by_user`, written as a pair. Postgres keeps only the channel metadata.

A or B, but decide before Phase 1. Option B fits the fan-out read pattern better.

### H5. `@channel` / `@here` in large channels causes a write and push storm
**Where:** §4.9, §4.10, §5.3

One `@channel` in a 500k-member channel means 500k mention-count writes plus up to 500k push notifications.

**Fix:**
- Limit `@channel` / `@here` by role and channel size (Slack does this too), and turn them off in broadcast channels.
- Compute mention counts for broadcast-mentions **at read time**: store "channel-wide mention at `msg_seq` X" once, and the reader's badge = own mentions + channel mentions after `last_read`.
- Batch and rate-limit push sends with a token bucket per channel.

### H6. Per-message Redis lookups in push fan-out add up
**Where:** §4.7

20k msg/s × ~50 members means ~1M membership and session lookups per second at the target load, and 5× that at peak. Redis Cluster can do it, but it's the biggest single load on Redis.

**Fix:** Have `fanout` keep an **in-memory `user → gateways` map** built from a `sessions.events` stream (connect/disconnect events published by the gateways), and channel membership cached per partition. Redis stays the fallback and the source for cold starts. This also removes a network hop from the delivery path.

---

## 🟡 Medium

### M1. Scylla compaction strategy doesn't match the workload
**Where:** §6 (`TimeWindowCompactionStrategy`)

TWCS assumes immutable data that expires by TTL. Here messages are kept forever and get edited and deleted, so tombstones and updates land in old windows that are never compacted together. Reads slow down and disk grows.

**Fix:** Use **Incremental Compaction (ICS)** (the default in recent ScyllaDB releases) or **LCS** for read-heavy tables. Keep TWCS only for TTL'd tables (typing or presence logs, if they exist). Model deletes as an updated `deleted=true` flag with the body cleared, and run a periodic hard delete for GDPR.

### M2. TLS termination point is undecided
**Where:** §4.1 ("TLS at the LB or on the pod")

Terminating 1.5M TLS sessions in the BEAM (`:ssl`) costs a lot of memory per connection (~20–40 KB extra) and CPU on handshake storms. Envoy in the middle doubles the connection count.

**Fix:** Terminate TLS at the **L4 load balancer** (cloud NLB with TLS listeners) or with **HAProxy in TCP mode on dedicated nodes**, then use plain TCP plus the mesh to the gateway pods. Include this in the Phase 3 capacity model. Note: then the gateway hop is covered by in-cluster encryption only, which is fine under the TLS + at-rest model.

### M3. Too many self-managed stateful systems for a v1 team
**Where:** §3, §8

Kafka, ScyllaDB, PostgreSQL, Redis, NATS, OpenSearch, ClickHouse and object storage add up to **8 stateful systems** on Kubernetes, each with its own operator, upgrades, backups and failure modes.

**Fix:**
- Drop ClickHouse from v1 (product analytics can start in Postgres or managed SaaS).
- Use **Redpanda** instead of Kafka if the team is small (one binary, no JVM, same API).
- Define a per-component "managed fallback" now (e.g. Aiven or ScyllaDB Cloud) and an RPO/RTO for each.
- Budget for at least **1–2 platform engineers** from Phase 0.

### M4. No DR targets
**Where:** §8 (backups listed, but no targets)

**Fix:** State **RPO/RTO** per data class:
- Messages: RPO ≤ 1 min, RTO ≤ 1 h.
- Membership and users: RPO 0 (synchronous replica), RTO ≤ 15 min.

Schedule **quarterly restore drills**. A backup that has never been restored doesn't count.

### M5. Elixir ↔ Rust RPC: `elixir-grpc` is the least mature link
**Where:** §4.1

Every client command passes through it. HTTP/2 flow control, deadlines and load balancing in `elixir-grpc` are less battle-tested than in tonic.

**Fix:** Either use **NATS request/reply** for gateway → service commands (the gateway already holds a NATS connection, and queue groups give load balancing for free), or keep gRPC and add a Phase 1 spike that load-tests it at 50k RPS per pod. NATS request/reply is recommended. It also removes the need for gateway-side service discovery.

### M6. Large-channel "unread bump" needs membership data in the gateway
**Where:** §5.3

To send "channel updated" only to members who aren't viewing, the gateway must know which of its local users belong to each large channel. That means a per-gateway membership index that has to stay in sync with `channels.membership` events. The plan doesn't mention it.

**Fix:** Write this down explicitly. The gateway keeps `channel → local user pids` for large channels, filled from a membership snapshot on subscribe and updated by membership events. Throttle bumps per channel (at most 1 per 5 s).

### M7. Search ACL filter breaks down for users in many channels
**Where:** §4.11

A `terms` filter with thousands of channel IDs per query is slow, and private-channel ACLs change often.

**Fix:** Filter by `workspace_id` + `channel_id` from a **cached "searchable channels" list per user** (computed once, refreshed on membership events), with a hard cap (e.g. 1,000 most recent channels). Beyond the cap, fall back to post-filtering. Store `is_private` on documents, so public channels in a workspace can be matched with one clause instead of listing IDs.

---

## ⚪ Low / consistency

- §5.3 still says "~35 NATS deliveries" (from the Go-gateway sizing). It's now ~20 gateways.
- §3 table says Envoy validates JWTs. For WebSockets, that has to happen in the gateway (first frame), so remove the overlap or make the split clear.
- §4.4 "DM ID derived from the sorted user pair": a Snowflake ID plus a unique `(user_a, user_b)` index is safer. A derived ID leaks who is talking to whom in logs and URLs, and doesn't work for group DMs.
- §4.6 dedupe "short-lived Redis/LRU set": with C1's transactional sequencer, keep dedupe in the sequencer's state store (same transaction) instead of Redis. One less moving part.
- §1 storage: 20k msg/s × 86,400 ≈ 1.7B messages/day, so "0.5–1 TB/day" only holds at ~300–600 bytes per stored row. State the assumed row size, and add ×3 replication, the compression ratio, and OpenSearch/Kafka copies to get a real disk budget.
- No **client protocol versioning** strategy. Add a `protocol_version` field to the connect frame, plus a minimum-supported-version kill switch.
- No **multi-device ordering** of local sends. Two devices sending at once will each see their own message "first" until the `seq` arrives. Document that clients reorder by `seq` once it arrives.

---

## Decisions needed before Phase 1

| # | Decision | Recommendation |
|---|---|---|
| C1 | Sequencer correctness model | Kafka EOS sequencer + changelog topic + separate persister |
| C2 | Final partition count | 1,024 partitions for `messages.*`, never resized |
| C3 | Sequence numbers | `event_seq` + `msg_seq` per channel |
| C4 | Thread model | Separate `thread_messages` table + followers |
| H4 | Membership store | Scylla `members_by_channel` / `channels_by_user` |
| M5 | Gateway → services RPC | NATS request/reply |

## What's good and should stay
- One writer per channel through Kafka partitions: simple and correct (once fenced, see C1).
- Hybrid push/pull fan-out with a member-count threshold.
- Lazy presence. This is the right call and often missed.
- Gateways that hold sockets only, with drain-and-jitter deploys.
- Unread counts from sequence differences instead of per-message counters.
- Protobuf + Buf as the single contract across Elixir, Rust and TypeScript.
