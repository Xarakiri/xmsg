<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Fan-out (Rust)

Phase 1a ([plan §5.8](../../docs/architecture-plan.md)). Delivers every sequenced event to the gateways where the channel's members are connected, which completes the round trip: one user's message appears on the others' screens.

## How it works
- **Sessions:** reads the compacted `sessions.events` topic from the beginning at startup (the gateway publishes one entry per authenticated connection and a tombstone when it closes), then keeps following it. The map is per connection, so a user with several devices on several gateways gets the event on each.
- **Membership:** applied in channel order from the `member.*` and `channel.created` events in `messages.persisted`, so nothing sequenced after a removal reaches the removed member. A channel not in memory (after a restart) is loaded from the wide-column store **only if its `membership_version` is at least the event's** (ADR-0006); otherwise fan-out waits for the persister to catch up instead of guessing.
- **Delivery:** each event is encoded **once** as a `ServerFrame` and sent to each gateway as a `GatewayBatch` on NATS `gw.{gateway_id}`, stamped with its stream `(partition, owner_epoch)` and a consecutive sequence. A new epoch starts whenever a partition is (re)assigned. `StreamHeads` go to `stream_heads.{gateway_id}` every second, so gateways notice lost batches (ADR-0009).
- **Workspace removals** (§6.5): a deny list of (user, workspace). From the moment someone is removed from a workspace, none of its events reach them, even before the removal job has taken them out of its channels one by one.
  - **Fed by:** NATS `auth.revocations`, the users service's fast path, and `users.events`, read from the start of its 7-day retention at startup and then followed.
  - **Cleared by:** `workspace.member_removal_completed`, unless a newer removal happened since.
  - **Startup:** nothing is delivered until both `sessions.events` and `users.events` are loaded.
- **Recipients:** all members for channel events (including the sender's other devices; a removed member still gets their own `member.left`); only the actor for rejections.
- **Progress:** offsets are stored after delivery and committed every second (at-least-once; clients dedupe by `event_seq`). On first start it begins at the latest offset; clients catch up through sync.
- **Startup check:** exits with code 2 if `messages.persisted`'s partition count differs from `XMSG_PARTITIONS` (ADR-0021).

## Not yet
- Pull mode for large channels (`ch.{channel_id}`) and the 10 s dual publish on mode switches; today every channel is delivered in push mode.
- The sequencer's Interactive Queries endpoint as the second snapshot source; gateway liveness (suspect after missed heartbeats, drop after 60 s); typing and read receipts; batching several events per gateway batch; per-partition parallelism.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka |
| `XMSG_NATS_URL` | `nats://127.0.0.1:4222` | NATS |
| `XMSG_CQL_NODES` / `XMSG_CQL_KEYSPACE` | `127.0.0.1:9042` / `xmsg` | Membership snapshots |
| `XMSG_PARTITIONS` | *(required)* | This installation's partition count |
| `XMSG_FANOUT_GROUP` | `xmsg-fanout` | Consumer group |

## Tests
`cargo test -p xmsg-fanout` covers the session map, membership with the snapshot rule, recipients and stream sequencing. The whole round trip runs in [`tools/e2e-smoke`](../../tools/e2e-smoke/src/main.rs) (CI job `pipeline`).
