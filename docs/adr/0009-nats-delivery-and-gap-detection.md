# ADR 0009: NATS for delivery, with gap detection at the gateway

**Status:** Accepted

## Context
Delivery to gateways needs sub-millisecond pub/sub across millions of subjects. NATS core is at-most-once, so messages can be dropped silently. Having every client poll for missed messages costs ~2.8M Redis reads per second at 1M users.

## Decision
- **NATS core** carries deliveries (`gw.*`, `ch.*`), typing (`typing.{partition}`), read receipts (`receipts.{partition}`), presence (`presence.{user_id}`, refcounted per gateway) and gateway → service requests.
- **Gap detection at the gateway:**
  - Each fan-out batch carries a stream ID `(partition, owner_epoch)` and a sequence; a new epoch resets tracking.
  - Fan-out sends `stream_heads` every 1 s, so a lost last batch on a quiet stream is found within ~1 s.
  - On `ch.*` subjects the channel's `event_seq` is the sequence.
  - On a gap the gateway sends `resync{partition}`; clients sync only their channels in that partition (usually 0–2).
- **Gateway liveness:** gateways heartbeat every 2 s; fan-out marks a silent gateway suspect after 3 misses and drops its sessions only after ~60 s. Gateways re-announce their sessions after a NATS reconnect.
- Clients still run a rare background check (every 30 min) and a sync on reconnect and foreground.
- **Clients never infer loss from `event_seq`** (clarified in review #8). Some events are deliberately not sent to every client (pull-mode membership changes, bumps for non-viewers, thread replies for non-followers), so for clients `event_seq` is monotonic, not contiguous. Gateways receive every event of the channels they serve and are the only place gaps are detected.

## Consequences
- Delivery is fast and cheap; correctness comes from sequence numbers plus sync, not from the transport.
- Typing and receipts are best-effort by design.

## Alternatives considered
- **NATS JetStream for delivery:** replay is nice, but adds latency and storage on the hottest path.
- **Redis Pub/Sub:** weaker at this subject count.
- **Client polling only:** too expensive.
