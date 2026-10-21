# ADR 0011: Two-step sync and a local-first client

**Status:** Accepted

## Context
After an outage, 1M clients reconnect within minutes. Fetching missed messages for every channel of every user would mean tens of millions of Scylla reads. For users, perceived speed matters more than server latency, since the network round trip (30–150 ms) is larger than all server work (~15–30 ms).

## Decision
- **Cheap step:** the client sends a sync token (its `event_seq` heads, or a server-side cursor above ~1,000 channels). The server compares it with `chan_head` in Redis and returns only which channels changed, new heads and unread/mention counts. No message bodies.
- **Lazy step:** bodies are fetched only for the open channel (and up to 5 recent ones) from `channel_events` (pointers for new messages, filled from `messages`). Everything else loads on open.
- **Head rules:** clients keep `max(local, server)` for heads and read positions; a server head below the client's means "server behind", so the client fetches events directly.
- **Local-first client:** optimistic send, instant open from SQLite/IndexedDB, prefetch, a local outbox with a 10-minute automatic retry limit, decoding off the main thread, virtualized lists.
- **Fast reconnect:** short-lived resume tokens and TLS session tickets (0-RTT off).
- Gateways apply admission control (e.g. 2k new sessions/s per pod).

## Consequences
- Mass reconnects are bounded by Redis reads, not Scylla.
- Clients carry real logic (sync engine, outbox, reordering by `msg_seq`), shared across web and mobile in a TypeScript core.

## Alternatives considered
- **Full gap fetch per channel on reconnect:** too expensive at scale.
