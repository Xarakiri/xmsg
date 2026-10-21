# ADR 0001: Elixir gateway, Rust services, Kotlin sequencer

**Status:** Accepted

## Context
The system holds ~1.5M mostly idle WebSocket connections, runs a dozen backend services that mostly wait on databases and Kafka, and has one service (the sequencer) that needs exactly-once stream processing with local state. The user's preference is Rust for backend work.

## Decision
- **Gateway: Elixir (BEAM).** One process per connection, supervisors, a preemptive scheduler and a long record at millions of sockets (WhatsApp, Discord). TLS is terminated before it ([0013](0013-tls-at-the-load-balancer.md)).
- **Sequencer: Kotlin + Kafka Streams** ([0003](0003-kafka-streams-sequencer.md)). The only JVM service.
- **Everything else: Rust** (tokio, axum, tonic). No GC pauses, low memory per pod, strong types.
- The gateway talks to services over **NATS request/reply**, not `elixir-grpc`, which is the least proven library in the stack. Rust services talk to each other over gRPC.

## Consequences
- Three backend languages plus TypeScript clients. Shared contracts are Protobuf (Buf) generated for all four; the partition function has shared test vectors ([0004](0004-fixed-partitions-and-partition-function.md)).
- Rust costs development speed: one Cargo workspace, `sccache`, fixed conventions (tokio only, axum + tonic, `anyhow`/`thiserror`).
- Hiring needs at least one Elixir and one Kotlin/Kafka engineer (roadmap team assumption).

## Alternatives considered
- **Go for core services:** faster to write, and its GC is fine for this latency budget. Rejected on team preference; not a performance call.
- **Rust gateway:** possible, but BEAM's per-connection isolation fits better.
- **Rust sequencer:** rejected in [0003](0003-kafka-streams-sequencer.md).
