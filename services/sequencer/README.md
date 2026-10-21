<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Sequencer (Kotlin + Kafka Streams)

Phase 0 skeleton of the sequencer ([plan §5.6](../../docs/architecture-plan.md), [ADR-0002](../../docs/adr/0002-kafka-single-writer-sequencer.md), [ADR-0003](../../docs/adr/0003-kafka-streams-sequencer.md)).

Reads `messages.in`, writes `messages.persisted` and `commands.results`, with `exactly_once_v2`.

## What it does today
- Per-channel `event_seq` and `msg_seq`; rejections take no `event_seq`.
- Channel creation with initial members; duplicate `channel.created` is a no-op.
- Membership enforcement on posts; membership commands (join, invite, leave, kick) validated against its own state; `membership_version` on every event.
- Dedupe of `dedupe_id` within 15 minutes of **event time**.
- Push/pull fan-out mode with hysteresis and an ordered `channel_mode_changed` event.
- Snowflake message IDs from event time, partition and a counter.
- Explicit, fixed names for every processor, store and changelog ([`Names.kt`](src/main/kotlin/dev/xmsg/sequencer/Names.kt)).

## Not yet (later in Phase 0 / Phase 1a)
- Threads, reactions, admin deletes, `last_mention_seq`.
- Interactive Queries membership endpoint.
- Recovery mode after Kafka disaster recovery (ADR-0014).
- Integration tests against real Kafka: crash/fencing test, upgrade test (version N → N+1), commit-latency benchmark.

## Run tests

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
./gradlew test
```

## Run against the Compose stack

```bash
cd ../../deploy/compose && docker compose up -d --wait kafka && docker compose up kafka-init
cd ../../services/sequencer && XMSG_KAFKA_BOOTSTRAP=localhost:9092 ./gradlew run
```
