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
- **Mentions:** clients write `<@user_id>` and `<!channel>` / `<!here>` in the body. The sequencer keeps members only, never the author, at most 50 (`mentioned_user_ids`), and sets `mentions_channel` only for admins outside broadcast channels; anyone else's `@channel` posts without the mention.
- **Edits and deletes** (`edit_message`, `delete_message`): an `event_seq` but no `msg_seq` (ADR-0005). The actor must be a member and the target `msg_seq` must exist; deleting someone else's message (`as_admin`) needs the admin role. Authorship is checked by the messages service.
- Snowflake message IDs from event time, partition and a counter.
- Explicit, fixed names for every processor, store and changelog ([`Names.kt`](src/main/kotlin/dev/xmsg/sequencer/Names.kt)).
- **Membership endpoint** (plan §5.6, [`MembershipEndpoint.kt`](src/main/kotlin/dev/xmsg/sequencer/MembershipEndpoint.kt)): `GET /v1/channels/{id}/members` on `XMSG_SEQUENCER_HTTP_PORT` (default 8090) answers a protobuf `ChannelMembers` (members and `membership_version`) straight from the stores, via Kafka Streams Interactive Queries. A channel owned by another instance gets a 307 to that instance (`application.server` = `XMSG_ADVERTISED_HOST:port`, default the hostname); 503 while rebalancing, 404 for an unknown channel. HTTP + protobuf rather than gRPC: one small read, no extra runtime. Fan-out uses it when its Cassandra snapshot is behind.

## Not yet (later in Phase 0 / Phase 1a)
- Threads, reactions, `last_mention_seq`.
- Recovery mode after Kafka disaster recovery (ADR-0014).
- Commit-latency benchmark at scale (cloud spike), and running "version N" from a previous release artifact in the upgrade test once releases exist.

## Startup check
Before starting Kafka Streams, the app refuses to run (exit code 2) if:
- `messages.in`, `messages.persisted` or `commands.results` is missing,
- the channel topics' partition count differs from `XMSG_PARTITIONS` (ADR-0021),
- the sequencer has committed offsets but any state changelog is missing, or `channel-heads` is empty (ADR-0003).

## Integration tests (real Kafka, needs Docker)
`./gradlew integrationTest` starts a single-node Kafka with Testcontainers and runs the sequencer as separate JVM processes:

| Test | What it proves |
|---|---|
| `CrashFencingIT` | Two sequencers share 4 partitions while 3,000 messages (plus retries) stream in; one is SIGKILLed repeatedly, sometimes restarted at once (fencing) and sometimes after the session timeout (rebalance and restore). Every message appears exactly once, `msg_seq` and `event_seq` have no gaps or repeats, IDs are unique. `-Dxmsg.crash.kills=N` sets the number of kills (12 by default). |
| `RestoreUpgradeIT` | A new instance on an empty state directory (a new version on a new host) restores counters, membership and the dedupe window from changelogs; changelog names are unchanged. |
| `StartupCheckIT` | Missing topics, a wrong partition count, and processed data without changelogs are all refused; the process exits with code 2. |

Process logs go to `build/it-logs/`.

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
