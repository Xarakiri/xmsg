<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# xmsg-chaos

Checks the Phase 1a exit criteria (plan §14) under load:

- real-time chat survives restarts;
- no `msg_seq` is lost or duplicated under rebalance chaos;
- removed members receive nothing after their removal.

## What it does

1. **Starts the stack.** It runs auth, channels, messages, persister, fan-out, two sequencer instances and the gateway from the built binaries, plus NATS when `--nats-server` gives its binary. Each runs in its own process group, so the tool can kill and restart it. Logs go to `target/chaos/<service>.log`.
2. **Sets up users and channels.** It signs up `--users` users through auth and connects them all to the gateway over WebSocket. It then creates `--channels` private channels with random members through the channels service.
3. **Sends continuously.** The first `--senders` users each send `--rate` messages per second to their channels.
   - Like the real client, an unacked send stays in an outbox. It is resent with the same `client_msg_id` after `--retry-after-ms` and after a reconnect.
   - About `--dup-rate` of the sends repeat an already-acked message, as if the ack had been lost.
4. **Injects faults while sending.** It kills one service at a time, in rotation, over `sequencer-1, fanout, persister, sequencer-2, messages`.
   - Each target alternates between SIGKILL and SIGTERM.
   - It restarts the service and waits for its ready line before the next kill.
   - With `--nats-server`, `nats` can be a target too (CI uses `nats,sequencer-1,fanout,persister,nats,sequencer-2,messages`). While NATS is down, sends fail and stay in the outbox, and live delivery stops. Afterwards the gateway resubscribes, detects the gap and sends `Resync`, and clients catch up from history.
   - A sequencer stays down for 5–15 s. That is longer than its 6 s session timeout, so its tasks move to the other instance and back. Each move is a Kafka Streams rebalance with a state restore and EOS fencing.
5. **Removes a member.** About 40% of the way in, the admin of channel 0 removes a member while everyone keeps sending. After the removed user's socket receives `member.left`, that user sends 5 more messages to the channel.
6. **Quiesces.** It stops sending and waits until:
   - every send is acked;
   - every send has an outcome in `messages.persisted`;
   - Cassandra's heads match the sequencer's output.

   It then sends one probe message per channel and waits for the same things again.

## Checks

Any violation fails the run (exit 1). The summary lists violations by kind.

- **(a) Sequencer output** (`messages.persisted`, read_committed):
  - per channel, `event_seq` is 1..E and `msg_seq` is 1..K, in partition order, with no gap and no duplicate;
  - every acked send has exactly one outcome, with the right author, channel and body;
  - nothing was sequenced that wasn't sent;
  - the only rejections are the removed member's, as `NOT_A_MEMBER`.
- **(b) Each member's view:** the live WebSocket events plus a catch-up through `GET /v1/channels/{id}/events?after=<last contiguous event_seq>` equal the sequencer's events 1..E exactly, with the same content.
  - Every live event must match the sequenced one.
  - Every live event must reach someone who was a member at that `event_seq`, or the user removed by that `member.left`.
  - A rejection must reach only its own sender.
- **(c) Store and heads:**
  - Cassandra's `messages`, `channel_events` and `channel_heads` hold exactly what the sequencer emitted.
  - Each probe gets `event_seq`/`msg_seq` = Cassandra's head + 1. This means the sequencer's state, after all the restores, agrees with Cassandra.
- **Removal:**
  - The removed user receives `member.left`, and no event of the channel after it.
  - Their later sends are rejected as `NOT_A_MEMBER`, and the rejections are delivered to them.
  - The history API answers 404.
  - Events from before the removal that a restarted fan-out delivers again are counted, not failed: the user had already received them.
- **Clean stops:** a SIGTERM must end in a clean exit, not death by the signal. Otherwise Kafka consumers never leave their group and delivery stalls until the session times out.

## Running it

Prerequisites:

- Kafka, Cassandra and PostgreSQL are running, and NATS too unless `--nats-server` is given, with the schema and topics, for example from `deploy/compose`.
- Nothing else of xmsg is running.
- The services are built:

```bash
cargo build --release -p xmsg-auth -p xmsg-channels -p xmsg-messages -p xmsg-persister -p xmsg-fanout -p xmsg-chaos
(cd services/sequencer && ./gradlew installDist)
(cd services/gateway && mix deps.get && MIX_ENV=prod mix compile)
```

Then run it:

```bash
XMSG_PARTITIONS=16 XMSG_DATABASE_URL=postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg ./target/release/xmsg-chaos
```

- The other settings default to localhost: `XMSG_KAFKA_BOOTSTRAP`, `XMSG_NATS_URL`, `XMSG_CQL_NODES`, `XMSG_AUTH_URL`, `XMSG_CHANNELS_URL`, `XMSG_MESSAGES_URL` and `XMSG_GATEWAY_URL`.
- See `--help` for the load and chaos options.
- `--seed` repeats a run's choices: channel members, kill durations and duplicates.
- CI runs a smaller version on every PR (the `chaos` job) and uploads the logs when it fails.

## Known limitations (Phase 1a)

- **SIGKILLed fan-out:** after a SIGKILL or a crash, fan-out delivers nothing for its partitions until Kafka expires the dead consumer. That takes about 45 s, librdkafka's default session timeout. Nothing is lost: the restarted instance resumes from the committed offsets, and clients dedupe the events delivered twice.
  - Graceful stops (SIGTERM) leave the group at once.
  - Static membership or a shorter session timeout would shorten the stall after a crash.
- **SIGKILLed persister:** the persister has the same 45 s stall for Cassandra writes, which are idempotent.
- **What isn't killed:** the gateway, Kafka and Cassandra are not killed. Their failures belong to the Phase 1b and Phase 3 chaos tests.
