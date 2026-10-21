<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# xmsg-loadgen

WebSocket load generator for the gateway (Phase 0 capacity spike).

- Opens N connections with dev tokens (`dev:<user_id>`; the gateway needs `XMSG_DEV_AUTH=true`), ramped at a set rate, and pings each one.
- With `--nats`, publishes `GatewayBatch`es directly to `gw.<gateway-id>` as fan-out would (spread over `--partitions` streams, each with its own sequence, plus `StreamHeads` every second), so delivery throughput and latency can be measured without the rest of the pipeline.
- Prints one line per second (connections, published vs received frames/s, delivery p50/p99, ping RTT p99, resyncs) and a summary.

Latency conventions (loadgen-only): delivered frames carry their send time in microseconds in `MessageCreated.message_id`; pings carry microseconds in `Ping.sent_at_ms`. Run the publisher and the connections on the same machine so the clocks agree.

```bash
cargo run --release -p xmsg-loadgen -- --help
cargo run --release -p xmsg-loadgen -- --connections 10000 --ramp 2000 --duration 30 \
  --nats nats://127.0.0.1:4222 --batches-per-sec 2000 --recipients 50 --partitions 16
```

One machine can open at most ~16k connections to one gateway address over loopback (ephemeral ports); larger tests need several load-generator machines. Results so far: [docs/spikes/2026-10-21-gateway-local.md](../../docs/spikes/2026-10-21-gateway-local.md).
