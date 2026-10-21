# Spike: gateway capacity on a laptop (local, Phase 0)

**Date:** 2026-10-21 · **Code:** `services/gateway` (Bandit, OTP 29 / Elixir 1.20, prod build), `tools/loadgen` · **Status:** done; the real measurement is still the cloud spike (plan §14, Phase 0).

## Setup
- Apple M3 Pro (5 performance + 6 efficiency cores), 18 GB, macOS. Gateway, NATS (native `nats-server`) and the load generator all on the same machine, over loopback.
- **The machine wasn't quiet:** Kaspersky Anti-Virus (`kavd`, ~40% CPU at rest and inspecting network traffic), the window server and desktop apps were busy. Results above ~100k frames/s are limited by the environment, not necessarily the gateway.
- Gateway: `MIX_ENV=prod`, `+P 5000000 +Q 1048576`, fd limit 65,536, dev tokens. No hibernation, no kernel tuning.
- Load: `xmsg-loadgen` opens N WebSockets (`dev:<id>`), pings every 5–10 s, and publishes `GatewayBatch`es straight to `gw.local` on NATS, each delivering one event to 50 distinct connected users, spread over 16 partitions. Delivery latency = publish → frame decoded by the client.

## Results

| Scenario | Result |
|---|---|
| Gateway at rest | 128 MB RSS |
| **10,000 idle connections** | 332 MB RSS → **~20 KB per connection** (plan budget 30–60 KB, before hibernation) |
| Connect (handshake + Welcome), 10k ramp at 2k/s | p50 0.6 ms, p99 5.4 ms, no failures |
| **100k frames/s to 10k connections** | **all delivered**, delivery p50 2.6–3.2 ms, **p99 6–12 ms**, ping RTT p99 ~5 ms, gateway ~2–2.5 cores |
| 200k frames/s | saturates at ~100–160k frames/s; latency grows to seconds (queueing); gateway only ~1.6–2.3 cores, load generator mostly idle |
| 300k frames/s | same ceiling; resyncs appeared (gap detection fired), connection failures during ramp |
| Ramp at 5k connections/s | ~22% connection failures (macOS default listen backlog); fine at 2k/s |

## What changed because of it
- **Delivery worker pool:** the first version dispatched every batch from one process. Batches are now routed by partition to one worker per scheduler (`XmsgGateway.Delivery.Worker`), keeping per-stream ordering and gap tracking. On this machine the ceiling didn't move, so the next bottleneck is outside that process (most likely the environment); the cloud spike must confirm.
- **Admission control matters:** a 5k/s connection ramp overflowed the listen backlog. The plan's per-pod admission control (§5.1) and backlog tuning are needed, not optional.

## Not measured here (cloud spike)
- 100k connections per pod (macOS loopback has ~16k ephemeral ports per destination; the load generator needs several source IPs or machines).
- 250k frames/s on 8 vCPU with WireGuard on, and the effect of hibernation and kernel tuning.
- Gateway behaviour with real fan-out and sequencer upstream.

## How to reproduce
```bash
nats-server -p 4222 -m 8222 &
cd services/gateway && MIX_ENV=prod XMSG_DEV_AUTH=true XMSG_GATEWAY_ID=local \
  elixir --erl "+P 5000000 +Q 1048576" -S mix run --no-halt &
cargo run --release -p xmsg-loadgen -- --connections 10000 --ramp 2000 --duration 30 \
  --nats nats://127.0.0.1:4222 --batches-per-sec 2000 --recipients 50 --partitions 16
```
