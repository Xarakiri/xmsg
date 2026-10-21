# ADR 0013: Terminate WebSocket TLS before the gateway

**Status:** Accepted

## Context
Terminating 1.5M TLS sessions inside the BEAM (`:ssl`) adds ~20–40 KB per connection and heavy CPU during reconnect handshake storms. An L7 proxy in front would double the connection count.

## Decision
- WebSocket TLS ends at the **L4 load balancer** (cloud NLB TLS listener) or **HAProxy in TCP mode** on dedicated edge nodes. The LB → gateway hop is encrypted at the network layer (WireGuard), not by Linkerd; see [0017](0017-network-layer-encryption.md), which corrects the original wording here.
- HTTP APIs keep TLS at Envoy, which also validates JWTs; WebSocket JWTs are validated by the gateway in the first frame.
- TLS session tickets on the LB; 0-RTT disabled.
- No `permessage-deflate` (~64 KB per connection); large sync responses use zstd at the application level.

## Consequences
- Gateway pods hold ~100k connections each (~20 pods for 1.5M).
- Regional edge gateways later terminate TLS locally in the same way.

## Alternatives considered
- **TLS in the BEAM:** simpler topology, much higher memory and CPU per connection.
- **Envoy as WebSocket proxy:** doubles connections.
