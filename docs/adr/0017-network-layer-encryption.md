# ADR 0017: Encrypt the LB → gateway hop at the network layer, keep the gateway out of the mesh

**Status:** Accepted. Corrects [0013](0013-tls-at-the-load-balancer.md), which said the hop was covered by Linkerd mTLS. Applies to the Large tier we operate; self-hosted tiers use application-level TLS instead ([0019](0019-self-hosting-and-deployment-tiers.md)).

## Context
WebSocket TLS ends at the load balancer ([0013](0013-tls-at-the-load-balancer.md)). Review #9 found two problems with how the next hop was described:
- Linkerd only encrypts traffic between **meshed pods**. A cloud load balancer is not a meshed pod, so traffic from it to the gateway was **plaintext** inside the network, contradicting the "TLS in transit" security model.
- Meshing the gateway's WebSocket port would put each of its ~100k connections per pod through the Linkerd sidecar proxy as well, roughly doubling connection memory and adding CPU and latency on the most carefully sized component.

## Decision
- **Node-to-node encryption at the network layer:** the cluster CNI is **Cilium with transparent WireGuard encryption**, so all traffic between nodes (including load balancer or edge nodes to gateway pods) is encrypted with no per-connection cost in the BEAM. On clouds that guarantee VPC-level encryption between instances, that may be used instead, as long as it covers the LB → node path.
- **Bare metal:** HAProxy runs as in-cluster edge pods on the same encrypted pod network.
- **The gateway's WebSocket port is excluded from the Linkerd mesh** (`config.linkerd.io/skip-inbound-ports`). Linkerd still covers service-to-service traffic, where connection counts are small; the gateway's outbound calls (NATS, Kafka, Redis) use TLS to those systems.

## Consequences
- "TLS in transit" holds end to end: TLS from client to LB, WireGuard from LB/edge to gateway, mTLS or TLS between services and to data stores.
- Cilium becomes part of the platform (CNI choice is fixed at cluster creation).
- WireGuard adds a small CPU cost per node for all traffic, which must be included in the Phase 0 gateway capacity test.

## Alternatives considered
- **TLS in the BEAM:** rejected in [0013](0013-tls-at-the-load-balancer.md) for memory and CPU.
- **Meshing the gateway port:** doubles connection overhead.
- **Plaintext inside the VPC:** cheapest, but breaks the stated security model.
