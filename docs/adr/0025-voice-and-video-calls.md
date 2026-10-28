# ADR 0025: Voice and video calls through LiveKit, with calls as sequenced channel events

**Status:** Accepted (2026-10-28). Phase 5 item, brought forward.

## Context
People want to talk, not only type:
- **Calls in DMs and group DMs:** the others are rung.
- **Drop-in huddles in channels:** members join and leave as they like.
- **Screen sharing** in both.

The plan names LiveKit with coturn (§14). Self-hosted installs need this to work behind ordinary firewalls and to stay optional, since calls need public UDP ports.

## Decision

### Media: LiveKit (Apache-2.0), TURN: coturn (BSD-3-Clause)
- LiveKit's SFU carries the audio and video. Clients use its JavaScript SDK and connect with a short-lived access token we sign.
- coturn relays media for clients behind UDP-blocking networks, and LiveKit announces it to clients (`rtc.turn_servers`).
- **Off by default.** Small installs enable calls with `install.sh --calls`: a compose profile that adds LiveKit and coturn, and needs UDP ports open. Medium installs turn on Helm values, or point at an existing LiveKit (and TURN).
- Recording (LiveKit Egress) is not part of this.

### The `calls` service
- **`POST /v1/channels/{id}/call`:**
  - Members only, checked against `members_by_channel`.
  - Joins the channel's active call, or starts one. A DM or group DM gets a new room per call (`c{channel}-{random}`); any other channel has one huddle room (`h{channel}`).
  - Two people starting at once get the same call (a lightweight transaction on `active_calls`).
  - It answers with the LiveKit URL and an access token for that room. The token allows camera, microphone and screen sharing (with its audio), and lasts 6 hours.
- **`POST /webhooks/livekit`:**
  - LiveKit reports joins, leaves and the end of a room. Each webhook is verified: a JWT signed with the installation's API secret, whose `sha256` claim covers the body.
  - The service updates `active_calls` and emits a `CallEvent` (STARTED, PARTICIPANTS or ENDED) to `messages.in`.
  - Its dedupe ID is the webhook's ID, so LiveKit's retries are dropped by the sequencer.

### Calls are sequenced channel events
- **Ordered like membership changes:** the sequencer orders a `CallEvent` like any channel event (an `event_seq`, no `msg_seq`). It only checks that the channel exists, because the calls service checked membership when it issued tokens.
- **Every member learns of it** through the usual delivery (push or pull), and sync replays it, so a client that was offline knows a call is on. The persister keeps it in the channel's event log.
- **Absolute participant list:** each event carries everyone who's in the call now, so a lost event heals with the next.
- **Ringing:** clients ring for a STARTED call in a DM or group DM if they aren't in it; huddles show a banner instead.

### Considered and not chosen
- **Ephemeral notices (like typing):** best effort, dropped when fanout hasn't loaded the channel's members. A missed ring is worse than a late one, and a client that reconnects wouldn't know a call is on.
- **LiveKit's embedded TURN:** fewer containers, but TURN over TLS needs the certificate inside LiveKit. coturn keeps TURN separate and replaceable.
- **Peer-to-peer WebRTC for 1:1 calls:** no server media at all, but two code paths, and no screen sharing to groups.

## Consequences
- **Two more services in a calls-enabled install:** LiveKit (about 512 MB for small calls; CPU grows with participants and video) and coturn. Open ports: UDP 7882 (LiveKit's media mux), TCP 7881 (its TCP fallback), and 3478 UDP/TCP for TURN.
- **The calls service needs** LiveKit's API key and secret (from `install.sh`) and a public LiveKit URL, which Caddy proxies at `/livekit`.
- **Short calls:** a call shorter than LiveKit's empty-room timeout still produces STARTED and ENDED.
- **Huddle history:** a huddle's room name repeats, so history shows each huddle as its own STARTED…ENDED run.
