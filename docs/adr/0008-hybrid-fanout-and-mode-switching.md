# ADR 0008: Push/pull fan-out, switched by the sequencer

**Status:** Accepted

## Context
Pushing each message to every member's gateway works for small conversations but sends 500k deliveries per post in a large channel.

## Decision
- **Push mode** (small channels): fan-out finds online members' gateways and publishes one batch per gateway to `gw.{gateway_id}`.
- **Pull mode** (large and broadcast channels): fan-out publishes once to `ch.{channel_id}`; each gateway with interested users subscribes and delivers locally (~20 deliveries instead of 500k). Members not viewing the channel get at most one "channel updated" bump per 5 s.
- **The sequencer decides the mode** from member count with hysteresis (pull at ≥ 1,000, back to push below 800) and emits `channel.mode_changed` in channel order.
- **Switching:** for ~10 s after a switch, fan-out publishes on both paths; gateways drop duplicates by `event_seq`.
- Gateways build their pull-mode index from each connecting user's `channels_by_user` (which carries `fanout_mode`), subscribing to `ch.{id}` before reading the snapshot and applying buffered events newer than its `membership_version`.

## Consequences
- Gateways hold per-channel indexes for pull-mode channels.
- Mode switches are visible events and must be covered by chaos tests (grow and shrink across the threshold under load).

## Alternatives considered
- **Push only:** too many deliveries for large channels.
- **Pull only:** every gateway would track membership for every channel.
