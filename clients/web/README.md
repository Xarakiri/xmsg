<!-- SPDX-License-Identifier: Apache-2.0 -->
# Web client (React + Vite)

The minimal Phase 1a client ([plan §14](../../docs/architecture-plan.md)): sign up / sign in, create channels and DMs, invite and leave, and chat in real time. Protocol, connection and state live in [`clients/core`](../core), shared with the future desktop and mobile apps.

## Run it

Start the services (Small-tier stack plus auth, channels, sequencer, messages, persister, fan-out and gateway), then:

```bash
cd clients
npm install
npm run dev --workspace @xmsg/web
```

Open http://localhost:5173. The Vite dev server is the browser's single origin and proxies `/auth` → auth (`:8081`), `/channels` → channels (`:8082`), `/messages` → the messages service's reads (`:8083`), `/readstate` → read state (`:8085`) and `/ws` → the gateway (`:4000`); override with `XMSG_AUTH_ORIGIN`, `XMSG_CHANNELS_ORIGIN`, `XMSG_MESSAGES_ORIGIN`, `XMSG_GATEWAY_ORIGIN`.

## How it behaves

- Signing in stores the refresh token in `localStorage`; access tokens (15 min) are refreshed a minute before they expire, and a reload resumes the session.
- Messages show instantly as **sending…**, become **sent** on the gateway's ack and take their place by `msg_seq` when the sequenced event arrives. Rejections stay visible with the reason.
- **Outbox** ([plan §9.2](../../docs/architecture-plan.md)): unconfirmed messages are resent when the connection comes back, and the sequencer drops repeats of the same `client_msg_id`. After **10 minutes** (inside the sequencer's 15-minute dedupe window) a message stops being resent and shows **Not sent · Retry**. A retry goes out under a **new** `client_msg_id`. If the original got through after all, its event still confirms it.
- **Mute** (ADR-0016): muted channels are dimmed, with no bold or badge. The toggle is shown at once and stored through `member.settings`, and sync brings the setting to your other devices.
- **Unread badges** (ADR-0012) show the channel's latest message minus your read position. Your own messages and joins count as read. The open channel is marked read while the tab is visible, and the sync brings your other devices' positions (merged with max).
- **Local database:** channels and messages live in **IndexedDB**, so the app opens instantly with what it had, and history isn't capped by localStorage's ~5 MB. A state saved by an older version moves over on first load, and fields added since get their defaults. Where IndexedDB is unavailable (some private modes), localStorage is used instead.
- **Sync** ([plan §6.6](../../docs/architecture-plan.md)) runs on reconnect, on a gateway `Resync` (only the named partition's channels), when the tab comes back to the foreground, and every 30 minutes. One sync runs at a time; requests made meanwhile merge into one follow-up. Each sync lists its channels with their heads and fetches events (up to 5 pages of 200) only for channels whose head moved past what it has. History and live events are applied in any order: messages by `msg_seq`, membership only if newer than the last change seen.
- **Large channels** (ADR-0008, plan §6.7): the app tells the gateway which channel is on screen (none while the tab is hidden). Pull-mode channels deliver full events only for that one; the others send a "channel updated" bump at most every 5 s, which moves their unread badge, and opening such a channel fetches what it missed.
- **Roles:** the header shows members' roles (`Ada (admin)`). In a broadcast channel only posters and admins see the composer; everyone else sees a note instead (the sequencer enforces it either way).

## Not yet
- Older history on demand (scrolling up), and jumping to the latest page when a channel is far behind (today the first 1,000 missing events load).
- Message times are when the client received them, not when they were sent.
- People are shown by user ID (no users directory yet). Copy your ID from the top of the sidebar to share it.
- Channel names reach members through `channel.created`; DMs are titled by the other members' IDs.
