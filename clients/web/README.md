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
- Unconfirmed messages are resent when the connection comes back; the sequencer drops repeats of the same `client_msg_id`.
- **Unread badges** (ADR-0012) show the channel's latest message minus your read position. Your own messages and joins count as read. The open channel is marked read while the tab is visible, and the sync brings your other devices' positions (merged with max).
- Channels and messages are kept in `localStorage`, so a reload keeps the conversation.
- **Sync** ([plan §6.6](../../docs/architecture-plan.md)) runs on reconnect, on a gateway `Resync` (only the named partition's channels), when the tab comes back to the foreground, and every 30 minutes. One sync runs at a time; requests made meanwhile merge into one follow-up. Each sync lists its channels with their heads and fetches events (up to 5 pages of 200) only for channels whose head moved past what it has. History and live events are applied in any order: messages by `msg_seq`, membership only if newer than the last change seen.

## Not yet
- Older history on demand (scrolling up), and jumping to the latest page when a channel is far behind (today the first 1,000 missing events load).
- Message times are when the client received them, not when they were sent.
- People are shown by user ID (no users directory yet). Copy your ID from the top of the sidebar to share it.
- Channel names reach members through `channel.created`; DMs are titled by the other members' IDs.
