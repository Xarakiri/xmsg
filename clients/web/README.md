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

Open http://localhost:5173. The Vite dev server is the browser's single origin and proxies `/auth` → auth (`:8081`), `/channels` → channels (`:8082`) and `/ws` → the gateway (`:4000`); override with `XMSG_AUTH_ORIGIN`, `XMSG_CHANNELS_ORIGIN`, `XMSG_GATEWAY_ORIGIN`.

## How it behaves

- Signing in stores the refresh token in `localStorage`; access tokens (15 min) are refreshed a minute before they expire, and a reload resumes the session.
- Messages show instantly as **sending…**, become **sent** on the gateway's ack and take their place by `msg_seq` when the sequenced event arrives. Rejections stay visible with the reason.
- Unconfirmed messages are resent when the connection comes back; the sequencer drops repeats of the same `client_msg_id`.
- Channels and messages are kept in `localStorage`, so a reload keeps the conversation.

## Not yet
- **History and sync:** only events received while connected are shown; anything sent while you were away, and channels you were added to meanwhile, appear once the read/sync API exists (next step).
- People are shown by user ID (no users directory yet). Copy your ID from the top of the sidebar to share it.
- Channel names reach members through `channel.created`; DMs are titled by the other members' IDs.
