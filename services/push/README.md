<!-- SPDX-License-Identifier: AGPL-3.0-only -->
# Push (Rust)

Phase 2 ([plan §5.11](../../docs/architecture-plan.md)). Notifications for new messages when you're not in the app. **Web Push first** (browser notifications, VAPID + RFC 8291 encryption); APNs/FCM and the push relay come with the mobile app.

## How it works
- **Subscriptions** (bearer token, as everywhere):

| Method | Path | Body | Reply |
|---|---|---|---|
| GET | `/v1/push/vapid-public-key` | | `{key}`: the `applicationServerKey` browsers subscribe with |
| POST | `/v1/push/subscriptions` | the browser's `PushSubscription` JSON `{endpoint, keys: {p256dh, auth}}` | 201; the endpoint must be `https://` |
| DELETE | `/v1/push/subscriptions` | `{endpoint}` | 204 |

  Stored in Postgres `push_endpoints` (`deploy/compose/init/postgres/002_push.sql`), keyed by endpoint, so a browser that signs in as someone else moves to them.
- **Who is told** (consumer group `xmsg-push` on `messages.persisted`, from the latest offset, so a new installation never pushes old history), for each new message:
  - every member but the author, from `members_by_channel`;
  - **not** anyone online and active right now (presence `online` in Valkey, `presence:last:{user}`; without `XMSG_VALKEY_URL` nobody counts as online);
  - by their channel setting (`member_settings`): **muted** or **nothing** → no push; **mentions** → only when mentioned or `@channel`; **all** → every message;
  - in channels over 1,000 members, mentions only; thread replies push only to the people they mention.
- **Sending:** the payload `{title, body (first 140 chars), channel_id, msg_seq, tag}` is encrypted per subscription (`aes128gcm`) and VAPID-signed; `tag` is the message ID, so a repeat (at-least-once) replaces rather than duplicates the notification. A push service answering 404 / 410 means the subscription is gone, and it's deleted.
- **Web app:** "Enable notifications" registers `/sw.js` (shows the notification, opens the channel on click), asks for permission and subscribes.

## Not yet
- APNs / FCM and the push relay (needs Apple and Firebase credentials), `push_subscribers` built from `channels.membership` (plan §5.11), followers for thread replies, quiet hours, per-device preferences.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `XMSG_PUSH_VAPID_PRIVATE_KEY` | *(required)* | P-256 private key, raw 32 bytes, base64url (e.g. `openssl ec -in key.pem -outform DER \| tail -c +8 \| head -c 32 \| base64 \| tr '/+' '_-' \| tr -d '='`) |
| `XMSG_PUSH_SUBJECT` | `mailto:admin@localhost` | VAPID contact (`sub` claim) |
| `XMSG_AUTH_JWKS_URL` | *(required)* | Auth service JWKS |
| `XMSG_DATABASE_URL` | `postgres://xmsg:xmsg-dev-only@127.0.0.1:5432/xmsg` | PostgreSQL |
| `XMSG_CQL_NODES` / `XMSG_CQL_KEYSPACE` | `127.0.0.1:9042` / `xmsg` | Members and settings |
| `XMSG_VALKEY_URL` | *(none)* | Presence; without it nobody is skipped as online |
| `XMSG_KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Kafka |
| `XMSG_PUSH_PORT` | `8086` | HTTP port |
| `XMSG_PUSH_ALLOW_HTTP` | `false` | Accept `http://` endpoints (tests only) |

## Tests

```bash
cargo test -p xmsg-push
```

The end-to-end smoke (`tools/e2e-smoke`) runs a stand-in push service: nothing is pushed while the recipient is active, and once they're away the message arrives encrypted and VAPID-signed.
