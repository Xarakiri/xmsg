-- SPDX-License-Identifier: AGPL-3.0-only
-- Wide-column schema (plan §7.2) for Cassandra 5 (UCS compaction). Small tier: RF 1.

CREATE KEYSPACE IF NOT EXISTS xmsg
  WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 1};

USE xmsg;

-- Writer: persister. message_id is in the key so nothing can be overwritten (ADR-0014).
CREATE TABLE IF NOT EXISTS messages (
  channel_id bigint, bucket int,
  msg_seq bigint, event_seq bigint, message_id bigint, author_id bigint,
  -- The sender's client_msg_id, so a client syncing history recognizes its own sends.
  client_msg_id text,
  body text, attachments blob,
  reply_count int, last_reply_at timestamp, edited_at timestamp, deleted boolean,
  PRIMARY KEY ((channel_id, bucket), msg_seq, message_id)
) WITH CLUSTERING ORDER BY (msg_seq DESC, message_id ASC)
  AND compaction = {'class': 'UnifiedCompactionStrategy'};

CREATE TABLE IF NOT EXISTS thread_messages (
  channel_id bigint, thread_root bigint, reply_seq bigint, message_id bigint, author_id bigint,
  body text, also_sent_to_channel boolean, edited_at timestamp, deleted boolean, attachments blob,
  PRIMARY KEY ((channel_id, thread_root), reply_seq, message_id)
) WITH compaction = {'class': 'UnifiedCompactionStrategy'};
-- Added with files in replies (ADR-0024); existing installations get it here.
ALTER TABLE thread_messages ADD IF NOT EXISTS attachments blob;

CREATE TABLE IF NOT EXISTS thread_followers (
  channel_id bigint, thread_root bigint, user_id bigint,
  PRIMARY KEY ((channel_id, thread_root), user_id)
);

CREATE TABLE IF NOT EXISTS channel_heads (
  channel_id bigint PRIMARY KEY,
  event_seq bigint, msg_seq bigint, last_mention_seq bigint,
  membership_version bigint, fanout_mode text, channel_type text
);

CREATE TABLE IF NOT EXISTS channel_events (
  channel_id bigint, bucket int, event_seq bigint, type text, payload blob,
  PRIMARY KEY ((channel_id, bucket), event_seq)
) WITH default_time_to_live = 2592000
  AND compaction = {'class': 'UnifiedCompactionStrategy'};

-- Membership: the persister is the only writer (ADR-0006).
CREATE TABLE IF NOT EXISTS members_by_channel (
  channel_id bigint, user_id bigint, role text, joined_at timestamp,
  PRIMARY KEY (channel_id, user_id)
);

CREATE TABLE IF NOT EXISTS channels_by_user (
  user_id bigint, workspace_id bigint, channel_id bigint,
  channel_type text, fanout_mode text, joined_at timestamp,
  PRIMARY KEY (user_id, workspace_id, channel_id)
);

CREATE TABLE IF NOT EXISTS channel_mentions (
  channel_id bigint, msg_seq bigint, kind text,
  PRIMARY KEY (channel_id, msg_seq)
) WITH CLUSTERING ORDER BY (msg_seq DESC);

CREATE TABLE IF NOT EXISTS reactions (
  channel_id bigint, message_id bigint, emoji text, user_id bigint,
  PRIMARY KEY ((channel_id, message_id), emoji, user_id)
);

CREATE TABLE IF NOT EXISTS reaction_counts (
  channel_id bigint, message_id bigint, emoji text, count int,
  PRIMARY KEY ((channel_id, message_id), emoji)
);

-- Writer: readstate owner per user (ADR-0012).
CREATE TABLE IF NOT EXISTS read_state (
  user_id bigint, channel_id bigint, last_read_msg_seq bigint, last_mention_seq bigint,
  PRIMARY KEY (user_id, channel_id)
);

CREATE TABLE IF NOT EXISTS user_mentions (
  user_id bigint, channel_id bigint, msg_seq bigint,
  PRIMARY KEY ((user_id, channel_id), msg_seq)
) WITH CLUSTERING ORDER BY (msg_seq DESC);

CREATE TABLE IF NOT EXISTS thread_read_state (
  user_id bigint, channel_id bigint, thread_root bigint, last_read_reply_seq bigint,
  PRIMARY KEY (user_id, channel_id, thread_root)
);

-- Writer: settings-writer (ADR-0016).
CREATE TABLE IF NOT EXISTS member_settings (
  user_id bigint, channel_id bigint, notify_level text, muted boolean, updated_at timestamp,
  PRIMARY KEY (user_id, channel_id)
);

-- Writer: push.
CREATE TABLE IF NOT EXISTS push_subscribers (
  channel_id bigint, user_id bigint,
  PRIMARY KEY (channel_id, user_id)
);

-- Writer: media (ADR-0024). One row per upload, never changed; the bytes are in object storage.
CREATE TABLE IF NOT EXISTS files (
  file_id bigint PRIMARY KEY, channel_id bigint, uploader_id bigint, name text, content_type text,
  size bigint, sha256 text, created_at timestamp, width int, height int, thumbnail_type text,
  message_id bigint, deleted_at timestamp, workspace_id bigint
);
-- Added with file deletion (ADR-0024); existing installations get them here. message_id:
-- the message the file was sent with (a file belongs to one); deleted_at: its message was
-- deleted, so it's gone, and its bytes are removed after the grace period.
ALTER TABLE files ADD IF NOT EXISTS message_id bigint;
ALTER TABLE files ADD IF NOT EXISTS deleted_at timestamp;
-- Added with storage quotas: whose quota the file counts against.
ALTER TABLE files ADD IF NOT EXISTS workspace_id bigint;

-- Writer: media. Files whose bytes are due for removal, by the day they were deleted.
CREATE TABLE IF NOT EXISTS file_purges (
  day int, file_id bigint, channel_id bigint, deleted_at timestamp, workspace_id bigint, size bigint,
  PRIMARY KEY (day, file_id)
);
ALTER TABLE file_purges ADD IF NOT EXISTS workspace_id bigint;
ALTER TABLE file_purges ADD IF NOT EXISTS size bigint;

-- Writer: media. Bytes stored per workspace (files until their bytes are purged), for
-- storage quotas (ADR-0024). Counters: approximate under retries, which is fine for a quota.
CREATE TABLE IF NOT EXISTS workspace_storage (
  workspace_id bigint PRIMARY KEY, bytes counter
);
