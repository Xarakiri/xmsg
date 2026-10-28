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
  size bigint, sha256 text, created_at timestamp, width int, height int, thumbnail_type text
);

-- Writer: calls (ADR-0025). A channel's call while it lasts: one row, removed when it ends.
CREATE TABLE IF NOT EXISTS active_calls (
  channel_id bigint PRIMARY KEY, call_id text, huddle boolean, started_at timestamp,
  participants set<bigint>, announced boolean
);
