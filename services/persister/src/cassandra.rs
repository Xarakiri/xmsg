// SPDX-License-Identifier: AGPL-3.0-only
//! Applies [`Write`]s to Cassandra 5 or ScyllaDB (CQL contract, ADR-0020).
//! Every statement runs at LOCAL_QUORUM (ADR-0006).

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::value::CqlTimestamp;

use crate::Write;

pub struct Store {
    session: Session,
    insert_message: PreparedStatement,
    insert_channel_event: PreparedStatement,
    select_deleted: PreparedStatement,
    insert_channel_mention: PreparedStatement,
    set_reaction_count: PreparedStatement,
    insert_thread_message: PreparedStatement,
    root_exists: PreparedStatement,
    update_root_replies: PreparedStatement,
    insert_reaction: PreparedStatement,
    delete_reaction: PreparedStatement,
    update_head_mention: PreparedStatement,
    update_message_body: PreparedStatement,
    delete_message: PreparedStatement,
    insert_member_by_channel: PreparedStatement,
    insert_channel_by_user: PreparedStatement,
    delete_member_by_channel: PreparedStatement,
    delete_channel_by_user: PreparedStatement,
    update_head: PreparedStatement,
    update_head_with_msg: PreparedStatement,
    select_members: PreparedStatement,
    update_user_fanout_mode: PreparedStatement,
}

impl Store {
    pub async fn connect(nodes: &[String], keyspace: &str) -> Result<Self, String> {
        let session = SessionBuilder::new()
            .known_nodes(nodes)
            .build()
            .await
            .map_err(|e| format!("connecting to {nodes:?}: {e}"))?;
        let prepare = async |cql: String| -> Result<PreparedStatement, String> {
            let mut p = session
                .prepare(cql.clone())
                .await
                .map_err(|e| format!("preparing `{cql}`: {e}"))?;
            p.set_consistency(Consistency::LocalQuorum);
            Ok(p)
        };
        let k = keyspace;
        Ok(Store {
            insert_message: prepare(format!(
                "INSERT INTO {k}.messages (channel_id, bucket, msg_seq, message_id, event_seq, author_id, client_msg_id, body, deleted) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, false)"
            ))
            .await?,
            insert_thread_message: prepare(format!(
                "INSERT INTO {k}.thread_messages (channel_id, thread_root, reply_seq, message_id, author_id, body, also_sent_to_channel, deleted) \
                 VALUES (?, ?, ?, ?, ?, ?, false, false)"
            ))
            .await?,
            root_exists: prepare(format!(
                "SELECT message_id FROM {k}.messages WHERE channel_id = ? AND bucket = ? AND msg_seq = ? AND message_id = ?"
            ))
            .await?,
            update_root_replies: prepare(format!(
                "UPDATE {k}.messages SET reply_count = ?, last_reply_at = ? \
                 WHERE channel_id = ? AND bucket = ? AND msg_seq = ? AND message_id = ?"
            ))
            .await?,
            set_reaction_count: prepare(format!(
                "UPDATE {k}.reaction_counts SET count = ? WHERE channel_id = ? AND message_id = ? AND emoji = ?"
            ))
            .await?,
            insert_reaction: prepare(format!(
                "INSERT INTO {k}.reactions (channel_id, message_id, emoji, user_id) VALUES (?, ?, ?, ?)"
            ))
            .await?,
            delete_reaction: prepare(format!(
                "DELETE FROM {k}.reactions WHERE channel_id = ? AND message_id = ? AND emoji = ? AND user_id = ?"
            ))
            .await?,
            insert_channel_mention: prepare(format!(
                "INSERT INTO {k}.channel_mentions (channel_id, msg_seq, kind) VALUES (?, ?, 'channel')"
            ))
            .await?,
            update_head_mention: prepare(format!(
                "UPDATE {k}.channel_heads SET last_mention_seq = ? WHERE channel_id = ?"
            ))
            .await?,
            select_deleted: prepare(format!(
                "SELECT deleted FROM {k}.messages WHERE channel_id = ? AND bucket = ? AND msg_seq = ? AND message_id = ?"
            ))
            .await?,
            update_message_body: prepare(format!(
                "UPDATE {k}.messages SET body = ?, edited_at = ? \
                 WHERE channel_id = ? AND bucket = ? AND msg_seq = ? AND message_id = ?"
            ))
            .await?,
            delete_message: prepare(format!(
                "UPDATE {k}.messages SET body = null, deleted = true \
                 WHERE channel_id = ? AND bucket = ? AND msg_seq = ? AND message_id = ?"
            ))
            .await?,
            insert_channel_event: prepare(format!(
                "INSERT INTO {k}.channel_events (channel_id, bucket, event_seq, type, payload) VALUES (?, ?, ?, ?, ?)"
            ))
            .await?,
            insert_member_by_channel: prepare(format!(
                "INSERT INTO {k}.members_by_channel (channel_id, user_id, role, joined_at) VALUES (?, ?, ?, ?)"
            ))
            .await?,
            insert_channel_by_user: prepare(format!(
                "INSERT INTO {k}.channels_by_user (user_id, workspace_id, channel_id, channel_type, fanout_mode, joined_at) \
                 VALUES (?, ?, ?, ?, ?, ?)"
            ))
            .await?,
            delete_member_by_channel: prepare(format!(
                "DELETE FROM {k}.members_by_channel WHERE channel_id = ? AND user_id = ?"
            ))
            .await?,
            delete_channel_by_user: prepare(format!(
                "DELETE FROM {k}.channels_by_user WHERE user_id = ? AND workspace_id = ? AND channel_id = ?"
            ))
            .await?,
            update_head: prepare(format!(
                "UPDATE {k}.channel_heads SET event_seq = ?, membership_version = ?, fanout_mode = ?, channel_type = ? \
                 WHERE channel_id = ?"
            ))
            .await?,
            update_head_with_msg: prepare(format!(
                "UPDATE {k}.channel_heads SET event_seq = ?, msg_seq = ?, membership_version = ?, fanout_mode = ?, \
                 channel_type = ? WHERE channel_id = ?"
            ))
            .await?,
            select_members: prepare(format!("SELECT user_id FROM {k}.members_by_channel WHERE channel_id = ?")).await?,
            update_user_fanout_mode: prepare(format!(
                "UPDATE {k}.channels_by_user SET fanout_mode = ? WHERE user_id = ? AND workspace_id = ? AND channel_id = ?"
            ))
            .await?,
            session,
        })
    }

    /// Applies writes strictly in order; stops at the first error.
    pub async fn apply(&self, writes: &[Write]) -> Result<(), String> {
        for write in writes {
            self.apply_one(write)
                .await
                .map_err(|e| format!("{write:?}: {e}"))?;
        }
        Ok(())
    }

    async fn apply_one(
        &self,
        write: &Write,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let s = &self.session;
        match write {
            Write::Message {
                channel_id,
                bucket,
                msg_seq,
                message_id,
                event_seq,
                author_id,
                client_msg_id,
                body,
            } => {
                s.execute_unpaged(
                    &self.insert_message,
                    (
                        channel_id,
                        bucket,
                        msg_seq,
                        message_id,
                        event_seq,
                        author_id,
                        client_msg_id,
                        body,
                    ),
                )
                .await?;
            }
            Write::MessageEdited {
                channel_id,
                bucket,
                msg_seq,
                message_id,
                body,
                edited_at_ms,
            } => {
                // The persister is the only writer and applies a channel's events in order,
                // so reading first is race-free (and avoids mixing LWT with plain writes).
                let key = (channel_id, bucket, msg_seq, message_id);
                let deleted = s
                    .execute_unpaged(&self.select_deleted, key)
                    .await?
                    .into_rows_result()?
                    .maybe_first_row::<(Option<bool>,)>()?
                    .and_then(|(d,)| d)
                    .unwrap_or(false);
                if !deleted {
                    s.execute_unpaged(
                        &self.update_message_body,
                        (
                            body,
                            CqlTimestamp(*edited_at_ms),
                            channel_id,
                            bucket,
                            msg_seq,
                            message_id,
                        ),
                    )
                    .await?;
                }
            }
            Write::ThreadReply {
                channel_id,
                root_msg_seq,
                root_message_id,
                reply_seq,
                message_id,
                author_id,
                body,
                reply_count,
                at_ms,
            } => {
                s.execute_unpaged(
                    &self.insert_thread_message,
                    (
                        channel_id,
                        root_msg_seq,
                        reply_seq,
                        message_id,
                        author_id,
                        body,
                    ),
                )
                .await?;
                let root = (
                    channel_id,
                    crate::bucket(*root_msg_seq),
                    root_msg_seq,
                    root_message_id,
                );
                let exists = s
                    .execute_unpaged(&self.root_exists, root)
                    .await?
                    .into_rows_result()?
                    .rows_num()
                    > 0;
                if exists {
                    s.execute_unpaged(
                        &self.update_root_replies,
                        (
                            *reply_count as i32,
                            CqlTimestamp(*at_ms),
                            channel_id,
                            crate::bucket(*root_msg_seq),
                            root_msg_seq,
                            root_message_id,
                        ),
                    )
                    .await?;
                }
            }
            Write::Reactions {
                channel_id,
                message_id,
                emoji,
                count,
                added,
                removed,
            } => {
                for user in added {
                    s.execute_unpaged(&self.insert_reaction, (channel_id, message_id, emoji, user))
                        .await?;
                }
                for user in removed {
                    s.execute_unpaged(&self.delete_reaction, (channel_id, message_id, emoji, user))
                        .await?;
                }
                // Absolute, from the sequencer: a replay sets the same value again.
                s.execute_unpaged(
                    &self.set_reaction_count,
                    (*count as i32, channel_id, message_id, emoji),
                )
                .await?;
            }
            Write::ChannelMention {
                channel_id,
                msg_seq,
            } => {
                s.execute_unpaged(&self.insert_channel_mention, (channel_id, msg_seq))
                    .await?;
                // Events arrive in channel order, so the latest mention is the highest.
                s.execute_unpaged(&self.update_head_mention, (msg_seq, channel_id))
                    .await?;
            }
            Write::MessageDeleted {
                channel_id,
                bucket,
                msg_seq,
                message_id,
            } => {
                s.execute_unpaged(
                    &self.delete_message,
                    (channel_id, bucket, msg_seq, message_id),
                )
                .await?;
            }
            Write::ChannelEvent {
                channel_id,
                bucket,
                event_seq,
                kind,
                payload,
            } => {
                s.execute_unpaged(
                    &self.insert_channel_event,
                    (channel_id, bucket, event_seq, kind, payload),
                )
                .await?;
            }
            Write::MemberAdded {
                channel_id,
                user_id,
                workspace_id,
                role,
                channel_type,
                fanout_mode,
                joined_at_ms,
            } => {
                let joined = CqlTimestamp(*joined_at_ms);
                s.execute_unpaged(
                    &self.insert_member_by_channel,
                    (channel_id, user_id, role, joined),
                )
                .await?;
                s.execute_unpaged(
                    &self.insert_channel_by_user,
                    (
                        user_id,
                        workspace_id,
                        channel_id,
                        channel_type,
                        fanout_mode,
                        joined,
                    ),
                )
                .await?;
            }
            Write::MemberRemoved {
                channel_id,
                user_id,
                workspace_id,
            } => {
                s.execute_unpaged(&self.delete_member_by_channel, (channel_id, user_id))
                    .await?;
                s.execute_unpaged(
                    &self.delete_channel_by_user,
                    (user_id, workspace_id, channel_id),
                )
                .await?;
            }
            Write::Head {
                channel_id,
                event_seq,
                msg_seq,
                membership_version,
                fanout_mode,
                channel_type,
            } => match msg_seq {
                Some(m) => {
                    s.execute_unpaged(
                        &self.update_head_with_msg,
                        (
                            event_seq,
                            m,
                            membership_version,
                            fanout_mode,
                            channel_type,
                            channel_id,
                        ),
                    )
                    .await?;
                }
                None => {
                    s.execute_unpaged(
                        &self.update_head,
                        (
                            event_seq,
                            membership_version,
                            fanout_mode,
                            channel_type,
                            channel_id,
                        ),
                    )
                    .await?;
                }
            },
            Write::FanoutModeForMembers {
                channel_id,
                workspace_id,
                fanout_mode,
            } => {
                let rows = s
                    .execute_unpaged(&self.select_members, (channel_id,))
                    .await?
                    .into_rows_result()?;
                for row in rows.rows::<(i64,)>()? {
                    let (user_id,) = row?;
                    s.execute_unpaged(
                        &self.update_user_fanout_mode,
                        (fanout_mode, user_id, workspace_id, channel_id),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    pub fn session(&self) -> &Session {
        &self.session
    }
}
