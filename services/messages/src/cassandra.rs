// SPDX-License-Identifier: AGPL-3.0-only
//! History reads from the wide-column store (written by the persister), at LOCAL_QUORUM
//! like its writes, so a head we read has its rows visible.

use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::statement::Consistency;
use scylla::statement::prepared::PreparedStatement;
use xmsg_proto::v1::ChannelType;

use crate::history::{EventRow, Head, MessageRow, ReplyRow, Store};

pub struct Cassandra {
    session: Session,
    member: PreparedStatement,
    head: PreparedStatement,
    events: PreparedStatement,
    messages: PreparedStatement,
    thread: PreparedStatement,
    my_channels: PreparedStatement,
}

fn channel_type(name: &str) -> i32 {
    (match name {
        "public" => ChannelType::Public,
        "private" => ChannelType::Private,
        "dm" => ChannelType::Dm,
        "group_dm" => ChannelType::GroupDm,
        "broadcast" => ChannelType::Broadcast,
        _ => ChannelType::Unspecified,
    }) as i32
}

impl Cassandra {
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
        Ok(Cassandra {
            member: prepare(format!("SELECT user_id FROM {k}.members_by_channel WHERE channel_id = ? AND user_id = ?")).await?,
            head: prepare(format!("SELECT event_seq, msg_seq, channel_type FROM {k}.channel_heads WHERE channel_id = ?")).await?,
            events: prepare(format!(
                "SELECT event_seq, type, payload FROM {k}.channel_events \
                 WHERE channel_id = ? AND bucket = ? AND event_seq > ? ORDER BY event_seq ASC LIMIT ?"
            ))
            .await?,
            messages: prepare(format!(
                "SELECT msg_seq, message_id, event_seq, author_id, client_msg_id, body, edited_at, deleted, attachments FROM {k}.messages \
                 WHERE channel_id = ? AND bucket = ? AND msg_seq >= ? AND msg_seq <= ?"
            ))
            .await?,
            my_channels: prepare(format!("SELECT channel_id FROM {k}.channels_by_user WHERE user_id = ? LIMIT 1000")).await?,
            thread: prepare(format!(
                "SELECT reply_seq, message_id, author_id, body, attachments FROM {k}.thread_messages \
                 WHERE channel_id = ? AND thread_root = ? AND reply_seq > ? ORDER BY reply_seq ASC LIMIT ?"
            ))
            .await?,
            session,
        })
    }
}

fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}

impl Store for Cassandra {
    async fn my_channels(&self, user_id: i64) -> Result<Vec<i64>, String> {
        self.session
            .execute_unpaged(&self.my_channels, (user_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64,)>()
            .map_err(err)?
            .map(|r| r.map(|(c,)| c).map_err(err))
            .collect()
    }

    async fn thread(
        &self,
        channel_id: i64,
        root_msg_seq: i64,
        after: i64,
        limit: usize,
    ) -> Result<Vec<ReplyRow>, String> {
        self.session
            .execute_unpaged(
                &self.thread,
                (channel_id, root_msg_seq, after, limit as i32),
            )
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64, i64, Option<i64>, Option<String>, Option<Vec<u8>>)>()
            .map_err(err)?
            .map(|r| {
                r.map(
                    |(reply_seq, message_id, author_id, body, attachments)| ReplyRow {
                        reply_seq,
                        message_id,
                        author_id: author_id.unwrap_or_default(),
                        body: body.unwrap_or_default(),
                        attachments: attachments.unwrap_or_default(),
                    },
                )
                .map_err(err)
            })
            .collect()
    }

    async fn is_member(&self, channel_id: i64, user_id: i64) -> Result<bool, String> {
        let rows = self
            .session
            .execute_unpaged(&self.member, (channel_id, user_id))
            .await
            .map_err(err)?;
        Ok(rows.into_rows_result().map_err(err)?.rows_num() > 0)
    }

    async fn head(&self, channel_id: i64) -> Result<Option<Head>, String> {
        let row = self
            .session
            .execute_unpaged(&self.head, (channel_id,))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .maybe_first_row::<(Option<i64>, Option<i64>, Option<String>)>()
            .map_err(err)?;
        Ok(row.and_then(|(seq, msg_seq, kind)| {
            seq.map(|event_seq| Head {
                event_seq,
                msg_seq: msg_seq.unwrap_or(0),
                channel_type: channel_type(kind.as_deref().unwrap_or("")),
            })
        }))
    }

    async fn events(
        &self,
        channel_id: i64,
        bucket: i32,
        after: i64,
        limit: usize,
    ) -> Result<Vec<EventRow>, String> {
        self.session
            .execute_unpaged(&self.events, (channel_id, bucket, after, limit as i32))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(i64, Option<String>, Option<Vec<u8>>)>()
            .map_err(err)?
            .map(|r| {
                r.map(|(event_seq, kind, payload)| EventRow {
                    event_seq,
                    kind: kind.unwrap_or_default(),
                    payload: payload.unwrap_or_default(),
                })
                .map_err(err)
            })
            .collect()
    }

    async fn messages(
        &self,
        channel_id: i64,
        bucket: i32,
        from: i64,
        to: i64,
    ) -> Result<Vec<MessageRow>, String> {
        self.session
            .execute_unpaged(&self.messages, (channel_id, bucket, from, to))
            .await
            .map_err(err)?
            .into_rows_result()
            .map_err(err)?
            .rows::<(
                i64,
                i64,
                Option<i64>,
                Option<i64>,
                Option<String>,
                Option<String>,
                Option<scylla::value::CqlTimestamp>,
                Option<bool>,
                Option<Vec<u8>>,
            )>()
            .map_err(err)?
            .map(|r| {
                r.map(
                    |(
                        msg_seq,
                        message_id,
                        event_seq,
                        author_id,
                        client_msg_id,
                        body,
                        edited_at,
                        deleted,
                        attachments,
                    )| {
                        MessageRow {
                            msg_seq,
                            message_id,
                            event_seq: event_seq.unwrap_or_default(),
                            author_id: author_id.unwrap_or_default(),
                            client_msg_id: client_msg_id.unwrap_or_default(),
                            body: body.unwrap_or_default(),
                            edited_at_ms: edited_at.map_or(0, |t| t.0),
                            deleted: deleted.unwrap_or(false),
                            attachments: attachments.unwrap_or_default(),
                        }
                    },
                )
                .map_err(err)
            })
            .collect()
    }
}
