// SPDX-License-Identifier: AGPL-3.0-only
//! The sequencer's membership endpoint as a snapshot source (plan §5.6): asked only when
//! the wide-column store is behind. Any sequencer instance will do: one that doesn't own
//! the channel's partition redirects to the one that does.

use std::time::Duration;

use xmsg_proto::Message;
use xmsg_proto::v1::ChannelMembers;

use crate::membership::{Snapshot, SnapshotSource};

pub struct Sequencer {
    base_url: String,
    http: reqwest::Client,
}

impl Sequencer {
    pub fn new(base_url: impl Into<String>) -> Self {
        Sequencer {
            base_url: base_url.into(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .expect("HTTP client"),
        }
    }
}

impl SnapshotSource for Sequencer {
    async fn load(&self, channel_id: i64, _at_least: i64) -> Result<Option<Snapshot>, String> {
        let url = format!("{}/v1/channels/{channel_id}/members", self.base_url);
        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        match response.status().as_u16() {
            200 => {
                let bytes = response.bytes().await.map_err(|e| e.to_string())?;
                let m = ChannelMembers::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
                Ok(Some(Snapshot {
                    version: m.membership_version,
                    members: m.user_ids.into_iter().collect(),
                }))
            }
            404 => Ok(None),
            status => Err(format!("membership endpoint: {status}")),
        }
    }
}
