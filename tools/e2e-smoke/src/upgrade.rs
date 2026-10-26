// SPDX-License-Identifier: AGPL-3.0-only
//! The upgrade test (ADR-0003): `upgrade-seed` runs against version N, `upgrade-verify`
//! against N+1 after the documented upgrade, with the state file in between.
//!
//! Seeding leaves state in every sequencer store that an upgrade could silently reset:
//! a channel head (3 messages), membership (bob), dedupe ids, and a reaction count.
//! Verifying checks that each one survived:
//! - the history still holds the 3 messages (Cassandra);
//! - an access token issued before the upgrade still connects (the signing key);
//! - bob resending a seeded client_msg_id is dropped as a duplicate (`dedupe`);
//! - the next message gets msg_seq 4, not 1 (`channel-heads`) and not 5 (`dedupe`);
//! - bob can still post without rejoining (`members`);
//! - bob's reaction makes the count 2, not 1 (`reaction-counts`).
//!
//! Env: XMSG_AUTH_URL, XMSG_CHANNELS_URL, XMSG_MESSAGES_URL, XMSG_GATEWAY_URL.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use xmsg_proto::Message;
use xmsg_proto::v1::{SequencedEvent, sequenced_event};

use super::{Client, call, connect, expect_ack, log_in, send, sign_up_and_log_in, wait_events};

const THUMBS_UP: &str = "%F0%9F%91%8D";

struct Urls {
    auth: String,
    channels: String,
    messages: String,
    gateway: String,
}

fn urls() -> Result<Urls, String> {
    let var = |name: &str| std::env::var(name).map_err(|_| format!("{name} is not set"));
    Ok(Urls {
        auth: var("XMSG_AUTH_URL")?,
        channels: var("XMSG_CHANNELS_URL")?,
        messages: var("XMSG_MESSAGES_URL")?,
        gateway: var("XMSG_GATEWAY_URL")?,
    })
}

/// Version N: two users, a channel with 3 messages, alice's 👍 on the first.
pub async fn seed(file: &str) -> Result<(), String> {
    let urls = urls()?;
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let emails = ["alice", "bob"].map(|name| format!("upgrade-{name}-{run_id}@example.org"));
    let alice = sign_up_and_log_in(&urls.auth, &emails[0]).await?;
    let bob = sign_up_and_log_in(&urls.auth, &emails[1]).await?;
    let (status, reply) = call(
        "POST",
        &format!("{}/v1/channels", urls.channels),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("upgrade-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("no channel_id")?;

    // [alice, bob]
    let mut clients = [
        connect(&urls.gateway, &alice.1).await?,
        connect(&urls.gateway, &bob.1).await?,
    ];
    for (i, id) in [(0, "u-1"), (1, "u-2"), (0, "u-3")] {
        send(&mut clients[i].ws, channel, id).await?;
        expect_ack(&mut clients[i], id).await?;
    }
    let seqs = message_seqs(&mut clients[1], channel, &["u-1", "u-2", "u-3"]).await?;
    if seqs != [1, 2, 3] {
        return Err(format!("seeding: expected msg_seq 1, 2, 3, got {seqs:?}"));
    }
    react(&urls, channel, 1, &alice.1).await?;
    reaction_count(&mut clients[1], channel, 1, 1).await?;

    let state = serde_json::json!({
        "channel": channel.to_string(),
        "alice": emails[0],
        "bob": emails[1],
        "alice_token": alice.1,
    });
    std::fs::write(file, state.to_string()).map_err(|e| format!("{file}: {e}"))?;
    println!(
        "upgrade seed: channel {channel} with messages 1..3 and a 👍 on message 1; state in {file}"
    );
    Ok(())
}

/// Version N+1: everything `seed` left must still be there and keep counting.
pub async fn verify(file: &str) -> Result<(), String> {
    let urls = urls()?;
    let state: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(file).map_err(|e| format!("{file}: {e}"))?)
            .map_err(|e| format!("{file}: {e}"))?;
    let field = |name: &str| state[name].as_str().ok_or(format!("{file}: no {name}"));
    let channel: i64 = field("channel")?.parse().map_err(|_| "bad channel")?;

    // Tokens issued by N still verify: the signing key came along.
    connect(&urls.gateway, field("alice_token")?)
        .await
        .map_err(|e| format!("a token issued before the upgrade: {e}"))?;
    let alice = log_in(&urls.auth, field("alice")?).await?;
    let bob = log_in(&urls.auth, field("bob")?).await?;

    let before = history(&urls, channel, &alice.1, 3).await?;
    if before != ["1:u-1", "2:u-2", "3:u-3"] {
        return Err(format!(
            "history after the upgrade: expected the 3 seeded messages, got {before:?}"
        ));
    }

    // [alice, bob]
    let mut clients = [
        connect(&urls.gateway, &alice.1).await?,
        connect(&urls.gateway, &bob.1).await?,
    ];
    // A retry from before the upgrade: dropped. Then two new messages, bob's without rejoining.
    for (i, id) in [(1, "u-2"), (0, "u-4"), (1, "u-5")] {
        send(&mut clients[i].ws, channel, id).await?;
        expect_ack(&mut clients[i], id).await?;
    }
    let seqs = message_seqs(&mut clients[0], channel, &["u-4", "u-5"]).await?;
    if seqs != [4, 5] {
        return Err(format!(
            "after the upgrade the next messages must be msg_seq 4 and 5, got {seqs:?} \
             (1 and 2: channel heads were reset; 5 and 6: dedupe was)"
        ));
    }
    react(&urls, channel, 1, &bob.1).await?;
    reaction_count(&mut clients[0], channel, 1, 2).await?;

    let after = history(&urls, channel, &alice.1, 5).await?;
    let expected = ["1:u-1", "2:u-2", "3:u-3", "4:u-4", "5:u-5"];
    if after != expected {
        return Err(format!(
            "history: expected {expected:?} (one u-2), got {after:?}"
        ));
    }
    println!(
        "upgrade verified: history, an old token, dedupe (u-2 dropped), channel head (msg_seq 4, 5), \
         membership (bob posts) and reaction counts (👍 2) survived"
    );
    Ok(())
}

/// The msg_seq of each client_msg_id, as delivered to `client` (waits up to 20 s).
async fn message_seqs(client: &mut Client, channel: i64, ids: &[&str]) -> Result<Vec<i64>, String> {
    let find = |events: &[SequencedEvent]| -> Vec<i64> {
        ids.iter()
            .filter_map(|id| {
                events
                    .iter()
                    .find(|e| {
                        e.channel_id == channel
                            && e.dedupe_id == *id
                            && matches!(
                                e.payload,
                                Some(sequenced_event::Payload::MessageCreated(_))
                            )
                    })
                    .map(|e| e.msg_seq)
            })
            .collect()
    };
    wait_events(client, |e| find(e).len() == ids.len()).await?;
    let seqs = find(&client.events);
    if seqs.len() != ids.len() {
        return Err(format!(
            "only {} of the messages {ids:?} were delivered",
            seqs.len()
        ));
    }
    Ok(seqs)
}

async fn react(urls: &Urls, channel: i64, msg_seq: i64, token: &str) -> Result<(), String> {
    let url = format!(
        "{}/v1/channels/{channel}/messages/{msg_seq}/reactions/{THUMBS_UP}?counter=1",
        urls.messages
    );
    match call("PUT", &url, token, None).await? {
        (202, _) => Ok(()),
        (status, reply) => Err(format!("reacting: {status} {reply}")),
    }
}

/// Waits until `client` sees the 👍 count on `msg_seq` reach `count`.
async fn reaction_count(
    client: &mut Client,
    channel: i64,
    msg_seq: i64,
    count: i64,
) -> Result<(), String> {
    let reached = |events: &[SequencedEvent]| {
        events.iter().any(|e| {
            e.channel_id == channel
                && matches!(&e.payload,
                    Some(sequenced_event::Payload::ReactionsChanged(r))
                        if r.msg_seq == msg_seq && r.emoji == "👍" && r.count == count)
        })
    };
    wait_events(client, reached).await?;
    if reached(&client.events) {
        Ok(())
    } else {
        Err(format!(
            "the 👍 count on message {msg_seq} never reached {count}"
        ))
    }
}

/// The channel's messages from the history API as "msg_seq:client_msg_id". The
/// persister writes a moment after delivery, so it waits up to 30 s for `at_least`.
async fn history(
    urls: &Urls,
    channel: i64,
    token: &str,
    at_least: usize,
) -> Result<Vec<String>, String> {
    let http = reqwest::Client::new();
    let url = format!("{}/v1/channels/{channel}/events?after=0", urls.messages);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut last = Vec::new();
    loop {
        let response = http
            .get(&url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| format!("history: {e}"))?;
        if response.status() == 200 {
            let bytes = response.bytes().await.map_err(|e| e.to_string())?;
            let page =
                xmsg_proto::v1::EventPage::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
            let messages: Vec<String> = page
                .events
                .iter()
                .filter(|e| matches!(e.payload, Some(sequenced_event::Payload::MessageCreated(_))))
                .map(|e| format!("{}:{}", e.msg_seq, e.dedupe_id))
                .collect();
            if messages.len() >= at_least {
                return Ok(messages);
            }
            last = messages;
        }
        if Instant::now() > deadline {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
