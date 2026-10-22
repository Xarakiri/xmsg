// SPDX-License-Identifier: AGPL-3.0-only
//! End-to-end smoke test of the message path that exists today:
//!
//!   client --WebSocket--> gateway --NATS--> messages --Kafka--> sequencer --> messages.persisted
//!   messages.persisted --> persister --> Cassandra, and --> fan-out --NATS--> gateway --WebSocket--> clients
//!
//! 1. Creates a channel by producing `channel.created` to `messages.in`
//!    (the channels service doesn't exist yet).
//! 2. Two users connect to the gateway and each sends a message.
//! 3. Waits for both `SendAck`s, then reads `messages.persisted` and checks the
//!    sequenced events: consecutive msg_seq, the right authors, and a rejection
//!    for a non-member.
//!
//! 4. Checks fan-out delivery: both members receive all three messages over
//!    their WebSockets in order, and the non-member gets only their rejection.
//! 5. If `XMSG_CQL_NODES` is set, checks what the persister wrote: the three
//!    messages, the members and the channel head.
//!
//! With `XMSG_CHANNELS_URL` set (needs `XMSG_AUTH_URL`), the channel is created
//! through the channels service, and at the end membership commands are checked
//! against the sequencer's verdicts: a non-admin can't remove someone, an outsider
//! can't join a private channel, an admin's invite and a member leaving are accepted.
//!
//! With `XMSG_USERS_URL` set too, profiles and workspaces are checked: a renamed user's
//! name is visible to a co-member, and someone outside a new workspace can't be put
//! into its channels until they accept an invite.
//!
//! With `XMSG_AUTH_URL` set, the three users sign up and log in through the auth
//! service and connect with real access tokens (the gateway checks them against the
//! JWKS); a forged token must be refused. Without it, development tokens are used.
//!
//! Exits 0 on success, 1 on failure.
//! Env: XMSG_GATEWAY_URL (ws://127.0.0.1:4000/ws), XMSG_KAFKA_BOOTSTRAP (127.0.0.1:9092),
//! XMSG_CQL_NODES (optional, e.g. 127.0.0.1:9042), XMSG_AUTH_URL (optional, e.g. http://127.0.0.1:8081),
//! XMSG_CHANNELS_URL (optional, e.g. http://127.0.0.1:8082),
//! XMSG_MESSAGES_URL (optional, e.g. http://127.0.0.1:8083; history reads, checked after membership),
//! XMSG_USERS_URL (optional, e.g. http://127.0.0.1:8084; needs XMSG_CHANNELS_URL),
//! XMSG_EXPECT_OUTBOX_RELAY=true (optional: the outbox poller and meta-relay run, so the
//! channel's `channel.created` must reach `messages.in` twice: fast path and outbox),
//! XMSG_EXPECT_REVOCATION=true (optional, needs XMSG_AUTH_URL: logging out closes the
//! session's open socket and its still-valid access token can't reconnect).

use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use rdkafka::ClientConfig;
use rdkafka::Message as _;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tokio_tungstenite::{connect_async, tungstenite::Message as Ws};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    ChannelCreated, ChannelType, ClientFrame, Connect, InboundEvent, RejectReason,
    SendMessageRequest, SequencedEvent, ServerFrame, client_frame, inbound_event, sequenced_event,
    server_frame,
};

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A connected user and the channel events it has received so far.
struct Client {
    ws: Socket,
    events: Vec<SequencedEvent>,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => {
            println!("✅ end-to-end smoke test passed");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("❌ end-to-end smoke test failed: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let gateway =
        std::env::var("XMSG_GATEWAY_URL").unwrap_or_else(|_| "ws://127.0.0.1:4000/ws".into());
    let bootstrap =
        std::env::var("XMSG_KAFKA_BOOTSTRAP").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let channels_url = std::env::var("XMSG_CHANNELS_URL").ok();
    let users = match std::env::var("XMSG_AUTH_URL") {
        Ok(auth) => {
            let mut users = Vec::new();
            for name in ["alice", "bob", "mallory"] {
                users.push(
                    sign_up_and_log_in(&auth, &format!("{name}-{run_id}@example.org")).await?,
                );
            }
            expect_refused(&gateway, &forge(&users[0].1), "invalid token").await?;
            println!("signed up and logged in 3 users via {auth}; a forged token was refused");
            users
        }
        Err(_) => [1_000, 2_000, 3_000]
            .map(|base| {
                (
                    base + run_id % 1_000,
                    format!("dev:{}", base + run_id % 1_000),
                )
            })
            .to_vec(),
    };
    let (alice, bob) = (users[0].0, users[1].0);

    // 1. Create the channel (alice and bob are members).
    let channel = match &channels_url {
        Some(url) => {
            let (status, reply) = call(
                "POST",
                &format!("{url}/v1/channels"),
                &users[0].1,
                Some(serde_json::json!({"type": "private", "name": format!("e2e-{run_id}"), "member_ids": [bob.to_string()]})),
            )
            .await?;
            if status != 201 {
                return Err(format!("creating the channel: {status} {reply}"));
            }
            let id = reply["channel_id"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .ok_or("no channel_id")?;
            println!("created channel {id} with members {alice}, {bob} via {url}");
            id
        }
        None => {
            let channel = run_id; // unique per run
            produce_channel_created(&bootstrap, channel, alice, bob).await?;
            println!("created channel {channel} with members {alice}, {bob}");
            channel
        }
    };

    // 2. Users send through the gateway.
    let mut clients = [
        connect(&gateway, &users[0].1).await?,
        connect(&gateway, &users[1].1).await?,
        connect(&gateway, &users[2].1).await?,
    ];
    // (socket index, client_msg_id): alice, bob, alice again, then mallory (not a member).
    for (i, id) in [(0, "a-1"), (1, "b-1"), (0, "a-2"), (2, "m-1")] {
        send(&mut clients[i].ws, channel, id).await?;
        // Each must be acked (accepted into messages.in); membership is checked later.
        expect_ack(&mut clients[i], id).await?;
    }
    println!("all four sends acked by the gateway");

    // 3. Check what the sequencer produced.
    let events = read_persisted(&bootstrap, channel, 5).await?;
    let messages: Vec<&SequencedEvent> = events
        .iter()
        .filter(|e| matches!(e.payload, Some(sequenced_event::Payload::MessageCreated(_))))
        .collect();
    let seqs: Vec<i64> = messages.iter().map(|e| e.msg_seq).collect();
    let ids: Vec<&str> = messages.iter().map(|e| e.dedupe_id.as_str()).collect();
    if seqs != [1, 2, 3] || ids != ["a-1", "b-1", "a-2"] {
        return Err(format!(
            "unexpected messages: msg_seq {seqs:?}, ids {ids:?}"
        ));
    }
    let rejected = events.iter().find_map(|e| match &e.payload {
        Some(sequenced_event::Payload::Rejected(r)) if e.dedupe_id == "m-1" => Some(r.reason),
        _ => None,
    });
    if rejected != Some(RejectReason::NotAMember as i32) {
        return Err(format!(
            "expected m-1 to be rejected as NOT_A_MEMBER, got {rejected:?}"
        ));
    }
    println!("sequencer output: msg_seq {seqs:?} for {ids:?}; non-member send rejected");

    check_delivery(&mut clients, channel).await?;

    if let Ok(nodes) = std::env::var("XMSG_CQL_NODES") {
        check_store(&nodes, channel, alice, bob).await?;
    }
    if let Some(url) = &channels_url {
        check_membership(url, channel, &users).await?;
        if let Ok(messages_url) = std::env::var("XMSG_MESSAGES_URL") {
            check_history(url, &messages_url, channel, &users).await?;
        }
        if let Ok(users_url) = std::env::var("XMSG_USERS_URL") {
            check_workspaces(&users_url, url, &users, run_id).await?;
        }
        if std::env::var("XMSG_EXPECT_OUTBOX_RELAY").as_deref() == Ok("true") {
            check_outbox_copy(&bootstrap, channel)?;
        }
    }
    if let Ok(auth) = std::env::var("XMSG_AUTH_URL")
        && std::env::var("XMSG_EXPECT_REVOCATION").as_deref() == Ok("true")
    {
        check_revocation(&auth, &gateway, run_id).await?;
    }
    Ok(())
}

/// The outbox safety net (ADR-0010): besides the fast path, the committed outbox row reaches
/// `messages.in` through the poller and meta-relay. The sequencer created the channel once
/// all the same (the checks above saw a single `channel.created` at event_seq 1).
fn check_outbox_copy(bootstrap: &str, channel: i64) -> Result<(), String> {
    use rdkafka::{Offset, TopicPartitionList};
    let partitions: u32 = std::env::var("XMSG_PARTITIONS")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(16);
    let partition = xmsg_partition::partition(channel, partitions) as i32;
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("e2e-outbox-{channel}"))
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| e.to_string())?;
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset("messages.in", partition, Offset::Beginning)
        .map_err(|e| e.to_string())?;
    consumer.assign(&tpl).map_err(|e| e.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut copies = 0;
    while copies < 2 && Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(500))
            && let Ok(e) = InboundEvent::decode(m.payload().unwrap_or_default())
            && e.channel_id == channel
            && matches!(e.payload, Some(inbound_event::Payload::ChannelCreated(_)))
        {
            copies += 1;
        }
    }
    if copies < 2 {
        return Err(format!(
            "outbox: expected channel.created twice in messages.in (fast path + outbox), saw {copies}"
        ));
    }
    println!(
        "outbox: channel.created reached messages.in by the fast path and through the outbox relay; sequenced once"
    );
    Ok(())
}

/// Users service + the channels service's workspace checks.
async fn check_workspaces(
    users_url: &str,
    channels_url: &str,
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let fail =
        |what: &str, got: &(u16, serde_json::Value)| format!("{what}: got {} {}", got.0, got.1);

    // Everyone signed up into the default workspace, so bob sees alice's new name.
    let name = format!("Alice {run_id}");
    let renamed = call(
        "PATCH",
        &format!("{users_url}/v1/me"),
        &alice.1,
        Some(serde_json::json!({"name": name})),
    )
    .await?;
    if renamed.0 != 200 {
        return Err(fail("alice renaming herself", &renamed));
    }
    let seen = call(
        "GET",
        &format!("{users_url}/v1/users?ids={},{}", alice.0, bob.0),
        &bob.1,
        None,
    )
    .await?;
    let names: Vec<&str> = seen.1["users"]
        .as_array()
        .map(|us| us.iter().filter_map(|u| u["name"].as_str()).collect())
        .unwrap_or_default();
    if seen.0 != 200 || !names.contains(&name.as_str()) {
        return Err(fail("bob reading alice's profile", &seen));
    }

    // A new workspace holds only alice: nobody else can be put into its channels.
    let created = call(
        "POST",
        &format!("{users_url}/v1/workspaces"),
        &alice.1,
        Some(serde_json::json!({"name": "E2E", "slug": format!("e2e-{run_id}")})),
    )
    .await?;
    let workspace = created.1["workspace_id"].as_str().map(str::to_string);
    let (201, Some(workspace)) = (created.0, workspace) else {
        return Err(fail("creating a workspace", &created));
    };
    let channels = format!("{channels_url}/v1/channels");
    let with_bob = call(
        "POST",
        &channels,
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": "ws", "workspace_id": workspace, "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if with_bob.0 != 400 {
        return Err(fail("a channel with an outsider as member", &with_bob));
    }
    let channel = call(
        "POST",
        &channels,
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": "ws", "workspace_id": workspace})),
    )
    .await?;
    let Some(channel_id) = channel.1["channel_id"]
        .as_str()
        .filter(|_| channel.0 == 201)
    else {
        return Err(fail("creating a channel in the new workspace", &channel));
    };
    let members = format!("{channels_url}/v1/channels/{channel_id}/members");
    let invite_mallory = serde_json::json!({"user_id": mallory.0.to_string()});
    let outsider = call("POST", &members, &alice.1, Some(invite_mallory.clone())).await?;
    if outsider.0 != 403 {
        return Err(fail("adding an outsider to the channel", &outsider));
    }
    let hidden = call(
        "GET",
        &format!("{users_url}/v1/workspaces/{workspace}/members"),
        &mallory.1,
        None,
    )
    .await?;
    if hidden.0 != 404 {
        return Err(fail("an outsider listing the members", &hidden));
    }

    // An invite brings mallory in; then the same add reaches the sequencer.
    let invite = call(
        "POST",
        &format!("{users_url}/v1/workspaces/{workspace}/invites"),
        &alice.1,
        None,
    )
    .await?;
    let Some(code) = invite.1["code"].as_str().filter(|_| invite.0 == 201) else {
        return Err(fail("creating an invite", &invite));
    };
    let accepted = call(
        "POST",
        &format!("{users_url}/v1/invites/{code}/accept"),
        &mallory.1,
        None,
    )
    .await?;
    if accepted.0 != 200 || accepted.1["status"] != "joined" {
        return Err(fail("mallory accepting the invite", &accepted));
    }
    let added = call("POST", &members, &alice.1, Some(invite_mallory)).await?;
    if added.0 != 200 && added.0 != 202 {
        return Err(fail(
            "adding mallory after she joined the workspace",
            &added,
        ));
    }
    println!(
        "users: a co-member sees the new name; an outsider can't join a workspace's channel (403) until invited, then can"
    );
    Ok(())
}

/// Sync reads (plan §6.6): the channel list carries the head, and the events after 0
/// are exactly what was delivered live: created, 3 messages, mallory's join and leave.
async fn check_history(
    channels_url: &str,
    messages_url: &str,
    channel: i64,
    users: &[(i64, String)],
) -> Result<(), String> {
    let (alice, mallory) = (&users[0], &users[2]);
    // The command verdicts come from the sequencer; the persister writes the head a moment later.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (status, list) = call(
            "GET",
            &format!("{channels_url}/v1/me/channels"),
            &alice.1,
            None,
        )
        .await?;
        let head = list["channels"]
            .as_array()
            .and_then(|cs| {
                cs.iter()
                    .find(|c| c["channel_id"].as_str() == Some(channel.to_string().as_str()))
            })
            .map(|c| c["event_seq"].clone());
        if status == 200 && head == Some(serde_json::json!("6")) {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "my channels: expected channel {channel} at event_seq 6, got {status} {list}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let events_url = format!("{messages_url}/v1/channels/{channel}/events?after=0");
    let http = reqwest::Client::new();
    let response = http
        .get(&events_url)
        .bearer_auth(&alice.1)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if response.status() != 200 {
        return Err(format!("history: {}", response.status()));
    }
    let bytes = response.bytes().await.map_err(|e| e.to_string())?;
    let page = xmsg_proto::v1::EventPage::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
    let kinds: Vec<String> = page
        .events
        .iter()
        .map(|e| match &e.payload {
            Some(sequenced_event::Payload::ChannelCreated(c)) => {
                format!("created:{}", c.name.starts_with("e2e-"))
            }
            Some(sequenced_event::Payload::MessageCreated(_)) => {
                format!("message:{}:{}", e.msg_seq, e.dedupe_id)
            }
            Some(sequenced_event::Payload::MemberJoined(_)) => "joined".into(),
            Some(sequenced_event::Payload::MemberLeft(_)) => "left".into(),
            other => format!("{other:?}"),
        })
        .collect();
    let expected = [
        "created:true",
        "message:1:a-1",
        "message:2:b-1",
        "message:3:a-2",
        "joined",
        "left",
    ];
    if kinds != expected || page.has_more || page.head_event_seq != 6 {
        return Err(format!(
            "history: expected {expected:?}, got {kinds:?} (has_more {}, head {})",
            page.has_more, page.head_event_seq
        ));
    }
    let refused = http
        .get(&events_url)
        .bearer_auth(&mallory.1)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .status();
    if refused != 404 {
        return Err(format!(
            "history for a member who left: expected 404, got {refused}"
        ));
    }
    println!(
        "history: my channels lists event_seq 6; events 1..6 rebuilt as delivered (with client_msg_ids); a former member is refused"
    );
    Ok(())
}

/// Membership commands through the channels service, decided by the sequencer (ADR-0006).
/// The channel has 4 events so far (created + 3 messages), so the next is event_seq 5.
async fn check_membership(url: &str, channel: i64, users: &[(i64, String)]) -> Result<(), String> {
    let members = format!("{url}/v1/channels/{channel}/members");
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let expect =
        |what: &str, got: (u16, serde_json::Value), status: u16, event_seq: Option<&str>| {
            if got.0 != status || (event_seq.is_some() && got.1["event_seq"].as_str() != event_seq)
            {
                return Err(format!(
                    "{what}: expected {status} {event_seq:?}, got {} {}",
                    got.0, got.1
                ));
            }
            Ok(())
        };
    let kick = call("DELETE", &format!("{members}/{}", alice.0), &bob.1, None).await?;
    expect("bob removing alice (bob isn't an admin)", kick, 403, None)?;
    let join = call("POST", &members, &mallory.1, None).await?;
    expect("mallory joining a private channel", join, 403, None)?;
    let invite = call(
        "POST",
        &members,
        &alice.1,
        Some(serde_json::json!({"user_id": mallory.0.to_string()})),
    )
    .await?;
    expect("alice inviting mallory", invite, 200, Some("5"))?;
    let leave = call(
        "DELETE",
        &format!("{members}/{}", mallory.0),
        &mallory.1,
        None,
    )
    .await?;
    expect("mallory leaving", leave, 200, Some("6"))?;
    println!(
        "membership via channels service: forbidden kick and join refused; invite (event_seq 5) and leave (6) accepted"
    );
    Ok(())
}

/// An authenticated JSON call; returns the status and body.
async fn call(
    method: &str,
    url: &str,
    token: &str,
    body: Option<serde_json::Value>,
) -> Result<(u16, serde_json::Value), String> {
    let http = reqwest::Client::new();
    let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| e.to_string())?;
    let mut request = http.request(method, url).bearer_auth(token);
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.map_err(|e| format!("{url}: {e}"))?;
    let status = response.status().as_u16();
    let body = response.json().await.unwrap_or(serde_json::Value::Null);
    Ok((status, body))
}

/// Without the channels service: produce `channel.created` straight to `messages.in`.
async fn produce_channel_created(
    bootstrap: &str,
    channel: i64,
    alice: i64,
    bob: i64,
) -> Result<(), String> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("partitioner", "murmur2_random")
        .create()
        .map_err(|e| e.to_string())?;
    let created = InboundEvent {
        channel_id: channel,
        workspace_id: 1,
        actor_id: alice,
        dedupe_id: format!("create-{channel}"),
        payload: Some(inbound_event::Payload::ChannelCreated(ChannelCreated {
            r#type: ChannelType::Private as i32,
            initial_member_ids: vec![alice, bob],
            ..Default::default()
        })),
        ..Default::default()
    };
    let key = xmsg_partition::channel_key(channel);
    producer
        .send(
            FutureRecord::to("messages.in")
                .key(&key[..])
                .payload(&created.encode_to_vec()),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| format!("producing channel.created: {e}"))?;
    Ok(())
}

/// What the persister wrote (plan §5.7): messages, membership and the head.
async fn check_store(nodes: &str, channel: i64, alice: i64, bob: i64) -> Result<(), String> {
    let db = scylla::client::session_builder::SessionBuilder::new()
        .known_nodes(nodes.split(','))
        .build()
        .await
        .map_err(|e| format!("connecting to {nodes}: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let messages: Vec<(i64, i64)> = db
            .query_unpaged(
                "SELECT msg_seq, author_id FROM xmsg.messages WHERE channel_id = ? AND bucket = 0",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .rows::<(i64, i64)>()
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let members: Vec<(i64,)> = db
            .query_unpaged(
                "SELECT user_id FROM xmsg.members_by_channel WHERE channel_id = ?",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .rows::<(i64,)>()
            .map_err(|e| e.to_string())?
            .collect::<Result<_, _>>()
            .map_err(|e| e.to_string())?;
        let head: Option<(i64, i64)> = db
            .query_unpaged(
                "SELECT event_seq, msg_seq FROM xmsg.channel_heads WHERE channel_id = ?",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .maybe_first_row::<(i64, i64)>()
            .map_err(|e| e.to_string())?;

        let mut member_ids: Vec<i64> = members.into_iter().map(|(u,)| u).collect();
        member_ids.sort();
        if messages == [(3, alice), (2, bob), (1, alice)]
            && member_ids == [alice, bob]
            && head == Some((4, 3))
        {
            println!("persister output: 3 messages, 2 members, head event_seq 4 / msg_seq 3");
            return Ok(());
        }
        if Instant::now() > deadline {
            return Err(format!(
                "store not as expected: messages {messages:?}, members {member_ids:?}, head {head:?} (is the persister running?)"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Signs a user up and logs in; returns (user ID, access token).
async fn sign_up_and_log_in(auth: &str, email: &str) -> Result<(i64, String), String> {
    let http = reqwest::Client::new();
    let password = "e2e smoke password";
    let post = |path: &'static str, body: serde_json::Value| {
        let request = http.post(format!("{auth}{path}")).json(&body);
        async move {
            let response = request.send().await.map_err(|e| format!("{path}: {e}"))?;
            let status = response.status();
            let body: serde_json::Value =
                response.json().await.map_err(|e| format!("{path}: {e}"))?;
            if status.is_success() {
                Ok(body)
            } else {
                Err(format!("{path}: {status} {body}"))
            }
        }
    };
    post(
        "/v1/signup",
        serde_json::json!({"email": email, "password": password, "name": "E2E"}),
    )
    .await?;
    let tokens = post(
        "/v1/login",
        serde_json::json!({"email": email, "password": password}),
    )
    .await?;
    let user_id = tokens["user_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("login: no user_id")?;
    let token = tokens["access_token"]
        .as_str()
        .ok_or("login: no access_token")?;
    Ok((user_id, token.to_string()))
}

/// The token with one character of its claims changed: the signature no longer matches.
fn forge(token: &str) -> String {
    let parts: Vec<&str> = token.split('.').collect();
    let claims = parts.get(1).copied().unwrap_or_default();
    // Flip one character of the payload; any change must break the signature.
    let mut tampered: Vec<char> = claims.chars().collect();
    if let Some(c) = tampered.get_mut(10) {
        *c = if *c == 'A' { 'B' } else { 'A' };
    }
    format!(
        "{}.{}.{}",
        parts[0],
        tampered.into_iter().collect::<String>(),
        parts.get(2).copied().unwrap_or_default()
    )
}

async fn expect_refused(url: &str, token: &str, reason: &str) -> Result<(), String> {
    let (mut ws, _) = connect_async(url)
        .await
        .map_err(|e| format!("connecting to {url}: {e}"))?;
    let frame = ClientFrame {
        frame: Some(client_frame::Frame::Connect(Connect {
            access_token: token.to_string(),
            protocol_version: 1,
            ..Default::default()
        })),
    };
    ws.send(Ws::Binary(frame.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        Ok(Some(Ok(Ws::Close(Some(close))))) if close.reason == reason => Ok(()),
        other => Err(format!("expected close \"{reason}\", got {other:?}")),
    }
}

/// Revocation (plan §6.4): logging out closes the session's open socket within seconds,
/// and its access token, still cryptographically valid for ~15 minutes, is refused.
async fn check_revocation(auth: &str, gateway: &str, run_id: i64) -> Result<(), String> {
    let http = reqwest::Client::new();
    let email = format!("dora-{run_id}@example.org");
    let password = "e2e smoke password";
    let post = async |path: &str, body: serde_json::Value| -> Result<serde_json::Value, String> {
        let response = http
            .post(format!("{auth}{path}"))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{path}: {e}"))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!("{path}: {status} {text}"));
        }
        Ok(serde_json::from_str(&text).unwrap_or(serde_json::Value::Null))
    };
    post(
        "/v1/signup",
        serde_json::json!({"email": email, "password": password, "name": "Dora"}),
    )
    .await?;
    let tokens = post(
        "/v1/login",
        serde_json::json!({"email": email, "password": password}),
    )
    .await?;
    let access = tokens["access_token"]
        .as_str()
        .ok_or("login: no access_token")?;
    let refresh = tokens["refresh_token"]
        .as_str()
        .ok_or("login: no refresh_token")?;

    let mut client = connect(gateway, access).await?;
    let started = Instant::now();
    post("/v1/logout", serde_json::json!({"refresh_token": refresh})).await?;
    match tokio::time::timeout(Duration::from_secs(5), client.ws.next()).await {
        Ok(Some(Ok(Ws::Close(Some(close))))) if close.reason == "session revoked" => {}
        other => {
            return Err(format!(
                "after logout: expected close \"session revoked\" within 5 s, got {other:?}"
            ));
        }
    }
    let closed_after = started.elapsed();
    expect_refused(gateway, access, "session revoked").await?;
    println!(
        "revocation: logout closed the open socket in {} ms; its still-valid access token is refused",
        closed_after.as_millis()
    );
    Ok(())
}

async fn connect(url: &str, token: &str) -> Result<Client, String> {
    let (mut ws, _) = connect_async(url)
        .await
        .map_err(|e| format!("connecting to {url}: {e}"))?;
    let frame = ClientFrame {
        frame: Some(client_frame::Frame::Connect(Connect {
            access_token: token.to_string(),
            protocol_version: 1,
            ..Default::default()
        })),
    };
    ws.send(Ws::Binary(frame.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    let mut client = Client {
        ws,
        events: Vec::new(),
    };
    match next_frame(&mut client).await? {
        server_frame::Frame::Welcome(_) => Ok(client),
        other => Err(format!("expected Welcome, got {other:?}")),
    }
}

async fn send(ws: &mut Socket, channel: i64, client_msg_id: &str) -> Result<(), String> {
    let frame = ClientFrame {
        frame: Some(client_frame::Frame::SendMessage(SendMessageRequest {
            channel_id: channel,
            client_msg_id: client_msg_id.into(),
            body: format!("hello from {client_msg_id}"),
        })),
    };
    ws.send(Ws::Binary(frame.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())
}

/// Waits for the SendAck; channel events arriving meanwhile are kept.
async fn expect_ack(client: &mut Client, client_msg_id: &str) -> Result<(), String> {
    loop {
        match next_frame(client).await? {
            server_frame::Frame::Ack(ack) if ack.client_msg_id == client_msg_id => return Ok(()),
            server_frame::Frame::Event(_) => {} // kept by next_frame
            other => {
                return Err(format!(
                    "expected SendAck for {client_msg_id}, got {other:?}"
                ));
            }
        }
    }
}

/// Reads frames until `done` holds for the events received, or 20 s pass.
async fn wait_events(
    client: &mut Client,
    done: impl Fn(&[SequencedEvent]) -> bool,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while !done(&client.events) {
        if tokio::time::Instant::now() > deadline {
            return Ok(()); // the caller reports what arrived
        }
        let _ = tokio::time::timeout(Duration::from_millis(500), next_frame(client)).await;
    }
    Ok(())
}

/// Fan-out: members receive every message in order; the non-member only its rejection.
async fn check_delivery(clients: &mut [Client; 3], channel: i64) -> Result<(), String> {
    let messages = |events: &[SequencedEvent]| -> Vec<(i64, String)> {
        let mut seen = Vec::new();
        for e in events.iter().filter(|e| e.channel_id == channel) {
            if let Some(sequenced_event::Payload::MessageCreated(_)) = e.payload {
                // Clients dedupe by event_seq (at-least-once delivery).
                if !seen.iter().any(|(s, _): &(i64, String)| *s == e.msg_seq) {
                    seen.push((e.msg_seq, e.dedupe_id.clone()));
                }
            }
        }
        seen
    };
    let expected: Vec<(i64, String)> =
        vec![(1, "a-1".into()), (2, "b-1".into()), (3, "a-2".into())];
    for (i, name) in [(0, "alice"), (1, "bob")] {
        wait_events(&mut clients[i], |e| messages(e).len() >= 3).await?;
        let got = messages(&clients[i].events);
        if got != expected {
            return Err(format!("{name} received {got:?}, expected {expected:?}"));
        }
    }
    let mallory = &mut clients[2];
    wait_events(mallory, |e| e.iter().any(|e| e.dedupe_id == "m-1")).await?;
    let got = messages(&mallory.events);
    let rejected = mallory.events.iter().any(|e| {
        e.dedupe_id == "m-1"
            && matches!(&e.payload, Some(sequenced_event::Payload::Rejected(r)) if r.reason == RejectReason::NotAMember as i32)
    });
    if !got.is_empty() || !rejected {
        return Err(format!(
            "non-member got messages {got:?}, rejection delivered: {rejected}"
        ));
    }
    println!(
        "fan-out delivered msg_seq 1..3 to both members over WebSocket; the non-member got only its rejection"
    );
    Ok(())
}

async fn next_frame(client: &mut Client) -> Result<server_frame::Frame, String> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let msg = tokio::time::timeout_at(deadline, client.ws.next())
            .await
            .map_err(|_| "timed out waiting for a server frame".to_string())?
            .ok_or("socket closed")?
            .map_err(|e| e.to_string())?;
        if let Ws::Binary(bytes) = msg {
            let frame = ServerFrame::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
            let frame = frame
                .frame
                .ok_or_else(|| "empty server frame".to_string())?;
            if let server_frame::Frame::Event(e) = &frame {
                client.events.push(e.clone());
            }
            return Ok(frame);
        }
    }
}

async fn read_persisted(
    bootstrap: &str,
    channel: i64,
    expected: usize,
) -> Result<Vec<SequencedEvent>, String> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("e2e-smoke-{channel}"))
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()
        .map_err(|e| e.to_string())?;
    consumer
        .subscribe(&["messages.persisted"])
        .map_err(|e| e.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut events = Vec::new();
    while events.len() < expected && Instant::now() < deadline {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(500))
            && let Ok(e) = SequencedEvent::decode(m.payload().unwrap_or_default())
            && e.channel_id == channel
        {
            events.push(e);
        }
    }
    if events.len() < expected {
        return Err(format!(
            "only {} of {expected} events for channel {channel} in messages.persisted (is the sequencer running?)",
            events.len()
        ));
    }
    Ok(events)
}
