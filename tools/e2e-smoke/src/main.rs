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

mod upgrade;

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
    Activity, ChannelCreated, ChannelType, ClientFrame, Connect, InboundEvent, PresenceStatus,
    PresenceUpdate, ReadReceipt, Receipt, RejectReason, SendMessageRequest, SequencedEvent,
    ServerFrame, ThreadRoot, Typing, TypingIndicator, WatchPresence, client_frame, inbound_event,
    sequenced_event, server_frame,
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
    // `upgrade-seed FILE` / `upgrade-verify FILE`: the upgrade test (see upgrade.rs).
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.iter().map(String::as_str).collect::<Vec<_>>()[..] {
        [] => run().await,
        ["upgrade-seed", file] => upgrade::seed(file).await,
        ["upgrade-verify", file] => upgrade::verify(file).await,
        _ => Err("usage: xmsg-e2e-smoke [upgrade-seed FILE | upgrade-verify FILE]".into()),
    };
    let what = args.first().map_or("end-to-end smoke test", String::as_str);
    match result {
        Ok(()) => {
            println!("✅ {what} passed");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("❌ {what} failed: {e}");
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
    let users_url = std::env::var("XMSG_USERS_URL").ok();
    // Sign-up policy (ADR-0026): the first account needs no invite and owns the
    // workspace; after that sign-up is invite-only by default. A fresh install gets
    // its users through owner-created invites (genuinely exercising the 403 once);
    // an existing install must be `open` for us to create accounts at all.
    let (users, fresh_install) = match std::env::var("XMSG_AUTH_URL") {
        Ok(auth) => {
            let (_, policy) = call("GET", &format!("{auth}/v1/signup-policy"), "", None).await?;
            let first_account = policy["first_account"] == true;
            let invite_only = policy["invite_only"] == true;
            let mut users = Vec::new();
            users.push(
                sign_up_and_log_in(&auth, &format!("alice-{run_id}@example.org"), None).await?,
            );
            if first_account {
                let refused =
                    sign_up(&auth, &format!("refused-{run_id}@example.org"), None).await?;
                if refused != 403 {
                    return Err(format!(
                        "invite-only sign-up refusal: expected 403, got {refused}"
                    ));
                }
                println!("invite-only sign-up was refused with 403, as it should be");
                let users_url = users_url
                    .as_deref()
                    .ok_or("XMSG_USERS_URL is needed to invite the other users on a fresh, invite-only install")?;
                for name in ["bob", "mallory"] {
                    let (_, invite) = call(
                        "POST",
                        &format!("{users_url}/v1/workspaces/1/invites"),
                        &users[0].1,
                        None,
                    )
                    .await?;
                    let code = invite["code"]
                        .as_str()
                        .ok_or("creating an invite: no code")?;
                    users.push(
                        sign_up_and_log_in(
                            &auth,
                            &format!("{name}-{run_id}@example.org"),
                            Some(code),
                        )
                        .await?,
                    );
                }
            } else if invite_only {
                return Err(
                    "the install is invite-only and not fresh: create accounts for the smoke test, or set the sign-up policy to open"
                        .to_string(),
                );
            } else {
                for name in ["bob", "mallory"] {
                    users.push(
                        sign_up_and_log_in(&auth, &format!("{name}-{run_id}@example.org"), None)
                            .await?,
                    );
                }
            }
            expect_refused(&gateway, &forge(&users[0].1), "invalid token").await?;
            println!("signed up and logged in 3 users via {auth}; a forged token was refused");
            (users, first_account)
        }
        Err(_) => (
            [1_000, 2_000, 3_000]
                .map(|base| {
                    (
                        base + run_id % 1_000,
                        format!("dev:{}", base + run_id % 1_000),
                    )
                })
                .to_vec(),
            false,
        ),
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
    check_typing(&mut clients, channel, &users).await?;
    check_presence(&mut clients, &users).await?;

    if let Ok(nodes) = std::env::var("XMSG_CQL_NODES") {
        check_store(&nodes, channel, alice, bob).await?;
    }
    if let Some(url) = &channels_url {
        check_membership(url, channel, &users).await?;
        if let Ok(messages_url) = std::env::var("XMSG_MESSAGES_URL") {
            check_history(url, &messages_url, channel, &users).await?;
        }
        if let Ok(readstate_url) = std::env::var("XMSG_READSTATE_URL") {
            check_readstate(&readstate_url, channel, &users).await?;
        }
        check_settings(url, channel, &users).await?;
        if let Ok(sequencer_url) = std::env::var("XMSG_SEQUENCER_URL") {
            check_membership_endpoint(&sequencer_url, channel, &users).await?;
        }
        if let Ok(messages_url) = std::env::var("XMSG_MESSAGES_URL") {
            check_edits(url, &messages_url, &mut clients, &users, run_id).await?;
        }
        if let Ok(readstate_url) = std::env::var("XMSG_READSTATE_URL") {
            check_mentions(url, &readstate_url, &mut clients, &users, run_id).await?;
        }
        if let Ok(messages_url) = std::env::var("XMSG_MESSAGES_URL") {
            check_reactions(url, &messages_url, &mut clients, &users, run_id).await?;
            check_threads(url, &messages_url, &mut clients, &users, run_id).await?;
            check_receipts(url, &mut clients, &users, channel).await?;
            check_search(&messages_url, &mut clients, &users, channel, run_id).await?;
            if let Ok(push_url) = std::env::var("XMSG_PUSH_URL") {
                check_push(&push_url, url, &mut clients, &users, run_id).await?;
            }
        }
        if let (Ok(calls_url), Ok(livekit_url), Ok(secret)) = (
            std::env::var("XMSG_CALLS_URL"),
            std::env::var("XMSG_LIVEKIT_API_URL"),
            std::env::var("XMSG_LIVEKIT_API_SECRET"),
        ) {
            check_calls(
                &calls_url,
                &livekit_url,
                &secret,
                url,
                &mut clients,
                &users,
                run_id,
            )
            .await?;
        }
        if let Ok(media_url) = std::env::var("XMSG_MEDIA_URL") {
            let messages_url = std::env::var("XMSG_MESSAGES_URL").ok();
            check_media(
                &media_url,
                url,
                messages_url.as_deref(),
                &mut clients,
                &users,
                run_id,
            )
            .await?;
        }
        if let Ok(users_url) = std::env::var("XMSG_USERS_URL") {
            let (workspace, ws_channel) =
                check_workspaces(&users_url, url, &users, run_id, fresh_install).await?;
            if std::env::var("XMSG_EXPECT_WORKSPACE_REMOVAL").as_deref() == Ok("true") {
                let messages_url = std::env::var("XMSG_MESSAGES_URL")
                    .map_err(|_| "XMSG_EXPECT_WORKSPACE_REMOVAL needs XMSG_MESSAGES_URL")?;
                check_workspace_removal(
                    &users_url,
                    &messages_url,
                    &gateway,
                    &users,
                    &workspace,
                    &ws_channel,
                )
                .await?;
            }
        }
        if std::env::var("XMSG_EXPECT_OUTBOX_RELAY").as_deref() == Ok("true") {
            check_outbox_copy(&bootstrap, channel)?;
        }
    }
    if let Ok(auth) = std::env::var("XMSG_AUTH_URL")
        && std::env::var("XMSG_EXPECT_REVOCATION").as_deref() == Ok("true")
    {
        check_revocation(&auth, &gateway, run_id, users.first(), users_url.as_deref()).await?;
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
    // Whether this run created the first account: only then does alice own the
    // default workspace (ADR-0026); with development tokens there is no owner either.
    fresh_install: bool,
) -> Result<(String, String), String> {
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

    // Default channels (ADR-0026): on a fresh install alice signed up first, so she
    // owns the default workspace and runs setup; bob joins the defaults on first
    // sign-in. On an existing install we have no owner rights, so the management
    // steps are skipped — but a member's idempotent default-channel join always runs.
    if fresh_install {
        let listed = call(
            "GET",
            &format!("{users_url}/v1/me/workspaces"),
            &alice.1,
            None,
        )
        .await?;
        let default_ws = listed.1["workspaces"]
            .as_array()
            .and_then(|ws| {
                ws.iter()
                    .find(|w| w["role"] == "owner" && w["setup_completed"] == false)
            })
            .and_then(|w| w["workspace_id"].as_str().map(str::to_string));
        let Some(default_ws) = default_ws.filter(|_| listed.0 == 200) else {
            return Err(fail(
                "finding alice's owner workspace awaiting setup",
                &listed,
            ));
        };
        let completed = call(
            "PATCH",
            &format!("{users_url}/v1/workspaces/{default_ws}"),
            &alice.1,
            Some(serde_json::json!({"name": "Home", "setup_completed": true})),
        )
        .await?;
        if completed.0 != 200 || completed.1["setup_completed"] != true {
            return Err(fail("alice completing setup", &completed));
        }

        let general = call(
            "POST",
            &format!("{channels_url}/v1/channels"),
            &alice.1,
            Some(serde_json::json!({"type": "public", "name": format!("general-{run_id}")})),
        )
        .await?;
        let Some(general_id) = general.1["channel_id"]
            .as_str()
            .filter(|_| general.0 == 201)
            .map(str::to_string)
        else {
            return Err(fail("creating the general channel", &general));
        };
        let defaults = format!("{channels_url}/v1/workspaces/{default_ws}/default-channels");
        let added = call(
            "POST",
            &defaults,
            &alice.1,
            Some(serde_json::json!({"channel_id": general_id})),
        )
        .await?;
        if added.0 != 204 {
            return Err(fail("alice making general a default channel", &added));
        }
        let join = call(
            "POST",
            &format!("{channels_url}/v1/me/default-channels/join"),
            &bob.1,
            Some(serde_json::json!({"workspace_id": default_ws})),
        )
        .await?;
        if join.0 != 200 || join.1["joined"] != serde_json::json!([general_id]) {
            return Err(fail("bob joining the defaults on first sign-in", &join));
        }
        let again = call(
            "POST",
            &format!("{channels_url}/v1/me/default-channels/join"),
            &bob.1,
            Some(serde_json::json!({"workspace_id": default_ws})),
        )
        .await?;
        if again.0 != 200 || again.1["joined"] != serde_json::json!([]) {
            return Err(fail("bob's default-channel join being idempotent", &again));
        }
        println!(
            "users: the first account owns the workspace, completes setup, and a member joins the default channel once"
        );
    } else {
        println!("not a fresh install: skipping the first-run owner checks");
        for token in [&alice.1, &bob.1] {
            let join = call(
                "POST",
                &format!("{channels_url}/v1/me/default-channels/join"),
                token,
                Some(serde_json::json!({"workspace_id": "1"})),
            )
            .await?;
            if join.0 != 200 {
                return Err(fail("a member joining the default channels", &join));
            }
            let again = call(
                "POST",
                &format!("{channels_url}/v1/me/default-channels/join"),
                token,
                Some(serde_json::json!({"workspace_id": "1"})),
            )
            .await?;
            if again.0 != 200 || again.1["joined"] != serde_json::json!([]) {
                return Err(fail("the default-channel join being idempotent", &again));
            }
        }
        println!("users: a member's default-channel join works and is idempotent");
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
    Ok((workspace, channel_id.to_string()))
}

/// Workspace removal (plan §6.5), continuing from `check_workspaces`: mallory is in the
/// new workspace and its channel. Once alice removes her, nothing of the channel reaches
/// her socket any more (fan-out's deny list, immediately), and the removal job takes her
/// out of the channel (history refuses her; the channel's history shows her leaving).
async fn check_workspace_removal(
    users_url: &str,
    messages_url: &str,
    gateway: &str,
    users: &[(i64, String)],
    workspace: &str,
    channel: &str,
) -> Result<(), String> {
    let (alice, mallory) = (&users[0], &users[2]);
    let channel_id: i64 = channel.parse().map_err(|_| "bad channel id")?;
    let mut alice_ws = connect(gateway, &alice.1).await?;
    let mut mallory_ws = connect(gateway, &mallory.1).await?;

    let removed = call(
        "DELETE",
        &format!(
            "{users_url}/v1/workspaces/{workspace}/members/{}",
            mallory.0
        ),
        &alice.1,
        None,
    )
    .await?;
    if removed.0 != 204 {
        return Err(format!(
            "removing mallory from the workspace: {} {}",
            removed.0, removed.1
        ));
    }
    let started = Instant::now();
    send(&mut alice_ws.ws, channel_id, "after-removal").await?;
    expect_ack(&mut alice_ws, "after-removal").await?;
    wait_events(&mut alice_ws, |e| {
        e.iter().any(|e| e.dedupe_id == "after-removal")
    })
    .await?;
    // Whatever mallory's socket receives for the next 3 s must contain nothing of the
    // channel sequenced after the removal. Events sequenced before it (her own join, say)
    // may still arrive late on this new socket; they aren't leaks.
    let cutoff = alice_ws
        .events
        .iter()
        .find(|e| e.dedupe_id == "after-removal")
        .map(|e| e.event_seq)
        .ok_or("alice's message after the removal never arrived")?;
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let _ = tokio::time::timeout(Duration::from_millis(300), next_frame(&mut mallory_ws)).await;
    }
    if let Some(leak) = mallory_ws
        .events
        .iter()
        .find(|e| e.channel_id == channel_id && e.event_seq >= cutoff)
    {
        return Err(format!(
            "removed member received {:?} of the workspace's channel",
            leak.payload
        ));
    }

    // The job takes her out of the channel: history refuses her, and shows her leaving.
    let http = reqwest::Client::new();
    let history = format!("{messages_url}/v1/channels/{channel_id}/events?after=0");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = http
            .get(&history)
            .bearer_auth(&mallory.1)
            .send()
            .await
            .map_err(|e| e.to_string())?
            .status();
        if status == 404 {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "30 s after the removal mallory still reads the channel's history ({status})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let left_after = started.elapsed();
    let bytes = http
        .get(&history)
        .bearer_auth(&alice.1)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .bytes()
        .await
        .map_err(|e| e.to_string())?;
    let page = xmsg_proto::v1::EventPage::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
    let left = page.events.iter().any(|e| {
        matches!(&e.payload, Some(sequenced_event::Payload::MemberLeft(l)) if l.user_id == mallory.0)
    });
    if !left {
        return Err("the channel's history doesn't show mallory leaving".into());
    }
    println!(
        "workspace removal: the removed member got nothing of the workspace's channel; the job took her out of it in {} ms",
        left_after.as_millis()
    );
    Ok(())
}

/// The sequencer's membership endpoint (plan §5.6): after mallory's join and leave the
/// channel has alice and bob again, at the version of the leave (event 6).
async fn check_membership_endpoint(
    url: &str,
    channel: i64,
    users: &[(i64, String)],
) -> Result<(), String> {
    let response = reqwest::get(format!("{url}/v1/channels/{channel}/members"))
        .await
        .map_err(|e| e.to_string())?;
    if response.status() != 200 {
        return Err(format!("membership endpoint: {}", response.status()));
    }
    let bytes = response.bytes().await.map_err(|e| e.to_string())?;
    let members =
        xmsg_proto::v1::ChannelMembers::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
    let mut expected = vec![users[0].0, users[1].0];
    expected.sort();
    if members.user_ids != expected || members.membership_version != 6 {
        return Err(format!(
            "membership endpoint: expected {expected:?} at version 6, got {:?} at {}",
            members.user_ids, members.membership_version
        ));
    }
    let unknown = reqwest::get(format!("{url}/v1/channels/1/members"))
        .await
        .map_err(|e| e.to_string())?
        .status();
    if unknown != 404 {
        return Err(format!(
            "membership endpoint, unknown channel: expected 404, got {unknown}"
        ));
    }
    println!(
        "membership endpoint: the sequencer serves alice and bob at version 6; an unknown channel is 404"
    );
    Ok(())
}

/// One request a stand-in push service received.
#[derive(Debug, Clone)]
struct PushRequest {
    path: String,
    headers: Vec<(String, String)>,
    body_len: usize,
}

/// A stand-in push service (what a browser vendor runs): records each POST and answers 201.
/// How the push service reaches the stand-in push endpoint: `XMSG_PUSH_MOCK_HOST`, e.g.
/// `host.docker.internal` when the push service runs in a container.
fn push_mock_host() -> String {
    std::env::var("XMSG_PUSH_MOCK_HOST").unwrap_or_else(|_| "127.0.0.1".into())
}

async fn mock_push_service()
-> Result<(u16, std::sync::Arc<std::sync::Mutex<Vec<PushRequest>>>), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Reachable from containers too when they reach this machine by another name.
    let bind = if push_mock_host() == "127.0.0.1" {
        "127.0.0.1:0"
    } else {
        "0.0.0.0:0"
    };
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .map_err(|e| e.to_string())?;
    let port = listener.local_addr().map_err(|e| e.to_string())?.port();
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = seen.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 8192];
                // Headers, then Content-Length bytes of body.
                let (head_end, length) = loop {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                    if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..i]).to_string();
                        let length = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length:")
                                    .map(|v| v.trim().parse::<usize>().unwrap_or(0))
                            })
                            .unwrap_or(0);
                        break (i + 4, length);
                    }
                };
                while buf.len() < head_end + length {
                    let Ok(n) = socket.read(&mut chunk).await else {
                        return;
                    };
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let mut lines = head.lines();
                let path = lines
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("")
                    .to_string();
                let headers = lines
                    .filter_map(|l| {
                        l.split_once(':')
                            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
                    })
                    .collect();
                log.lock().unwrap().push(PushRequest {
                    path,
                    headers,
                    body_len: buf.len() - head_end,
                });
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 201 Created\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
                    )
                    .await;
            });
        }
    });
    Ok((port, seen))
}

/// Web Push (plan §5.11): bob subscribes with a stand-in push service. While he's active
/// in the app, alice's message pushes nothing; once he's idle, it arrives encrypted
/// (aes128gcm) and VAPID-signed.
async fn check_push(
    push_url: &str,
    channels_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    let (alice, bob) = (&users[0], &users[1]);
    let (port, seen) = mock_push_service().await?;
    let (status, key) = call(
        "GET",
        &format!("{push_url}/v1/push/vapid-public-key"),
        &bob.1,
        None,
    )
    .await?;
    if status != 200 || key["key"].as_str().is_none_or(str::is_empty) {
        return Err(format!("push: VAPID key: {status} {key}"));
    }
    // The browser key and auth secret from RFC 8291's example (Appendix A).
    let subscription = serde_json::json!({
        "endpoint": format!("http://{}:{port}/push/bob-{run_id}", push_mock_host()),
        "keys": {
            "p256dh": "BCVxsr7N_eNgVRqvHtD0zTZsEc6-VV-JvLexhqUzORcxaOzi6-AYWXvTBHm4bjyPjs7Vd8pZGH6SRpkNtoIAiw4",
            "auth": "BTBZMqHH6r4Tts7J_aSIgg"
        }
    });
    let (status, reply) = call(
        "POST",
        &format!("{push_url}/v1/push/subscriptions"),
        &bob.1,
        Some(subscription),
    )
    .await?;
    if status != 201 {
        return Err(format!("push: subscribing: {status} {reply}"));
    }
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-push-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("push: creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("push: no channel_id")?;
    // Wait for the channel to reach bob, so membership is stored before the first message.
    let created = |events: &[SequencedEvent]| events.iter().any(|e| e.channel_id == channel);
    wait_events(&mut clients[1], created).await?;

    // bob is active in the app: nothing is pushed.
    send(&mut clients[0].ws, channel, "push-while-active").await?;
    expect_ack(&mut clients[0], "push-while-active").await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    if !seen.lock().unwrap().is_empty() {
        return Err(format!(
            "push: pushed while bob was active: {:?}",
            seen.lock().unwrap()
        ));
    }

    // bob goes idle (away): the next message is pushed.
    let idle = ClientFrame {
        frame: Some(client_frame::Frame::Activity(Activity { active: false })),
    };
    clients[1]
        .ws
        .send(Ws::Binary(idle.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    tokio::time::sleep(Duration::from_secs(1)).await;
    send(&mut clients[0].ws, channel, "push-while-away").await?;
    expect_ack(&mut clients[0], "push-while-away").await?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let got = loop {
        if let Some(r) = seen.lock().unwrap().first().cloned() {
            break r;
        }
        if Instant::now() > deadline {
            return Err("push: nothing reached bob's push service while he was away".into());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let header = |k: &str| {
        got.headers
            .iter()
            .find(|(name, _)| name == k)
            .map(|(_, v)| v.clone())
            .unwrap_or_default()
    };
    if got.path != format!("/push/bob-{run_id}")
        || header("content-encoding") != "aes128gcm"
        || !header("authorization").starts_with("vapid t=")
        || header("ttl").is_empty()
        || got.body_len == 0
    {
        return Err(format!("push: unexpected request {got:?}"));
    }
    // Back to active, as the other checks expect.
    let active = ClientFrame {
        frame: Some(client_frame::Frame::Activity(Activity { active: true })),
    };
    clients[1]
        .ws
        .send(Ws::Binary(active.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    println!(
        "push: nothing while bob is active; once away, alice's message reached his push service encrypted (aes128gcm, {} bytes) and VAPID-signed",
        got.body_len
    );
    Ok(())
}

/// Search (plan §5.12): a member finds a message in their channel; someone who left the
/// channel (mallory) doesn't, because search is scoped to the caller's channels.
async fn check_search(
    messages_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    channel: i64,
    run_id: i64,
) -> Result<(), String> {
    let (bob, mallory) = (&users[1], &users[2]);
    let word = format!("zebra{run_id}");
    send_body(
        &mut clients[0].ws,
        channel,
        "search-me",
        &format!("the {word} crossed the road"),
    )
    .await?;
    expect_ack(&mut clients[0], "search-me").await?;
    let found = async |token: &str| -> Result<Vec<String>, String> {
        let (status, reply) = call(
            "GET",
            &format!("{messages_url}/v1/search?q={word}"),
            token,
            None,
        )
        .await?;
        if status != 200 {
            return Err(format!("search: GET /v1/search: {status} {reply}"));
        }
        Ok(reply["results"]
            .as_array()
            .map(|r| {
                r.iter()
                    .filter_map(|x| x["body"].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default())
    };
    // Indexing is asynchronous (the indexer flushes every second, OpenSearch refreshes
    // every second), and the search backend can still be starting: retry every
    // answer that isn't one hit, 200 included, until the deadline.
    let started = Instant::now();
    let deadline = started + Duration::from_secs(30);
    loop {
        let last = match found(&bob.1).await {
            Ok(hits) if hits.len() == 1 && hits[0].contains(&word) => break,
            Ok(hits) => format!("bob expected 1 hit for {word}, got {hits:?}"),
            Err(e) => e,
        };
        if Instant::now() > deadline {
            return Err(format!("search: {last}"));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let searchable_after = started.elapsed();
    let leaked = found(&mallory.1).await?;
    if !leaked.is_empty() {
        return Err(format!(
            "search: mallory (not in the channel) found {leaked:?}"
        ));
    }
    println!(
        "search: bob finds a message in his channel ({} ms after sending); mallory, who left it, finds nothing",
        searchable_after.as_millis()
    );
    Ok(())
}

/// Read receipts (ADR-0012): in a DM, bob's receipt reaches alice; in a channel, nobody's.
async fn check_receipts(
    channels_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    channel: i64,
) -> Result<(), String> {
    use sequenced_event::Payload;
    let (alice, bob) = (&users[0], &users[1]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/dms"),
        &alice.1,
        Some(serde_json::json!({"user_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 200 && status != 201 {
        return Err(format!("receipts: opening the DM: {status} {reply}"));
    }
    let dm: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("receipts: no channel_id")?;
    // A message, so the DM is live in fan-out (it learns the channel's type from events).
    send(&mut clients[0].ws, dm, "dm-hello").await?;
    expect_ack(&mut clients[0], "dm-hello").await?;
    let sent = |events: &[SequencedEvent]| {
        events
            .iter()
            .find(|e| {
                e.channel_id == dm
                    && e.dedupe_id == "dm-hello"
                    && matches!(e.payload, Some(Payload::MessageCreated(_)))
            })
            .map(|e| e.msg_seq)
    };
    wait_events(&mut clients[1], |e| sent(e).is_some()).await?;
    let msg_seq = sent(&clients[1].events).ok_or("receipts: bob didn't get the DM")?;
    async fn receipt_seen(client: &mut Client, within: Duration) -> Option<ReadReceipt> {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, next_frame(client)).await {
                Ok(Ok(server_frame::Frame::Receipt(r))) => return Some(r),
                Ok(Ok(_)) => {}
                _ => break,
            }
        }
        None
    }
    let receipt = |channel_id: i64, msg_seq: i64| {
        ClientFrame {
            frame: Some(client_frame::Frame::Receipt(Receipt {
                channel_id,
                msg_seq,
            })),
        }
        .encode_to_vec()
    };
    let [alice_ws, bob_ws, _] = clients;
    // In a channel (not a DM): dropped.
    bob_ws
        .ws
        .send(Ws::Binary(receipt(channel, 1).into()))
        .await
        .map_err(|e| e.to_string())?;
    if let Some(r) = receipt_seen(alice_ws, Duration::from_secs(2)).await {
        return Err(format!("receipts: a channel receipt reached alice: {r:?}"));
    }
    bob_ws
        .ws
        .send(Ws::Binary(receipt(dm, msg_seq).into()))
        .await
        .map_err(|e| e.to_string())?;
    let got = receipt_seen(alice_ws, Duration::from_secs(10)).await;
    let want = ReadReceipt {
        channel_id: dm,
        user_id: bob.0,
        msg_seq,
    };
    if got.as_ref() != Some(&want) {
        return Err(format!("receipts: alice expected {want:?}, got {got:?}"));
    }
    println!("receipts: bob's DM receipt reached alice; a receipt in a channel reached nobody");
    Ok(())
}

/// Threads (ADR-0005), in a channel of their own: a reply takes a reply_seq and no
/// msg_seq, reaches the members live, and is listed by the thread API; a reply naming the
/// wrong root id is refused.
async fn check_threads(
    channels_url: &str,
    messages_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    use sequenced_event::Payload;
    let (alice, bob) = (&users[0], &users[1]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-threads-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("threads: creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("threads: no channel_id")?;
    send(&mut clients[0].ws, channel, "thread-root").await?;
    expect_ack(&mut clients[0], "thread-root").await?;
    let root = |events: &[SequencedEvent]| {
        events.iter().find_map(|e| match &e.payload {
            Some(Payload::MessageCreated(m))
                if e.channel_id == channel && e.dedupe_id == "thread-root" =>
            {
                Some((e.msg_seq, m.message_id))
            }
            _ => None,
        })
    };
    wait_events(&mut clients[1], |e| root(e).is_some()).await?;
    let (root_seq, root_id) = root(&clients[1].events).ok_or("threads: bob didn't get the root")?;
    let reply_frame = |id: &str, root_message_id: i64| ClientFrame {
        frame: Some(client_frame::Frame::SendMessage(SendMessageRequest {
            channel_id: channel,
            client_msg_id: id.into(),
            body: format!("reply {id}"),
            thread_root: Some(ThreadRoot {
                msg_seq: root_seq,
                message_id: root_message_id,
            }),
            ..Default::default()
        })),
    };
    for (id, root_message_id) in [("thread-reply", root_id), ("thread-bad", root_id + 1)] {
        clients[1]
            .ws
            .send(Ws::Binary(
                reply_frame(id, root_message_id).encode_to_vec().into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
        expect_ack(&mut clients[1], id).await?;
    }
    let replied = |events: &[SequencedEvent]| {
        events.iter().find_map(|e| match &e.payload {
            Some(Payload::ThreadReply(r))
                if e.channel_id == channel && e.dedupe_id == "thread-reply" =>
            {
                Some((e.msg_seq, r.root_msg_seq, r.reply_seq, r.reply_count))
            }
            _ => None,
        })
    };
    wait_events(&mut clients[0], |e| replied(e).is_some()).await?;
    match replied(&clients[0].events) {
        Some((0, r, 1, 1)) if r == root_seq => {}
        other => {
            return Err(format!(
                "threads: alice expected reply 1 of root {root_seq} with no msg_seq, got {other:?}"
            ));
        }
    }
    let refused = |events: &[SequencedEvent]| {
        events.iter().any(|e| {
            e.dedupe_id == "thread-bad"
                && matches!(&e.payload, Some(Payload::Rejected(r)) if r.reason == RejectReason::Invalid as i32)
        })
    };
    wait_events(&mut clients[1], refused).await?;
    if !refused(&clients[1].events) {
        return Err("threads: a reply naming the wrong root id wasn't refused".into());
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, page) = call(
            "GET",
            &format!("{messages_url}/v1/channels/{channel}/threads/{root_seq}"),
            &alice.1,
            None,
        )
        .await?;
        let bodies: Vec<&str> = page["replies"]
            .as_array()
            .map(|r| r.iter().filter_map(|x| x["body"].as_str()).collect())
            .unwrap_or_default();
        if status == 200 && bodies == ["reply thread-reply"] {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!("threads: the thread API shows {status} {page}"));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!(
        "threads: bob's reply reached alice as reply 1 with no msg_seq; the thread API lists it; a wrong root id is refused"
    );
    Ok(())
}

/// Reactions (ADR-0015), in a channel of their own: through `reactions.in`, the
/// reaction-batcher and the sequencer, which keeps each user's latest operation.
async fn check_reactions(
    channels_url: &str,
    messages_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    use sequenced_event::Payload;
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-reactions-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("reactions: creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("reactions: no channel_id")?;
    send(&mut clients[0].ws, channel, "react-me").await?;
    expect_ack(&mut clients[0], "react-me").await?;
    let created = |events: &[SequencedEvent]| {
        events
            .iter()
            .find(|e| e.channel_id == channel && e.dedupe_id == "react-me")
            .map(|e| e.msg_seq)
    };
    wait_events(&mut clients[1], |e| created(e).is_some()).await?;
    let msg_seq = created(&clients[1].events).ok_or("reactions: bob didn't get the message")?;
    let url = |emoji: &str, counter: i64| {
        format!(
            "{messages_url}/v1/channels/{channel}/messages/{msg_seq}/reactions/{emoji}?counter={counter}"
        )
    };
    let (status, _) = call("PUT", &url("%F0%9F%91%8D", 1), &mallory.1, None).await?;
    if status != 404 {
        return Err(format!(
            "reactions: a non-member's reaction: expected 404, got {status}"
        ));
    }
    // bob reacts and changes his mind; alice reacts. Whatever the batching, the end
    // state is alice's reaction only.
    for (who, method, counter) in [(bob, "PUT", 1), (alice, "PUT", 1), (bob, "DELETE", 2)] {
        let (status, reply) = call(method, &url("%F0%9F%91%8D", counter), &who.1, None).await?;
        if status != 202 {
            return Err(format!(
                "reactions: {method} by {}: {status} {reply}",
                who.0
            ));
        }
    }
    // The end state: count 1, reached either by bob's removal or, when everything fell
    // into one batch, by alice's add alone (bob's add was replaced by his removal).
    let done = |events: &[SequencedEvent]| {
        events
            .iter()
            .filter(|e| e.channel_id == channel)
            .any(|e| match &e.payload {
                Some(Payload::ReactionsChanged(r)) if r.msg_seq == msg_seq && r.emoji == "👍" => {
                    e.msg_seq == 0
                        && r.count == 1
                        && (r.removed.contains(&bob.0)
                            || (r.added.contains(&alice.0) && !r.added.contains(&bob.0)))
                }
                _ => false,
            })
    };
    let started = Instant::now();
    wait_events(&mut clients[1], done).await?;
    if !done(&clients[1].events) {
        let seen: Vec<_> = clients[1]
            .events
            .iter()
            .filter(|e| e.channel_id == channel)
            .filter_map(|e| match &e.payload {
                Some(Payload::ReactionsChanged(r)) => {
                    Some((r.count, r.added.clone(), r.removed.clone()))
                }
                _ => None,
            })
            .collect();
        return Err(format!(
            "reactions: bob never saw the end state (count 1, alice only); saw {seen:?}"
        ));
    }
    println!(
        "reactions: a non-member is refused; bob's add and remove and alice's add end at 1, delivered in {} ms through the batcher",
        started.elapsed().as_millis()
    );
    Ok(())
}

/// Calls (ADR-0025), against a real LiveKit: alice gets a token for her channel's huddle
/// (mallory, not a member, gets nothing); the room is created and deleted through
/// LiveKit's API, LiveKit's signed webhook reaches the calls service, and bob receives
/// the call's end as a sequenced event. (Joining needs WebRTC, which this can't do.)
#[allow(clippy::too_many_arguments)]
async fn check_calls(
    calls_url: &str,
    livekit_url: &str,
    secret: &str,
    channels_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-calls-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("calls: creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("calls: no channel_id")?;
    // Membership reaches Cassandra a moment after the channel is created.
    let deadline = Instant::now() + Duration::from_secs(30);
    let joined = loop {
        let (status, reply) = call(
            "POST",
            &format!("{calls_url}/v1/channels/{channel}/call"),
            &alice.1,
            None,
        )
        .await?;
        if status == 200 {
            break reply;
        }
        if status != 404 || Instant::now() > deadline {
            return Err(format!("calls: alice's call: {status} {reply}"));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
    };
    let room = joined["call_id"]
        .as_str()
        .ok_or("calls: no call_id")?
        .to_string();
    if room != format!("h{channel}")
        || joined["huddle"] != true
        || joined["token"].as_str().is_none_or(str::is_empty)
    {
        return Err(format!("calls: unexpected answer {joined}"));
    }
    let (status, _) = call(
        "POST",
        &format!("{calls_url}/v1/channels/{channel}/call"),
        &mallory.1,
        None,
    )
    .await?;
    if status != 404 {
        return Err(format!(
            "calls: a non-member's call: expected 404, got {status}"
        ));
    }

    // LiveKit's server API, with a token signed like the calls service's.
    let keys = xmsg_calls::livekit::Keys {
        api_key: "xmsg".into(),
        api_secret: secret.into(),
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let admin = keys.admin_token(now, 600);
    let http = reqwest::Client::new();
    let twirp = async |method: &str, body: serde_json::Value| -> Result<(), String> {
        let response = http
            .post(format!("{livekit_url}/twirp/livekit.RoomService/{method}"))
            .bearer_auth(&admin)
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("calls: LiveKit {method}: {e}"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!(
                "calls: LiveKit {method}: {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            ))
        }
    };
    // LiveKit has no healthcheck to wait on: give it a moment.
    let deadline = Instant::now() + Duration::from_secs(30);
    while let Err(e) = twirp("CreateRoom", serde_json::json!({ "name": room })).await {
        if Instant::now() > deadline {
            return Err(e);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    twirp("DeleteRoom", serde_json::json!({ "room": room })).await?;

    let ended = |events: &[SequencedEvent]| {
        events.iter().any(|e| {
            e.channel_id == channel
                && matches!(&e.payload, Some(sequenced_event::Payload::CallEvent(c))
                    if c.call_id == room && c.change == xmsg_proto::v1::CallChange::Ended as i32)
        })
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ended(&clients[1].events) {
        if Instant::now() > deadline {
            return Err(
                "calls: bob never saw the call end (LiveKit's webhook didn't come through)".into(),
            );
        }
        let _ = tokio::time::timeout(Duration::from_millis(500), next_frame(&mut clients[1])).await;
    }
    // Over: the call is gone.
    let (status, _) = call(
        "GET",
        &format!("{calls_url}/v1/channels/{channel}/call"),
        &bob.1,
        None,
    )
    .await?;
    if status != 404 {
        return Err(format!("calls: the ended call still answers {status}"));
    }
    println!(
        "calls: a member gets a huddle token, a non-member nothing; LiveKit's signed webhook ends the call, and bob sees it sequenced"
    );
    Ok(())
}

/// A 1x1 transparent PNG.
const PIXEL_PNG: &[u8] = &[
    0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
    0x89, 0x00, 0x00, 0x00, 0x0A, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
    0x42, 0x60, 0x82,
];

/// Files (ADR-0024), in a channel of their own: alice uploads an image and a page that
/// claims to be one; bob (a member) gets both back, the image inline with a thumbnail and
/// the page only as a download; mallory (not a member) gets nothing.
async fn check_media(
    media_url: &str,
    channels_url: &str,
    messages_url: Option<&str>,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-media-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("media: creating the channel: {status} {reply}"));
    }
    let channel = reply["channel_id"]
        .as_str()
        .ok_or("media: no channel_id")?
        .to_string();
    let channel_id: i64 = channel.parse().map_err(|_| "media: bad channel_id")?;
    let http = reqwest::Client::new();
    let upload =
        async |name: &str, content_type: &str, body: &[u8]| -> Result<serde_json::Value, String> {
            // The membership the channel was created with reaches Cassandra a moment later.
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
                let response = http
                    .post(format!(
                        "{media_url}/v1/channels/{channel}/files?name={name}"
                    ))
                    .bearer_auth(&alice.1)
                    .header("content-type", content_type)
                    .body(body.to_vec())
                    .send()
                    .await
                    .map_err(|e| format!("media upload: {e}"))?;
                let status = response.status().as_u16();
                let reply: serde_json::Value = response.json().await.unwrap_or_default();
                if status == 201 {
                    return Ok(reply);
                }
                if status != 404 || Instant::now() > deadline {
                    return Err(format!("media upload of {name}: {status} {reply}"));
                }
                tokio::time::sleep(Duration::from_millis(300)).await;
            }
        };
    let get = async |who: &(i64, String),
                     path: &str|
           -> Result<(u16, reqwest::header::HeaderMap, Vec<u8>), String> {
        let response = http
            .get(format!("{media_url}{path}"))
            .bearer_auth(&who.1)
            .send()
            .await
            .map_err(|e| format!("media GET {path}: {e}"))?;
        let status = response.status().as_u16();
        let headers = response.headers().clone();
        let body = response.bytes().await.map_err(|e| e.to_string())?.to_vec();
        Ok((status, headers, body))
    };
    let header = |h: &reqwest::header::HeaderMap, name: &str| {
        h.get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string()
    };

    let image = upload("pixel.png", "image/png", PIXEL_PNG).await?;
    let id = image["file_id"].as_str().ok_or("media: no file_id")?;
    if image["content_type"] != "image/png"
        || image["width"] != 1
        || image["thumbnail_type"] != "image/png"
    {
        return Err(format!("media: unexpected metadata {image}"));
    }
    let (status, headers, body) = get(bob, &format!("/v1/files/{id}")).await?;
    if status != 200 || body != PIXEL_PNG || header(&headers, "content-type") != "image/png" {
        return Err(format!(
            "media: bob's download: {status}, {} bytes, {headers:?}",
            body.len()
        ));
    }
    let (status, headers, _) = get(bob, &format!("/v1/files/{id}/thumbnail")).await?;
    if status != 200 || header(&headers, "content-type") != "image/png" {
        return Err(format!("media: thumbnail: {status} {headers:?}"));
    }
    let (status, _, _) = get(mallory, &format!("/v1/files/{id}")).await?;
    if status != 404 {
        return Err(format!(
            "media: a non-member's download: expected 404, got {status}"
        ));
    }

    let page = upload(
        "cat.png",
        "image/png",
        b"<html><script>alert(1)</script></html>",
    )
    .await?;
    let id = page["file_id"].as_str().ok_or("media: no file_id")?;
    let (status, headers, _) = get(bob, &format!("/v1/files/{id}")).await?;
    if status != 200
        || header(&headers, "content-type") != "application/octet-stream"
        || !header(&headers, "content-disposition").starts_with("attachment")
        || header(&headers, "x-content-type-options") != "nosniff"
    {
        return Err(format!(
            "media: a fake image must be a plain download: {status} {headers:?}"
        ));
    }
    // The image in a message: only its ID is sent; everyone gets its details.
    let image_id: i64 = image["file_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("media: bad file_id")?;
    let with_files = |client_msg_id: &str| ClientFrame {
        frame: Some(client_frame::Frame::SendMessage(SendMessageRequest {
            channel_id,
            client_msg_id: client_msg_id.into(),
            file_ids: vec![image_id],
            ..Default::default()
        })),
    };
    clients[0]
        .ws
        .send(Ws::Binary(with_files("files-1").encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    expect_ack(&mut clients[0], "files-1").await?;
    let attached = |events: &[SequencedEvent]| {
        events.iter().find_map(|e| match &e.payload {
            Some(sequenced_event::Payload::MessageCreated(m))
                if e.channel_id == channel_id && e.dedupe_id == "files-1" =>
            {
                Some(m.attachments.clone())
            }
            _ => None,
        })
    };
    wait_events(&mut clients[1], |e| attached(e).is_some()).await?;
    let files =
        attached(&clients[1].events).ok_or("media: bob didn't get the message with a file")?;
    if files.len() != 1
        || files[0].file_id != image_id
        || files[0].name != "pixel.png"
        || files[0].content_type != "image/png"
        || !files[0].has_thumbnail
    {
        return Err(format!("media: unexpected attachments {files:?}"));
    }
    // Bob can't attach alice's upload.
    clients[1]
        .ws
        .send(Ws::Binary(with_files("files-2").encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    loop {
        match next_frame(&mut clients[1]).await? {
            server_frame::Frame::SendRejected(r) if r.client_msg_id == "files-2" => break,
            server_frame::Frame::Event(_) => {}
            other => {
                return Err(format!(
                    "media: bob attaching alice's file: expected a rejection, got {other:?}"
                ));
            }
        }
    }
    // A thread reply with the file: delivered with its details, and in the thread API.
    let root = clients[1].events.iter().find_map(|e| match &e.payload {
        Some(sequenced_event::Payload::MessageCreated(m))
            if e.channel_id == channel_id && e.dedupe_id == "files-1" =>
        {
            Some((e.msg_seq, m.message_id))
        }
        _ => None,
    });
    let (root_seq, root_id) = root.ok_or("media: no root for the reply")?;
    // A file belongs to one message: the reply gets an upload of its own.
    let second = upload("pixel-2.png", "image/png", PIXEL_PNG).await?;
    let reply_image_id: i64 = second["file_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("media: bad file_id")?;
    let reply = ClientFrame {
        frame: Some(client_frame::Frame::SendMessage(SendMessageRequest {
            channel_id,
            client_msg_id: "files-reply".into(),
            body: "another, in a thread".into(),
            thread_root: Some(ThreadRoot {
                msg_seq: root_seq,
                message_id: root_id,
            }),
            file_ids: vec![reply_image_id],
        })),
    };
    clients[0]
        .ws
        .send(Ws::Binary(reply.encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    expect_ack(&mut clients[0], "files-reply").await?;
    let in_reply = |events: &[SequencedEvent]| {
        events.iter().find_map(|e| match &e.payload {
            Some(sequenced_event::Payload::ThreadReply(r))
                if e.channel_id == channel_id && e.dedupe_id == "files-reply" =>
            {
                Some(r.attachments.clone())
            }
            _ => None,
        })
    };
    wait_events(&mut clients[1], |e| in_reply(e).is_some()).await?;
    let files =
        in_reply(&clients[1].events).ok_or("media: bob didn't get the reply with a file")?;
    if files.len() != 1 || files[0].file_id != reply_image_id {
        return Err(format!("media: unexpected reply attachments {files:?}"));
    }
    if let Some(messages_url) = messages_url {
        // The persister writes the reply a moment after delivery.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (status, page) = call(
                "GET",
                &format!("{messages_url}/v1/channels/{channel_id}/threads/{root_seq}"),
                &users[1].1,
                None,
            )
            .await?;
            let file = &page["replies"][0]["attachments"][0];
            if status == 200
                && file["file_id"].as_str() == Some(reply_image_id.to_string().as_str())
                && file["has_thumbnail"] == true
            {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "media: the thread API's reply has no file: {status} {page}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        // Sent once, it can't be sent again (the media service records it a moment after
        // the message is sequenced, so early tries may still pass).
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let id = format!("files-again-{attempt}");
            clients[0]
                .ws
                .send(Ws::Binary(with_files(&id).encode_to_vec().into()))
                .await
                .map_err(|e| e.to_string())?;
            let refused = loop {
                match next_frame(&mut clients[0]).await? {
                    server_frame::Frame::SendRejected(r) if r.client_msg_id == id => break true,
                    server_frame::Frame::Ack(a) if a.client_msg_id == id => break false,
                    _ => {}
                }
            };
            if refused {
                break;
            }
            if Instant::now() > deadline {
                return Err("media: a file already sent could still be sent again".into());
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        // Deleting the message takes its file with it at once; the reply's file stays.
        let (status, reply) = call(
            "DELETE",
            &format!("{messages_url}/v1/channels/{channel_id}/messages/{root_seq}"),
            &users[0].1,
            None,
        )
        .await?;
        if status != 202 {
            return Err(format!("media: deleting the message: {status} {reply}"));
        }
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let (gone, _, _) = get(bob, &format!("/v1/files/{image_id}")).await?;
            if gone == 404 {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "media: a deleted message's file still answers {gone}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let (status, _, _) = get(bob, &format!("/v1/files/{reply_image_id}")).await?;
        if status != 200 {
            return Err(format!("media: the reply's file must stay, got {status}"));
        }
    }
    println!(
        "media: an image round-trips with a thumbnail, a page posing as an image is only a download, a non-member gets 404; a message and a thread reply carry the file's details, nobody can attach someone else's upload or send a file twice, and deleting a message takes its file"
    );
    Ok(())
}

/// Mentions (ADR-0012), in a channel of their own: the sequencer keeps members only and
/// `@channel` only from admins; the readstate feeder records them; reading clears them.
async fn check_mentions(
    channels_url: &str,
    readstate_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    use sequenced_event::Payload;
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-mentions-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("mentions: creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("mentions: no channel_id")?;
    // bob (a plain member) tries @here first: the message goes through, the channel
    // mention doesn't. Then alice (the creator, so an admin) mentions bob, mallory (not a
    // member) and everyone. bob's own send marks only what came before it read.
    send_body(&mut clients[1].ws, channel, "mention-b", "<!here> lunch?").await?;
    expect_ack(&mut clients[1], "mention-b").await?;
    let created = |events: &[SequencedEvent], id: &str| {
        events.iter().find_map(|e| match &e.payload {
            Some(Payload::MessageCreated(m)) if e.channel_id == channel && e.dedupe_id == id => {
                Some((e.msg_seq, m.mentioned_user_ids.clone(), m.mentions_channel))
            }
            _ => None,
        })
    };
    wait_events(&mut clients[1], |e| created(e, "mention-b").is_some()).await?;
    let body = format!("hi <@{}> <@{}> <!channel>", bob.0, mallory.0);
    send_body(&mut clients[0].ws, channel, "mention-a", &body).await?;
    expect_ack(&mut clients[0], "mention-a").await?;
    wait_events(&mut clients[1], |e| created(e, "mention-a").is_some()).await?;
    let a = created(&clients[1].events, "mention-a")
        .ok_or("mentions: bob didn't get alice's message")?;
    let b = created(&clients[1].events, "mention-b")
        .ok_or("mentions: bob didn't get his own message")?;
    if a.1 != vec![bob.0] || !a.2 {
        return Err(format!(
            "mentions: alice's message: expected [bob] and @channel, got {a:?}"
        ));
    }
    if !b.1.is_empty() || b.2 {
        return Err(format!(
            "mentions: bob's @here should be dropped, got {b:?}"
        ));
    }

    let unread = async |token: &str| -> Result<Option<Vec<String>>, String> {
        let (status, reply) = call(
            "GET",
            &format!("{readstate_url}/v1/me/mentions"),
            token,
            None,
        )
        .await?;
        if status != 200 {
            return Err(format!("mentions: GET /v1/me/mentions: {status} {reply}"));
        }
        Ok(reply["mentions"].as_array().and_then(|list| {
            list.iter()
                .find(|m| m["channel_id"].as_str() == Some(channel.to_string().as_str()))
                .map(|m| {
                    m["msg_seqs"]
                        .as_array()
                        .map(|s| {
                            s.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default()
                })
        }))
    };
    // The feeder and the owner apply a moment after the sequencer: poll for each state.
    let wait_for =
        async |token: &str, want: Option<Vec<String>>, what: &str| -> Result<(), String> {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let got = unread(token).await?;
                if got == want {
                    return Ok(());
                }
                if Instant::now() > deadline {
                    return Err(format!("mentions: {what}: expected {want:?}, got {got:?}"));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        };
    wait_for(&bob.1, Some(vec![a.0.to_string()]), "bob's unread mention").await?;
    let (status, _) = call(
        "POST",
        &format!("{readstate_url}/v1/channels/{channel}/read"),
        &bob.1,
        Some(serde_json::json!({"msg_seq": a.0.to_string()})),
    )
    .await?;
    if status != 202 {
        return Err(format!("mentions: bob's mark-read: {status}"));
    }
    wait_for(&bob.1, None, "after bob read the channel").await?;
    println!(
        "mentions: members only, @channel only from admins; bob's unread mention is listed by readstate and cleared by reading"
    );
    Ok(())
}

/// Edits and deletes (plan §5.5), in a channel of their own so the main channel's
/// event counts stay as the other checks expect.
async fn check_edits(
    channels_url: &str,
    messages_url: &str,
    clients: &mut [Client; 3],
    users: &[(i64, String)],
    run_id: i64,
) -> Result<(), String> {
    use sequenced_event::Payload;
    let (alice, bob) = (&users[0], &users[1]);
    let (status, reply) = call(
        "POST",
        &format!("{channels_url}/v1/channels"),
        &alice.1,
        Some(serde_json::json!({"type": "private", "name": format!("e2e-edits-{run_id}"), "member_ids": [bob.0.to_string()]})),
    )
    .await?;
    if status != 201 {
        return Err(format!("edits: creating the channel: {status} {reply}"));
    }
    let channel: i64 = reply["channel_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .ok_or("edits: no channel_id")?;
    for (i, id) in [(0, "edit-a"), (1, "edit-b")] {
        send(&mut clients[i].ws, channel, id).await?;
        expect_ack(&mut clients[i], id).await?;
    }
    let msg_seq = |events: &[SequencedEvent], id: &str| {
        events
            .iter()
            .find(|e| {
                e.channel_id == channel
                    && e.dedupe_id == id
                    && matches!(e.payload, Some(Payload::MessageCreated(_)))
            })
            .map(|e| e.msg_seq)
    };
    wait_events(&mut clients[1], |e| {
        msg_seq(e, "edit-a").is_some() && msg_seq(e, "edit-b").is_some()
    })
    .await?;
    let (Some(a), Some(b)) = (
        msg_seq(&clients[1].events, "edit-a"),
        msg_seq(&clients[1].events, "edit-b"),
    ) else {
        return Err("edits: bob didn't receive both messages".into());
    };
    let url = |seq: i64| format!("{messages_url}/v1/channels/{channel}/messages/{seq}");

    // Only the author edits.
    let (status, _) = call(
        "PATCH",
        &url(a),
        &bob.1,
        Some(serde_json::json!({"body": "hijacked", "edit_id": "x"})),
    )
    .await?;
    if status != 403 {
        return Err(format!(
            "edits: bob editing alice's message: expected 403, got {status}"
        ));
    }
    let (status, reply) = call(
        "PATCH",
        &url(a),
        &alice.1,
        Some(serde_json::json!({"body": "edited by alice", "edit_id": "e1"})),
    )
    .await?;
    if status != 202 {
        return Err(format!(
            "edits: alice's edit: expected 202, got {status} {reply}"
        ));
    }
    let edited = |events: &[SequencedEvent]| {
        events.iter().any(|e| {
            e.channel_id == channel
                && e.msg_seq == 0
                && matches!(&e.payload, Some(Payload::MessageEdited(m)) if m.msg_seq == a && m.body == "edited by alice")
        })
    };
    wait_events(&mut clients[1], edited).await?;
    if !edited(&clients[1].events) {
        return Err("edits: bob never received alice's edit".into());
    }

    // Deleting someone else's message needs the admin role: bob isn't, alice (the creator) is.
    let (status, _) = call("DELETE", &url(a), &bob.1, None).await?;
    if status != 202 {
        return Err(format!(
            "edits: bob's delete request: expected 202, got {status}"
        ));
    }
    let refused = |events: &[SequencedEvent]| {
        events.iter().any(|e| {
            e.channel_id == channel
                && e.dedupe_id == format!("delete:{a}:{}", bob.0)
                && matches!(&e.payload, Some(Payload::Rejected(r)) if r.reason == RejectReason::Forbidden as i32)
        })
    };
    wait_events(&mut clients[1], refused).await?;
    if !refused(&clients[1].events) {
        return Err(
            "edits: bob's delete of alice's message wasn't refused by the sequencer".into(),
        );
    }
    let (status, _) = call("DELETE", &url(b), &alice.1, None).await?;
    if status != 202 {
        return Err(format!(
            "edits: alice's admin delete: expected 202, got {status}"
        ));
    }
    let deleted = |events: &[SequencedEvent]| {
        events.iter().any(|e| {
            e.channel_id == channel
                && matches!(&e.payload, Some(Payload::MessageDeleted(d)) if d.msg_seq == b)
        })
    };
    wait_events(&mut clients[1], deleted).await?;
    if !deleted(&clients[1].events) {
        return Err("edits: bob never received the delete of his message".into());
    }

    // History shows the current state: the edited body, and nothing of the deleted one.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let response = reqwest::Client::new()
            .get(format!(
                "{messages_url}/v1/channels/{channel}/events?after=0"
            ))
            .bearer_auth(&bob.1)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let bytes = response.bytes().await.map_err(|e| e.to_string())?;
        let page = xmsg_proto::v1::EventPage::decode(bytes.as_ref()).map_err(|e| e.to_string())?;
        let body_of = |seq: i64| {
            page.events.iter().find_map(|e| match &e.payload {
                Some(Payload::MessageCreated(m)) if e.msg_seq == seq => Some(m.body.clone()),
                _ => None,
            })
        };
        let has_delete = page
            .events
            .iter()
            .any(|e| matches!(&e.payload, Some(Payload::MessageDeleted(d)) if d.msg_seq == b));
        if body_of(a).as_deref() == Some("edited by alice")
            && body_of(b).as_deref() == Some("")
            && has_delete
        {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "edits: history shows {:?} / {:?}, delete event: {has_delete}",
                body_of(a),
                body_of(b)
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!(
        "edits: only the author edits; members get the edit live; a non-admin's delete of another's message is refused by the sequencer, an admin's goes through; history shows the edited body and none for the deleted message"
    );
    Ok(())
}

/// Personal settings (ADR-0016): a change goes to `member.settings` and the
/// settings-writer stores it; a non-member can't set any.
async fn check_settings(url: &str, channel: i64, users: &[(i64, String)]) -> Result<(), String> {
    let (bob, mallory) = (&users[1], &users[2]);
    let settings = format!("{url}/v1/channels/{channel}/settings");
    let (status, body) = call(
        "PUT",
        &settings,
        &bob.1,
        Some(serde_json::json!({"notify_level": "mentions", "muted": true})),
    )
    .await?;
    if status != 202 {
        return Err(format!("bob muting the channel: {status} {body}"));
    }
    let (status, _) = call(
        "PUT",
        &settings,
        &mallory.1,
        Some(serde_json::json!({"notify_level": "all", "muted": true})),
    )
    .await?;
    if status != 404 {
        return Err(format!(
            "a former member setting settings: expected 404, got {status}"
        ));
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let (status, body) = call("GET", &format!("{url}/v1/me/settings"), &bob.1, None).await?;
        let stored = body["settings"].as_array().and_then(|ss| {
            ss.iter()
                .find(|s| s["channel_id"].as_str() == Some(channel.to_string().as_str()))
                .cloned()
        });
        if status == 200
            && stored
                == Some(
                    serde_json::json!({"channel_id": channel.to_string(), "notify_level": "mentions", "muted": true}),
                )
        {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!(
                "bob's settings not stored within 20 s: {status} {body}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!(
        "settings: bob's mute went through member.settings and the settings-writer; a non-member is refused"
    );
    Ok(())
}

/// Read positions (ADR-0012): own sends mark the channel read up to them, a join starts
/// at the channel's current message, and mark-read / mark-unread go through the owner.
/// The channel: alice sent 1 and 3, bob sent 2, mallory joined after 3 (then left).
async fn check_readstate(url: &str, channel: i64, users: &[(i64, String)]) -> Result<(), String> {
    let (alice, bob, mallory) = (&users[0], &users[1], &users[2]);
    let position = async |token: &str| -> Result<Option<String>, String> {
        let (status, body) = call("GET", &format!("{url}/v1/me/read-state"), token, None).await?;
        if status != 200 {
            return Err(format!("read-state: {status} {body}"));
        }
        Ok(body["positions"].as_array().and_then(|ps| {
            ps.iter()
                .find(|p| p["channel_id"].as_str() == Some(channel.to_string().as_str()))
                .and_then(|p| p["last_read_msg_seq"].as_str().map(str::to_string))
        }))
    };
    let wait_for = async |who: &str, token: &str, want: &str| -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let got = position(token).await?;
            if got.as_deref() == Some(want) {
                return Ok(());
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "{who}'s read position: expected {want}, got {got:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    wait_for("alice (sent 1 and 3)", &alice.1, "3").await?;
    wait_for("bob (sent 2)", &bob.1, "2").await?;
    wait_for("mallory (joined after 3)", &mallory.1, "3").await?;

    let read = format!("{url}/v1/channels/{channel}/read");
    let (status, body) = call(
        "POST",
        &read,
        &bob.1,
        Some(serde_json::json!({"msg_seq": "3"})),
    )
    .await?;
    if status != 202 {
        return Err(format!("mark read: {status} {body}"));
    }
    wait_for("bob after mark-read 3", &bob.1, "3").await?;
    // An older mark-read (e.g. a slow device) can't move him back...
    call(
        "POST",
        &read,
        &bob.1,
        Some(serde_json::json!({"msg_seq": "1"})),
    )
    .await?;
    // ...but an explicit "unread from 2" does.
    let unread = format!("{url}/v1/channels/{channel}/unread");
    call(
        "POST",
        &unread,
        &bob.1,
        Some(serde_json::json!({"msg_seq": "2"})),
    )
    .await?;
    wait_for("bob after unread-from 2", &bob.1, "1").await?;
    println!(
        "read state: own sends and joins set positions (alice 3, bob 2, mallory 3); mark-read can't go back, mark-unread does"
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

/// Signs a user up (with an invite code when the install demands one) and logs in;
/// returns (user ID, access token).
const PASSWORD: &str = "e2e smoke password";

async fn sign_up(auth: &str, email: &str, invite: Option<&str>) -> Result<u16, String> {
    let mut body = serde_json::json!({"email": email, "password": PASSWORD, "name": "E2E"});
    if let Some(invite) = invite {
        body["invite"] = serde_json::json!(invite);
    }
    let response = reqwest::Client::new()
        .post(format!("{auth}/v1/signup"))
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("/v1/signup: {e}"))?;
    Ok(response.status().as_u16())
}

async fn sign_up_and_log_in(
    auth: &str,
    email: &str,
    invite: Option<&str>,
) -> Result<(i64, String), String> {
    let status = sign_up(auth, email, invite).await?;
    if status != 201 {
        return Err(format!("signing up {email}: got {status}, want 201"));
    }
    log_in(auth, email).await
}

/// Logs in a user `sign_up_and_log_in` created: (user id, access token).
async fn log_in(auth: &str, email: &str) -> Result<(i64, String), String> {
    let response = reqwest::Client::new()
        .post(format!("{auth}/v1/login"))
        .json(&serde_json::json!({"email": email, "password": PASSWORD}))
        .send()
        .await
        .map_err(|e| format!("/v1/login: {e}"))?;
    let status = response.status();
    let tokens: serde_json::Value = response
        .json()
        .await
        .map_err(|e| format!("/v1/login: {e}"))?;
    if !status.is_success() {
        return Err(format!("/v1/login: {status} {tokens}"));
    }
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
/// `owner` is an authenticated workspace owner (the smoke run's first account), used
/// to open the sign-up policy when the install is invite-only (ADR-0026).
async fn check_revocation(
    auth: &str,
    gateway: &str,
    run_id: i64,
    owner: Option<&(i64, String)>,
    users_url: Option<&str>,
) -> Result<(), String> {
    let http = reqwest::Client::new();
    let email = format!("dora-{run_id}@example.org");
    let password = "e2e smoke password";
    // Dora signs up after the run's other accounts: on an invite-only install that
    // takes the owner opening the policy (or an invite) first.
    let (_, policy) = call("GET", &format!("{auth}/v1/signup-policy"), "", None).await?;
    if policy["invite_only"] == true {
        let (Some((_, owner_token)), Some(users_url)) = (owner, users_url) else {
            return Err(
                "sign-up is invite-only and no workspace owner is at hand for dora's account"
                    .to_string(),
            );
        };
        let opened = call(
            "PATCH",
            &format!("{users_url}/v1/workspaces/1"),
            owner_token,
            Some(serde_json::json!({"signup": "open"})),
        )
        .await?;
        if opened.0 != 200 {
            return Err(format!(
                "opening the sign-up policy for dora's account: {} {}",
                opened.0, opened.1
            ));
        }
    }
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

    let channels = std::env::var("XMSG_CHANNELS_URL").ok();
    if let Some(url) = &channels {
        let (status, body) = call("GET", &format!("{url}/v1/me/channels"), access, None).await?;
        if status != 200 {
            return Err(format!("before logout, my channels: {status} {body}"));
        }
    }
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
    // HTTP services refuse the token too (xmsg-authn's deny set, fed over NATS).
    if let Some(url) = &channels {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (status, _) = call("GET", &format!("{url}/v1/me/channels"), access, None).await?;
            if status == 401 {
                break;
            }
            if Instant::now() > deadline {
                return Err(format!(
                    "after logout, the channels service still answers {status}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
    println!(
        "revocation: logout closed the open socket in {} ms; its still-valid access token is refused by the gateway and HTTP services",
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
    send_body(
        ws,
        channel,
        client_msg_id,
        &format!("hello from {client_msg_id}"),
    )
    .await
}

async fn send_body(
    ws: &mut Socket,
    channel: i64,
    client_msg_id: &str,
    body: &str,
) -> Result<(), String> {
    let frame = ClientFrame {
        frame: Some(client_frame::Frame::SendMessage(SendMessageRequest {
            channel_id: channel,
            client_msg_id: client_msg_id.into(),
            body: body.into(),
            ..Default::default()
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

/// Presence (plan §5.9): through the gateway's pings, the presence service and Valkey,
/// and back to a watching client.
async fn check_presence(clients: &mut [Client; 3], users: &[(i64, String)]) -> Result<(), String> {
    let frame = |f: client_frame::Frame| ClientFrame { frame: Some(f) }.encode_to_vec();
    async fn presence_seen(client: &mut Client, within: Duration) -> Vec<PresenceUpdate> {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, next_frame(client)).await {
                Ok(Ok(server_frame::Frame::Presence(p))) => return p.updates,
                Ok(Ok(_)) => {}
                _ => break,
            }
        }
        Vec::new()
    }
    let status_of = |updates: &[PresenceUpdate], user: i64| {
        updates.iter().find(|u| u.user_id == user).map(|u| u.status)
    };
    let [alice, bob, _] = clients;
    let (alice_id, nobody) = (users[0].0, 1_i64);
    bob.ws
        .send(Ws::Binary(
            frame(client_frame::Frame::WatchPresence(WatchPresence {
                user_ids: vec![alice_id, nobody],
            }))
            .into(),
        ))
        .await
        .map_err(|e| e.to_string())?;
    let now = presence_seen(bob, Duration::from_secs(10)).await;
    if status_of(&now, alice_id) != Some(PresenceStatus::Online as i32)
        || status_of(&now, nobody) != Some(PresenceStatus::Offline as i32)
    {
        return Err(format!(
            "presence: expected alice online and user 1 offline, got {now:?}"
        ));
    }
    for (active, want) in [
        (false, PresenceStatus::Away),
        (true, PresenceStatus::Online),
    ] {
        alice
            .ws
            .send(Ws::Binary(
                frame(client_frame::Frame::Activity(Activity { active })).into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
        let changed = presence_seen(bob, Duration::from_secs(10)).await;
        if status_of(&changed, alice_id) != Some(want as i32) {
            return Err(format!(
                "presence: after alice's activity {active}, bob expected {want:?}, got {changed:?}"
            ));
        }
    }
    println!(
        "presence: bob sees alice online, away when idle, online again; a user who never connected is offline"
    );
    Ok(())
}

/// Typing (plan §5.9): best-effort, through the gateway, the partition's fan-out and back.
/// A member's typing reaches the other members; a non-member's reaches nobody.
async fn check_typing(
    clients: &mut [Client; 3],
    channel: i64,
    users: &[(i64, String)],
) -> Result<(), String> {
    let typing = |channel_id| ClientFrame {
        frame: Some(client_frame::Frame::Typing(Typing { channel_id })),
    };
    // Waits up to `within` for a typing frame on `client`; other frames are kept as usual.
    async fn typing_seen(client: &mut Client, within: Duration) -> Option<TypingIndicator> {
        let deadline = tokio::time::Instant::now() + within;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, next_frame(client)).await {
                Ok(Ok(server_frame::Frame::Typing(t))) => return Some(t),
                Ok(Ok(_)) => {}
                _ => return None,
            }
        }
        None
    }
    let [alice, bob, mallory] = clients;
    mallory
        .ws
        .send(Ws::Binary(typing(channel).encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    if let Some(t) = typing_seen(alice, Duration::from_secs(2)).await {
        return Err(format!(
            "typing: a non-member's typing reached alice: {t:?}"
        ));
    }
    bob.ws
        .send(Ws::Binary(typing(channel).encode_to_vec().into()))
        .await
        .map_err(|e| e.to_string())?;
    let seen = typing_seen(alice, Duration::from_secs(10)).await;
    let expected = TypingIndicator {
        channel_id: channel,
        user_id: users[1].0,
    };
    if seen.as_ref() != Some(&expected) {
        return Err(format!("typing: alice expected {expected:?}, got {seen:?}"));
    }
    if let Some(t) = typing_seen(bob, Duration::from_millis(500)).await {
        return Err(format!("typing: bob saw his own typing: {t:?}"));
    }
    println!(
        "typing: bob's typing reached alice through the partition's fan-out; a non-member's reached nobody"
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
