// SPDX-License-Identifier: AGPL-3.0-only
//! Phase 1a exit test (plan §14): real-time chat that survives restarts, no lost or
//! duplicate `msg_seq` under rebalance chaos, and removed members receive nothing after
//! their removal. See README.md.

mod api;
mod check;
mod client;
mod stack;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use tokio::sync::watch;
use xmsg_proto::v1::sequenced_event::Payload;

use check::{Cassandra, Delivery, Persisted, Removal, Report};
use client::{Cmd, Ledger, Shared, User};
use stack::Stack;

#[derive(Parser, Debug, Clone)]
#[command(
    about = "Chat under load while services are killed and restarted; checks nothing is lost, duplicated or leaked"
)]
struct Args {
    /// Repository root (built binaries are taken from target/release and services/).
    #[arg(long, default_value = ".")]
    repo: PathBuf,
    /// Where service logs and sequencer state go (wiped at start).
    #[arg(long, default_value = "target/chaos")]
    logs: PathBuf,
    #[arg(long, default_value_t = 16)]
    users: usize,
    #[arg(long, default_value_t = 5)]
    channels: usize,
    /// Users that send continuously (the first N users).
    #[arg(long, default_value_t = 8)]
    senders: usize,
    /// Messages per second per sender.
    #[arg(long, default_value_t = 5.0)]
    rate: f64,
    /// Seconds of sending (and chaos).
    #[arg(long, default_value_t = 180)]
    duration: u64,
    /// Seconds between one restarted service being ready and the next kill.
    #[arg(long, default_value_t = 8)]
    kill_every: u64,
    /// Services to kill, in rotation.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "sequencer-1,fanout,persister,sequencer-2,messages"
    )]
    targets: Vec<String>,
    /// Sequencer instances (two, so an outage moves tasks: a real rebalance).
    #[arg(long, default_value_t = 2)]
    sequencers: usize,
    /// Run this `nats-server` binary as part of the stack (on the port of `XMSG_NATS_URL`),
    /// so `nats` can be a target. Without it, NATS is taken as already running.
    #[arg(long)]
    nats_server: Option<PathBuf>,
    /// Fraction of sends repeated after their ack (as if the ack was lost).
    #[arg(long, default_value_t = 0.03)]
    dup_rate: f64,
    /// Resend an unacked send after this many milliseconds (the real client's outbox).
    #[arg(long, default_value_t = 3000)]
    retry_after_ms: u64,
    #[arg(long)]
    seed: Option<u64>,
}

/// Small deterministic PRNG (xorshift), so a failing run can be repeated with --seed.
#[derive(Clone)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    fn chance(&mut self, p: f64) -> bool {
        (self.next() % 1_000_000) as f64 / 1_000_000.0 < p
    }
}

struct Kill {
    target: String,
    signal: &'static str,
    down: Duration,
    ready_after: Duration,
    /// For graceful stops: how the process exited (None = had to be killed).
    exit: Option<std::process::ExitStatus>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let _ = std::fs::remove_dir_all(&args.logs);
    if let Err(e) = std::fs::create_dir_all(&args.logs) {
        eprintln!("❌ {}: {e}", args.logs.display());
        return ExitCode::FAILURE;
    }
    let logs = std::fs::canonicalize(&args.logs).unwrap();
    let repo = std::fs::canonicalize(&args.repo).unwrap();
    let mut stack = match Stack::new(&repo, &logs, args.sequencers, args.nats_server.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Some(t) = args
        .targets
        .iter()
        .find(|t| !stack.services.iter().any(|s| &s.name == *t))
    {
        eprintln!("❌ unknown target {t} (nats needs --nats-server)");
        return ExitCode::FAILURE;
    }
    println!("starting the services (logs in {})", logs.display());
    if let Err(e) = stack.start_all().await {
        eprintln!("❌ {e}");
        stack.stop_all().await;
        return ExitCode::FAILURE;
    }
    let stack = Arc::new(tokio::sync::Mutex::new(stack));
    let result = tokio::select! {
        r = run(args, stack.clone()) => r,
        _ = tokio::signal::ctrl_c() => Err("interrupted".into()),
    };
    stack.lock().await.stop_all().await;
    match result {
        Ok(()) => {
            println!("✅ chaos test passed");
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("❌ chaos test failed: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args, stack: Arc<tokio::sync::Mutex<Stack>>) -> Result<(), String> {
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.into());
    let urls = api::Urls {
        auth: env("XMSG_AUTH_URL", "http://127.0.0.1:8081"),
        channels: env("XMSG_CHANNELS_URL", "http://127.0.0.1:8082"),
        messages: env("XMSG_MESSAGES_URL", "http://127.0.0.1:8083"),
    };
    let gateway = env("XMSG_GATEWAY_URL", "ws://127.0.0.1:4000/ws");
    let bootstrap = env("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092");
    let cql = env("XMSG_CQL_NODES", "127.0.0.1:9042");
    let seed = args.seed.unwrap_or(now_ms() as u64) | 1;
    let mut rng = Rng(seed);
    let started = Instant::now();
    let run_id = now_ms();
    println!("seed {seed}");

    // Users and their sockets.
    let ledger = Arc::new(Mutex::new(Ledger::default()));
    let (stop_tx, stop_rx) = watch::channel(false);
    let shared = Shared {
        ledger: ledger.clone(),
        retry_after: Duration::from_millis(args.retry_after_ms),
        stop: stop_rx.clone(),
    };
    let mut users = Vec::new();
    for i in 0..args.users {
        let (id, token) =
            api::sign_up(&urls.auth, &format!("chaos-{run_id}-{i}@example.org")).await?;
        users.push(User::spawn(gateway.clone(), id, token, &shared));
    }
    let users = Arc::new(users);
    // Let the gateway publish the sessions and fan-out learn them.
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("{} users signed up and connected", users.len());

    // Channels: random private memberships. Channel 0 is where user 1 gets removed by user 0.
    let mut persisted = Persisted::open(&bootstrap, run_id - 5_000)?;
    let mut channels: Vec<(i64, Vec<usize>)> = Vec::new();
    for c in 0..args.channels {
        let admin = if c == 0 { 0 } else { c % args.users };
        let mut members: Vec<usize> = (0..args.users)
            .filter(|&u| u == admin || (c == 0 && u == 1) || rng.chance(0.6))
            .collect();
        if members.len() < 3 {
            members = (0..args.users).collect();
        }
        let others: Vec<i64> = members
            .iter()
            .filter(|&&u| u != admin)
            .map(|&u| users[u].id)
            .collect();
        let id = api::create_channel(
            &urls.channels,
            &users[admin].token,
            &format!("chaos-{run_id}-{c}"),
            &others,
        )
        .await?;
        persisted.ours.insert(id);
        channels.push((id, members));
    }
    let removal = Arc::new(Mutex::new(Removal {
        channel: channels[0].0,
        victim: users[1].id,
        ..Default::default()
    }));
    println!("{} channels created", channels.len());
    let of_user: Vec<Vec<i64>> = (0..args.users)
        .map(|u| {
            channels
                .iter()
                .filter(|(_, m)| m.contains(&u))
                .map(|(c, _)| *c)
                .collect()
        })
        .collect();

    // Senders.
    let (senders_stop, senders_rx) = watch::channel(false);
    let mut tasks = Vec::new();
    for u in 0..args.senders.min(args.users) {
        let (users, ledger, mut stop, mut rng) = (
            users.clone(),
            ledger.clone(),
            senders_rx.clone(),
            Rng(seed ^ ((u as u64 + 1) * 0x9E37_79B9)),
        );
        let mine = of_user[u].clone();
        let (rate, dup_rate) = (args.rate, args.dup_rate);
        tasks.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs_f64(1.0 / rate));
            let mut acked: Vec<(String, i64, String)> = Vec::new();
            for n in 0.. {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = stop.changed() => return,
                }
                if mine.is_empty() {
                    continue;
                }
                if rng.chance(dup_rate) && !acked.is_empty() {
                    let (id, channel, body) = acked[rng.below(acked.len() as u64) as usize].clone();
                    ledger.lock().unwrap().duplicates += 1;
                    let _ = users[u].tx.send(Cmd::Repeat { id, channel, body });
                    continue;
                }
                let channel = mine[rng.below(mine.len() as u64) as usize];
                let id = format!("u{u}-{n}");
                let body = format!("message {id} to {channel}");
                users[u].send(&ledger, id.clone(), channel, body.clone());
                // Remember a few for repeats once acked.
                if n % 7 == 0 {
                    acked.retain(|(i, _, _)| {
                        ledger.lock().unwrap().sends.get(i).is_some_and(|s| s.acked)
                    });
                    acked.push((id, channel, body));
                }
            }
        }));
    }

    // Chaos: kill one service at a time, restart it, wait until it's ready, repeat.
    let kills: Arc<Mutex<Vec<Kill>>> = Arc::default();
    let (chaos_stop, chaos_rx) = watch::channel(false);
    let chaos = {
        let (stack, kills, targets, mut stop, mut rng) = (
            stack.clone(),
            kills.clone(),
            args.targets.clone(),
            chaos_rx.clone(),
            Rng(seed ^ 0xC4A05),
        );
        let kill_every = Duration::from_secs(args.kill_every);
        tokio::spawn(async move {
            let mut count: HashMap<String, u32> = HashMap::new();
            for round in 0.. {
                let target = targets[round % targets.len()].clone();
                tokio::select! {
                    _ = tokio::time::sleep(kill_every) => {}
                    _ = stop.changed() => return Ok(()),
                }
                let n = count.entry(target.clone()).or_default();
                *n += 1;
                // Each target alternates between SIGKILL and a graceful stop, starting with
                // either in turn, so even one round covers both.
                let (signal, name) = if (*n as usize + round % targets.len()) % 2 == 1 {
                    (libc::SIGKILL, "SIGKILL")
                } else {
                    (libc::SIGTERM, "SIGTERM")
                };
                // Sequencer outages mostly outlast its 6 s session timeout, so its tasks move
                // to the other instance and back (rebalance, restore, EOS fencing).
                let down = if target.starts_with("sequencer") {
                    Duration::from_millis(5_000 + rng.below(10_000))
                } else {
                    Duration::from_millis(1_000 + rng.below(5_000))
                };
                let mut stack = stack.lock().await;
                let service = stack.get(&target);
                println!(
                    "  [{:>4}s] {name} {target}, down for {:.1}s",
                    started.elapsed().as_secs(),
                    down.as_secs_f64()
                );
                let exit = service.stop(signal, Duration::from_secs(20)).await;
                tokio::time::sleep(down).await;
                service.start()?;
                let ready_after = service.wait_ready(Duration::from_secs(150)).await?;
                println!(
                    "  [{:>4}s] {target} ready again after {:.1}s",
                    started.elapsed().as_secs(),
                    ready_after.as_secs_f64()
                );
                // An outage must not take anything else down with it (e.g. the gateway
                // when NATS goes away).
                if let Some(dead) = stack
                    .services
                    .iter_mut()
                    .find_map(|s| (!s.is_running()).then(|| s.name.clone()))
                {
                    return Err(format!("{dead} died while {target} was down"));
                }
                kills.lock().unwrap().push(Kill {
                    target,
                    signal: name,
                    down,
                    ready_after,
                    exit,
                });
            }
            Ok::<(), String>(())
        })
    };

    // Removal: part way through, the admin removes the victim while everyone keeps sending.
    let removal_task = {
        let (users, removal, ledger, urls) =
            (users.clone(), removal.clone(), ledger.clone(), urls.clone());
        let at = Duration::from_secs(args.duration * 2 / 5);
        tokio::spawn(async move {
            tokio::time::sleep(at).await;
            let (channel, victim) = {
                let r = removal.lock().unwrap();
                (r.channel, r.victim)
            };
            let url = format!("{}/v1/channels/{channel}/members/{victim}", urls.channels);
            let deadline = Instant::now() + Duration::from_secs(120);
            loop {
                match api::call("DELETE", &url, &users[0].token, None).await {
                    Ok((200 | 202, reply)) => {
                        println!(
                            "  [{:>4}s] admin removed user {victim} from channel {channel}: {reply}",
                            started.elapsed().as_secs()
                        );
                        break;
                    }
                    other if Instant::now() > deadline => {
                        return Err(format!("removing the member: {other:?}"));
                    }
                    _ => tokio::time::sleep(Duration::from_secs(1)).await,
                }
            }
            // Wait until the victim's socket gets its member.left.
            let deadline = Instant::now() + Duration::from_secs(180);
            let left = loop {
                let found = users[1]
                    .view
                    .lock()
                    .unwrap()
                    .live
                    .iter()
                    .find_map(|(at, e)| match &e.payload {
                        Some(Payload::MemberLeft(l))
                            if e.channel_id == channel && l.user_id == victim =>
                        {
                            Some((e.event_seq, *at))
                        }
                        _ => None,
                    });
                if let Some(f) = found {
                    break f;
                }
                if Instant::now() > deadline {
                    return Err("the removed member never received member.left".to_string());
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            println!(
                "  [{:>4}s] user {victim} received member.left (event_seq {})",
                started.elapsed().as_secs(),
                left.0
            );
            {
                let mut r = removal.lock().unwrap();
                r.left_seq = Some(left.0);
                r.seen_at = Some(left.1);
            }
            // The removed member keeps trying: every send must be rejected.
            for i in 0..5 {
                let id = format!("removed-{i}");
                users[1].send(&ledger, id.clone(), channel, format!("after removal {i}"));
                removal.lock().unwrap().after.push(id);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Ok(())
        })
    };

    tokio::time::sleep(Duration::from_secs(args.duration)).await;
    let _ = senders_stop.send(true);
    for t in tasks {
        let _ = t.await;
    }
    let _ = chaos_stop.send(true);
    chaos.await.map_err(|e| e.to_string())??;
    removal_task.await.map_err(|e| e.to_string())??;
    let sending_secs = started.elapsed().as_secs_f64();
    println!("sending stopped after {sending_secs:.0}s; quiescing");
    {
        let mut stack = stack.lock().await;
        for s in stack.services.iter_mut() {
            if !s.is_running() {
                return Err(format!("{} is not running at the end", s.name));
            }
        }
    }

    let db = Cassandra::connect(&cql).await?;
    quiesce(&ledger, &mut persisted, &db).await?;

    // Probes: the sequencer's state (after every restore) must continue exactly where
    // Cassandra's head is.
    let mut report = Report::default();
    let mut probes = Vec::new();
    for (channel, members) in &channels {
        let Some(&u) = members.iter().find(|&&u| u != 1) else {
            continue;
        };
        let head = db.head(*channel).await?.unwrap_or_default();
        let id = format!("probe-{channel}");
        users[u].send(&ledger, id.clone(), *channel, format!("probe {channel}"));
        probes.push((id, *channel, head));
    }
    quiesce(&ledger, &mut persisted, &db).await?;
    let outcomes = persisted.outcomes();
    for (id, channel, (event_seq, msg_seq)) in &probes {
        let got = outcomes
            .get(id.as_str())
            .and_then(|o| o.first())
            .map(|e| (e.event_seq, e.msg_seq));
        if got != Some((event_seq + 1, msg_seq + 1)) {
            report.fail(
                "sequencer head differs from Cassandra",
                format!("channel {channel}: Cassandra head ({event_seq}, {msg_seq}), probe sequenced as {got:?}"),
            );
        }
    }
    drop(outcomes);

    // Wait for live delivery to settle (a fan-out restarted near the end may still be catching up).
    let canon = check::canonical(&persisted);
    let members: HashMap<i64, Vec<HashSet<i64>>> = persisted
        .by_channel
        .iter()
        .map(|(&c, e)| (c, check::membership(e)))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let missing: usize = users
            .iter()
            .map(|u| {
                let view = u.view.lock().unwrap();
                canon
                    .iter()
                    .filter(|(c, events)| {
                        members[c].last().is_some_and(|m| m.contains(&u.id)) && !events.is_empty()
                    })
                    .filter(|(c, events)| {
                        let last = events.last().unwrap().event_seq;
                        !view
                            .live
                            .iter()
                            .any(|(_, e)| e.channel_id == **c && e.event_seq == last)
                    })
                    .count()
            })
            .sum();
        if missing == 0 || Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let _ = stop_tx.send(true);

    // The checks.
    let removal = removal.lock().unwrap().clone();
    {
        let ledger = ledger.lock().unwrap();
        check::sequence(&persisted, &ledger, &removal, &mut report);
    }
    check::store(&persisted, &db, &mut report).await?;
    let mut delivery = Delivery::default();
    let mut resyncs = 0;
    let mut reconnects = 0;
    for u in users.iter() {
        let (received, rejected) = {
            let view = u.view.lock().unwrap();
            resyncs += view.resyncs;
            reconnects += view.reconnects;
            (view.live.clone(), view.send_rejected.clone())
        };
        for r in rejected {
            report.fail("send refused by the gateway", format!("user {}: {r}", u.id));
        }
        let seen = check::live(
            u.id,
            &received,
            &canon,
            &members,
            &removal,
            &mut delivery,
            &mut report,
        );
        for (&channel, events) in &canon {
            let member_at_end = members[&channel].last().is_some_and(|m| m.contains(&u.id));
            let had = seen.get(&channel).cloned().unwrap_or_default();
            if u.id == removal.victim && channel == removal.channel {
                // Up to their removal; history is closed to them now.
                if !removal.left_seq.is_some_and(|l| had.contains(&l)) {
                    report.fail(
                        "removed member never received member.left",
                        format!("user {}", u.id),
                    );
                }
                match api::history(&urls.messages, &u.token, channel, 0).await? {
                    api::History::Status(404) => {}
                    api::History::Status(s) => {
                        report.fail("history open to a removed member", format!("status {s}"))
                    }
                    api::History::Events(e) => report.fail(
                        "history open to a removed member",
                        format!("{} events", e.len()),
                    ),
                }
                for id in &removal.after {
                    let delivered = received.iter().any(|(_, e)| {
                        &e.dedupe_id == id && matches!(e.payload, Some(Payload::Rejected(_)))
                    });
                    if !delivered {
                        report.fail("rejection not delivered to the removed member", id.clone());
                    }
                }
                continue;
            }
            if !member_at_end {
                continue;
            }
            delivery.expected += events
                .iter()
                .filter(|e| members[&channel][e.event_seq as usize].contains(&u.id))
                .count() as u64;
            delivery.live += had.len() as u64;
            // Catch up after the last contiguous event, as a client does after a resync.
            let prefix = (1..).take_while(|s| had.contains(s)).last().unwrap_or(0);
            let mut view: HashMap<i64, String> = received
                .iter()
                .filter(|(_, e)| e.channel_id == channel && e.event_seq > 0)
                .map(|(_, e)| (e.event_seq, check::summary(e)))
                .collect();
            match api::history(&urls.messages, &u.token, channel, prefix).await? {
                api::History::Events(fetched) => {
                    for e in fetched {
                        if !had.contains(&e.event_seq) {
                            delivery.caught_up += 1;
                        }
                        let s = check::summary(&e);
                        if let Some(prev) = view.insert(e.event_seq, s.clone())
                            && prev != s
                        {
                            report.fail("history differs from live", format!("user {} channel {channel} event_seq {}: live {prev}, history {s}", u.id, e.event_seq));
                        }
                    }
                }
                api::History::Status(s) => report.fail(
                    "history refused to a member",
                    format!("user {} channel {channel}: {s}", u.id),
                ),
            }
            let expected: Vec<(i64, String)> = events
                .iter()
                .map(|e| (e.event_seq, check::summary(e)))
                .collect();
            let mut got: Vec<(i64, String)> = view.into_iter().collect();
            got.sort();
            if got != expected {
                let missing = expected.iter().filter(|e| !got.contains(e)).count();
                report.fail(
                    "member's view differs from the sequencer",
                    format!(
                        "user {} channel {channel}: {} events, expected {} ({missing} missing)",
                        u.id,
                        got.len(),
                        expected.len()
                    ),
                );
            }
        }
    }

    // A graceful stop must be clean: exit 0 (the JVM reports 143 after running its
    // shutdown hooks). Dying from the signal means the consumer never left its group.
    let kills = kills.lock().unwrap();
    for k in kills.iter().filter(|k| k.signal == "SIGTERM") {
        let clean = k.exit.is_some_and(|s| s.success() || s.code() == Some(143));
        if !clean {
            report.fail(
                "graceful stop not clean",
                format!("{}: {:?}", k.target, k.exit),
            );
        }
    }

    // Summary.
    let ledger = ledger.lock().unwrap();
    let messages: usize = canon
        .values()
        .map(|e| {
            e.iter()
                .filter(|e| matches!(e.payload, Some(Payload::MessageCreated(_))))
                .count()
        })
        .sum();
    let rejections = persisted
        .outcomes()
        .values()
        .flatten()
        .filter(|e| matches!(e.payload, Some(Payload::Rejected(_))))
        .count();
    println!(
        "\nsummary ({:.0}s total, {sending_secs:.0}s of sending under chaos, seed {seed})",
        started.elapsed().as_secs_f64()
    );
    println!(
        "  users {}, channels {}, senders {} at {}/s",
        args.users, args.channels, args.senders, args.rate
    );
    println!(
        "  sends {} ({} retries of unacked sends, {} deliberate repeats of acked ones); messages sequenced {messages}; rejections {rejections}",
        ledger.sends.len(),
        ledger.retries,
        ledger.duplicates
    );
    let mut by_target: HashMap<&str, (u32, u32)> = HashMap::new();
    for k in kills.iter() {
        let e = by_target.entry(&k.target).or_default();
        if k.signal == "SIGKILL" {
            e.0 += 1
        } else {
            e.1 += 1
        }
    }
    let mut targets: Vec<_> = by_target.into_iter().collect();
    targets.sort();
    println!(
        "  restarts {}: {}",
        kills.len(),
        targets
            .iter()
            .map(|(t, (k, s))| format!("{t} {k}×SIGKILL {s}×SIGTERM"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    if let Some(slowest) = kills.iter().max_by_key(|k| k.ready_after) {
        let down: f64 = kills.iter().map(|k| k.down.as_secs_f64()).sum();
        println!(
            "  slowest restart: {} ({:.1}s to ready); total downtime {down:.0}s",
            slowest.target,
            slowest.ready_after.as_secs_f64()
        );
    }
    println!(
        "  live delivery: {}/{} member events ({:.1}%), {} redelivered, {} caught up via history, {} resyncs, {} reconnects",
        delivery.live,
        delivery.expected,
        100.0 * delivery.live as f64 / delivery.expected.max(1) as f64,
        delivery.redelivered,
        delivery.caught_up,
        resyncs,
        reconnects
    );
    println!(
        "  removal: user {} removed from channel {} at event_seq {:?}; {} sends after removal rejected; {} stale redeliveries (from before the removal)",
        removal.victim,
        removal.channel,
        removal.left_seq,
        removal.after.len(),
        delivery.stale_after_removal
    );
    if report.count() == 0 {
        println!("  no violations");
        Ok(())
    } else {
        report.print();
        Err(format!("{} violations", report.count()))
    }
}

/// Waits until every send is acked, has an outcome in the sequencer's output, and
/// Cassandra's heads match that output.
async fn quiesce(
    ledger: &Mutex<Ledger>,
    persisted: &mut Persisted,
    db: &Cassandra,
) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let unacked = ledger.lock().unwrap().unacked();
        if unacked == 0 {
            break;
        }
        if Instant::now() > deadline {
            return Err(format!("{unacked} sends still unacked"));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    loop {
        tokio::task::block_in_place(|| persisted.drain())?;
        let outcomes = persisted.outcomes();
        let pending = ledger
            .lock()
            .unwrap()
            .sends
            .keys()
            .filter(|id| !outcomes.contains_key(id.as_str()))
            .count();
        if pending == 0 {
            break;
        }
        if Instant::now() > deadline {
            // The checks report each one.
            println!("  {pending} acked sends have no outcome in messages.persisted");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    let channels: Vec<i64> = persisted.by_channel.keys().copied().collect();
    loop {
        let mut behind = 0;
        for &c in &channels {
            let (e, m) = persisted.head(c);
            if db.head(c).await? != Some((e, m)) {
                behind += 1;
            }
        }
        if behind == 0 || Instant::now() > deadline {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
