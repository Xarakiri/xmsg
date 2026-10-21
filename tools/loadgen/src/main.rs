// SPDX-License-Identifier: AGPL-3.0-only
//! Load generator for the xmsg gateway (Phase 0 capacity spike, plan §14).
//!
//! Opens many WebSocket connections (dev tokens `dev:<user_id>`), keeps them
//! alive with pings, and, with `--nats`, publishes fan-out batches straight to
//! the gateway's NATS subject to measure delivery throughput and latency.
//!
//! Each delivered frame carries its send time (microseconds since the Unix
//! epoch) in `MessageCreated.message_id`, and pings carry microseconds in
//! `Ping.sent_at_ms`; both are loadgen-only conventions. Run the load generator
//! on the same machine as the publisher so the clocks agree.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    ClientFrame, Connect, Delivery, GatewayBatch, MessageCreated, Ping, SequencedEvent,
    ServerFrame, StreamHead, StreamHeads, client_frame, sequenced_event, server_frame,
};

#[derive(Parser, Debug, Clone)]
#[command(about = "WebSocket load generator for the xmsg gateway")]
struct Args {
    /// Gateway WebSocket URL.
    #[arg(long, default_value = "ws://127.0.0.1:4000/ws")]
    url: String,
    /// Number of connections to open.
    #[arg(long, default_value_t = 1_000)]
    connections: u64,
    /// New connections per second while ramping up.
    #[arg(long, default_value_t = 500)]
    ramp: u64,
    /// Seconds to keep running after the ramp-up finishes.
    #[arg(long, default_value_t = 30)]
    duration: u64,
    /// First user ID; connection i uses `dev:<user_start + i>`.
    #[arg(long, default_value_t = 1)]
    user_start: i64,
    /// Seconds between pings on each connection.
    #[arg(long, default_value_t = 10)]
    ping_every: u64,
    /// NATS URL; when set, publish delivery batches to `gw.<gateway-id>`.
    #[arg(long)]
    nats: Option<String>,
    /// Gateway ID used in NATS subjects.
    #[arg(long, default_value = "local")]
    gateway_id: String,
    /// Batches published per second (each batch = one event for `recipients` users).
    #[arg(long, default_value_t = 100)]
    batches_per_sec: u64,
    /// Recipients per batch, chosen among connected users.
    #[arg(long, default_value_t = 50)]
    recipients: u64,
    /// Fan-out partitions to spread batches over (one stream each, like real fan-out).
    #[arg(long, default_value_t = 16)]
    partitions: u32,
}

#[derive(Default)]
struct Counters {
    connected: AtomicU64,
    failed: AtomicU64,
    closed: AtomicU64,
    frames: AtomicU64,
    resyncs: AtomicU64,
    published_frames: AtomicU64,
}

enum Sample {
    Connect(u64),
    Rtt(u64),
    Delivery(u64),
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_micros() as u64
}

fn histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).expect("histogram bounds")
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let counters = Arc::new(Counters::default());
    let (stop_tx, stop_rx) = watch::channel(false);
    let (sample_tx, sample_rx) = mpsc::unbounded_channel();

    let reporter = tokio::spawn(report(counters.clone(), sample_rx, stop_rx.clone()));

    if let Some(nats) = args.nats.clone() {
        tokio::spawn(publish(
            args.clone(),
            nats,
            counters.clone(),
            stop_rx.clone(),
        ));
    }

    let started = Instant::now();
    let gap = Duration::from_secs_f64(1.0 / args.ramp.max(1) as f64);
    let mut next = tokio::time::Instant::now();
    for i in 0..args.connections {
        let user_id = args.user_start + i as i64;
        tokio::spawn(connection(
            args.url.clone(),
            user_id,
            Duration::from_secs(args.ping_every.max(1)),
            counters.clone(),
            sample_tx.clone(),
            stop_rx.clone(),
        ));
        next += gap;
        tokio::time::sleep_until(next).await;
    }
    eprintln!(
        "ramp-up done: {} connections attempted in {:.1}s",
        args.connections,
        started.elapsed().as_secs_f64()
    );

    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(args.duration)) => {}
        _ = tokio::signal::ctrl_c() => eprintln!("interrupted"),
    }
    let _ = stop_tx.send(true);
    drop(sample_tx);
    let _ = reporter.await;
}

async fn connection(
    url: String,
    user_id: i64,
    ping_every: Duration,
    counters: Arc<Counters>,
    samples: mpsc::UnboundedSender<Sample>,
    mut stop: watch::Receiver<bool>,
) {
    let start = Instant::now();
    let Ok((ws, _)) = connect_async(url.as_str()).await else {
        counters.failed.fetch_add(1, Relaxed);
        return;
    };
    let (mut sink, mut stream) = ws.split();

    let connect = ClientFrame {
        frame: Some(client_frame::Frame::Connect(Connect {
            access_token: format!("dev:{user_id}"),
            protocol_version: 1,
            ..Default::default()
        })),
    };
    if sink
        .send(WsMessage::Binary(connect.encode_to_vec().into()))
        .await
        .is_err()
    {
        counters.failed.fetch_add(1, Relaxed);
        return;
    }

    // Wait for Welcome.
    let welcomed = tokio::time::timeout(Duration::from_secs(15), async {
        while let Some(Ok(msg)) = stream.next().await {
            if let WsMessage::Binary(bytes) = msg
                && let Ok(ServerFrame {
                    frame: Some(server_frame::Frame::Welcome(_)),
                }) = ServerFrame::decode(bytes.as_ref())
            {
                return true;
            }
        }
        false
    })
    .await;
    if !matches!(welcomed, Ok(true)) {
        counters.failed.fetch_add(1, Relaxed);
        return;
    }
    counters.connected.fetch_add(1, Relaxed);
    let _ = samples.send(Sample::Connect(start.elapsed().as_micros() as u64));

    let mut ticker = tokio::time::interval(ping_every);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let ping = ClientFrame { frame: Some(client_frame::Frame::Ping(Ping { sent_at_ms: now_us() as i64 })) };
                if sink.send(WsMessage::Binary(ping.encode_to_vec().into())).await.is_err() {
                    break;
                }
            }
            msg = stream.next() => match msg {
                Some(Ok(WsMessage::Binary(bytes))) => handle_frame(&bytes, &counters, &samples),
                Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = stop.changed() => {
                let _ = sink.close().await;
                return;
            }
        }
    }
    counters.connected.fetch_sub(1, Relaxed);
    counters.closed.fetch_add(1, Relaxed);
}

fn handle_frame(bytes: &[u8], counters: &Counters, samples: &mpsc::UnboundedSender<Sample>) {
    let Ok(frame) = ServerFrame::decode(bytes) else {
        return;
    };
    match frame.frame {
        Some(server_frame::Frame::Pong(pong)) => {
            let _ = samples.send(Sample::Rtt(now_us().saturating_sub(pong.sent_at_ms as u64)));
        }
        Some(server_frame::Frame::Event(SequencedEvent {
            payload: Some(sequenced_event::Payload::MessageCreated(created)),
            ..
        })) => {
            counters.frames.fetch_add(1, Relaxed);
            let _ = samples.send(Sample::Delivery(
                now_us().saturating_sub(created.message_id as u64),
            ));
        }
        Some(server_frame::Frame::Resync(_)) => {
            counters.resyncs.fetch_add(1, Relaxed);
        }
        _ => {}
    }
}

/// Publishes GatewayBatch messages as fan-out would, plus StreamHeads every second.
async fn publish(
    args: Args,
    url: String,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) {
    let client = match async_nats::connect(url.as_str()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("NATS connect to {url} failed: {e}");
            return;
        }
    };
    let subject = format!("gw.{}", args.gateway_id);
    let heads_subject = format!("stream_heads.{}", args.gateway_id);
    let owner_epoch = now_us();
    let users = args.connections.max(1);
    let mut rng = XorShift(now_us() | 1);
    let partitions = args.partitions.max(1);
    let mut seqs = vec![0_u64; partitions as usize];
    let mut next_partition: u32 = 0;
    let mut credit = 0.0_f64;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    let mut heads = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tick.tick() => {
                credit += args.batches_per_sec as f64 / 100.0;
                while credit >= 1.0 {
                    credit -= 1.0;
                    let partition = next_partition;
                    next_partition = (next_partition + 1) % partitions;
                    seqs[partition as usize] += 1;
                    let seq = seqs[partition as usize];
                    let event = ServerFrame {
                        frame: Some(server_frame::Frame::Event(SequencedEvent {
                            payload: Some(sequenced_event::Payload::MessageCreated(MessageCreated {
                                message_id: now_us() as i64,
                                body: String::new(),
                            })),
                            ..Default::default()
                        })),
                    };
                    // Distinct recipients: the gateway delivers once per user, so repeats
                    // would be counted as published but never received.
                    let wanted = args.recipients.min(users) as usize;
                    let mut user_ids: Vec<i64> = Vec::with_capacity(wanted);
                    while user_ids.len() < wanted {
                        let id = args.user_start + (rng.next() % users) as i64;
                        if !user_ids.contains(&id) {
                            user_ids.push(id);
                        }
                    }
                    let batch = GatewayBatch {
                        partition,
                        owner_epoch,
                        seq,
                        deliveries: vec![Delivery { user_ids, server_frame: event.encode_to_vec() }],
                    };
                    if client.publish(subject.clone(), batch.encode_to_vec().into()).await.is_err() {
                        eprintln!("NATS publish failed");
                        return;
                    }
                    counters.published_frames.fetch_add(args.recipients.min(users), Relaxed);
                }
            }
            _ = heads.tick() => {
                let heads = seqs.iter().enumerate().map(|(p, &seq)| StreamHead { partition: p as u32, owner_epoch, seq }).collect();
                let msg = StreamHeads { heads };
                let _ = client.publish(heads_subject.clone(), msg.encode_to_vec().into()).await;
            }
            _ = stop.changed() => {
                let _ = client.flush().await;
                return;
            }
        }
    }
}

async fn report(
    counters: Arc<Counters>,
    mut samples: mpsc::UnboundedReceiver<Sample>,
    mut stop: watch::Receiver<bool>,
) {
    let (mut connect, mut rtt, mut delivery) = (histogram(), histogram(), histogram());
    let (mut window_delivery, mut last_frames, mut last_published) = (histogram(), 0, 0);
    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.tick().await;
    println!(
        "{:>5} {:>8} {:>6} {:>6} {:>10} {:>10} {:>9} {:>9} {:>9} {:>7}",
        "t(s)",
        "conn",
        "failed",
        "closed",
        "pub fr/s",
        "recv fr/s",
        "dlv p50",
        "dlv p99",
        "rtt p99",
        "resyncs"
    );
    loop {
        tokio::select! {
            sample = samples.recv() => match sample {
                Some(Sample::Connect(v)) => { let _ = connect.record(v.max(1)); }
                Some(Sample::Rtt(v)) => { let _ = rtt.record(v.max(1)); }
                Some(Sample::Delivery(v)) => {
                    let _ = delivery.record(v.max(1));
                    let _ = window_delivery.record(v.max(1));
                }
                None => break,
            },
            _ = tick.tick() => {
                let frames = counters.frames.load(Relaxed);
                let published = counters.published_frames.load(Relaxed);
                println!("{:>5} {:>8} {:>6} {:>6} {:>10} {:>10} {:>9} {:>9} {:>9} {:>7}",
                    started.elapsed().as_secs(),
                    counters.connected.load(Relaxed),
                    counters.failed.load(Relaxed),
                    counters.closed.load(Relaxed),
                    published - last_published,
                    frames - last_frames,
                    fmt_us(window_delivery.value_at_quantile(0.5)),
                    fmt_us(window_delivery.value_at_quantile(0.99)),
                    fmt_us(rtt.value_at_quantile(0.99)),
                    counters.resyncs.load(Relaxed));
                last_frames = frames;
                last_published = published;
                window_delivery.reset();
            }
            _ = stop.changed() => break,
        }
    }
    println!("\nsummary after {:.0}s", started.elapsed().as_secs_f64());
    println!(
        "  connections: {} open, {} failed, {} closed early",
        counters.connected.load(Relaxed),
        counters.failed.load(Relaxed),
        counters.closed.load(Relaxed)
    );
    println!(
        "  connect (handshake + welcome): p50 {} p99 {} max {}",
        fmt_us(connect.value_at_quantile(0.5)),
        fmt_us(connect.value_at_quantile(0.99)),
        fmt_us(connect.max())
    );
    println!(
        "  ping RTT: p50 {} p99 {} max {} ({} samples)",
        fmt_us(rtt.value_at_quantile(0.5)),
        fmt_us(rtt.value_at_quantile(0.99)),
        fmt_us(rtt.max()),
        rtt.len()
    );
    println!(
        "  delivery: {} frames received of {} published; latency p50 {} p99 {} max {}",
        counters.frames.load(Relaxed),
        counters.published_frames.load(Relaxed),
        fmt_us(delivery.value_at_quantile(0.5)),
        fmt_us(delivery.value_at_quantile(0.99)),
        fmt_us(delivery.max())
    );
    println!("  resyncs received: {}", counters.resyncs.load(Relaxed));
}

fn fmt_us(us: u64) -> String {
    if us == 0 {
        "-".into()
    } else if us < 1_000 {
        format!("{us}µs")
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

/// Small deterministic PRNG; good enough for picking recipients.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}
