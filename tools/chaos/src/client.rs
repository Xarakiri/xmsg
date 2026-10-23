// SPDX-License-Identifier: AGPL-3.0-only
//! A WebSocket client that behaves like the real one (clients/core): sends wait in an
//! outbox until the gateway acks them and are resent with the same `client_msg_id`
//! after a timeout or a reconnect. Everything received is recorded for the checks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message as Ws};
use xmsg_proto::Message;
use xmsg_proto::v1::{
    ClientFrame, Connect, Ping, SendMessageRequest, SequencedEvent, ServerFrame, client_frame,
    server_frame,
};

/// Every send made during the run, by `client_msg_id`.
#[derive(Default)]
pub struct Ledger {
    pub sends: HashMap<String, SendRecord>,
    /// Resends of unacked sends (timeouts and reconnects).
    pub retries: u64,
    /// Deliberate repeats of already-acked sends (a lost ack, as far as the client knows).
    pub duplicates: u64,
}

pub struct SendRecord {
    pub user: i64,
    pub channel: i64,
    pub body: String,
    pub acked: bool,
    pub first_sent: Instant,
}

impl Ledger {
    pub fn unacked(&self) -> usize {
        self.sends.values().filter(|s| !s.acked).count()
    }
}

/// What one user's socket received.
#[derive(Default)]
pub struct View {
    pub live: Vec<(Instant, SequencedEvent)>,
    pub resyncs: u64,
    /// `SendRejected` frames (refused before the sequencer): never expected here.
    pub send_rejected: Vec<String>,
    pub reconnects: u64,
}

pub enum Cmd {
    Send {
        id: String,
        channel: i64,
        body: String,
    },
    /// Writes the frame again without touching the outbox.
    Repeat {
        id: String,
        channel: i64,
        body: String,
    },
}

pub struct User {
    pub id: i64,
    pub token: String,
    pub view: Arc<Mutex<View>>,
    pub tx: mpsc::UnboundedSender<Cmd>,
}

pub struct Shared {
    pub ledger: Arc<Mutex<Ledger>>,
    pub retry_after: Duration,
    pub stop: watch::Receiver<bool>,
}

impl User {
    pub fn spawn(gateway: String, id: i64, token: String, shared: &Shared) -> User {
        let (tx, rx) = mpsc::unbounded_channel();
        let view = Arc::new(Mutex::new(View::default()));
        tokio::spawn(run(
            gateway,
            token.clone(),
            rx,
            view.clone(),
            shared.ledger.clone(),
            shared.retry_after,
            shared.stop.clone(),
        ));
        User {
            id,
            token,
            view,
            tx,
        }
    }

    /// Registers the send in the ledger and hands it to the connection.
    pub fn send(&self, ledger: &Mutex<Ledger>, id: String, channel: i64, body: String) {
        ledger.lock().unwrap().sends.insert(
            id.clone(),
            SendRecord {
                user: self.id,
                channel,
                body: body.clone(),
                acked: false,
                first_sent: Instant::now(),
            },
        );
        let _ = self.tx.send(Cmd::Send { id, channel, body });
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

fn frame(f: client_frame::Frame) -> Ws {
    Ws::Binary(ClientFrame { frame: Some(f) }.encode_to_vec().into())
}

fn send_frame(id: &str, channel: i64, body: &str) -> Ws {
    frame(client_frame::Frame::SendMessage(SendMessageRequest {
        channel_id: channel,
        client_msg_id: id.into(),
        body: body.into(),
        ..Default::default()
    }))
}

async fn open(gateway: &str, token: &str) -> Result<Socket, String> {
    let (mut ws, _) = connect_async(gateway).await.map_err(|e| e.to_string())?;
    ws.send(frame(client_frame::Frame::Connect(Connect {
        access_token: token.into(),
        protocol_version: 1,
        ..Default::default()
    })))
    .await
    .map_err(|e| e.to_string())?;
    let welcome = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(Ok(msg)) = ws.next().await {
            if let Ws::Binary(b) = msg
                && let Ok(ServerFrame {
                    frame: Some(server_frame::Frame::Welcome(_)),
                }) = ServerFrame::decode(b.as_ref())
            {
                return true;
            }
        }
        false
    })
    .await;
    match welcome {
        Ok(true) => Ok(ws),
        _ => Err("no Welcome".into()),
    }
}

async fn run(
    gateway: String,
    token: String,
    mut rx: mpsc::UnboundedReceiver<Cmd>,
    view: Arc<Mutex<View>>,
    ledger: Arc<Mutex<Ledger>>,
    retry_after: Duration,
    mut stop: watch::Receiver<bool>,
) {
    // client_msg_id -> (channel, body, last sent)
    let mut outbox: HashMap<String, (i64, String, Instant)> = HashMap::new();
    let mut first = true;
    loop {
        let mut ws = match open(&gateway, &token).await {
            Ok(ws) => ws,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
        };
        if !first {
            view.lock().unwrap().reconnects += 1;
        }
        first = false;
        // Back online: resend the whole outbox (the sequencer drops repeats).
        let mut broken = false;
        for (id, (channel, body, sent)) in outbox.iter_mut() {
            if ws.send(send_frame(id, *channel, body)).await.is_err() {
                broken = true;
                break;
            }
            *sent = Instant::now();
            ledger.lock().unwrap().retries += 1;
        }
        let mut tick = tokio::time::interval(Duration::from_millis(250));
        // The gateway closes sockets that stay silent for a minute; the real client pings.
        let mut ping = tokio::time::interval(Duration::from_secs(20));
        while !broken {
            tokio::select! {
                msg = ws.next() => match msg {
                    Some(Ok(Ws::Binary(bytes))) => {
                        let Ok(ServerFrame { frame: Some(f) }) = ServerFrame::decode(bytes.as_ref()) else { continue };
                        match f {
                            server_frame::Frame::Ack(ack) => {
                                outbox.remove(&ack.client_msg_id);
                                if let Some(s) = ledger.lock().unwrap().sends.get_mut(&ack.client_msg_id) {
                                    s.acked = true;
                                }
                            }
                            server_frame::Frame::Event(e) => view.lock().unwrap().live.push((Instant::now(), e)),
                            server_frame::Frame::Resync(_) => view.lock().unwrap().resyncs += 1,
                            server_frame::Frame::SendRejected(r) => {
                                outbox.remove(&r.client_msg_id);
                                view.lock().unwrap().send_rejected.push(format!("{} {:?} {}", r.client_msg_id, r.reason, r.detail));
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Ws::Close(_))) | Some(Err(_)) | None => broken = true,
                    Some(Ok(_)) => {}
                },
                cmd = rx.recv() => {
                    let Some(cmd) = cmd else { return };
                    let (id, channel, body, keep) = match cmd {
                        Cmd::Send { id, channel, body } => (id, channel, body, true),
                        Cmd::Repeat { id, channel, body } => (id, channel, body, false),
                    };
                    if keep {
                        outbox.insert(id.clone(), (channel, body.clone(), Instant::now()));
                    }
                    if ws.send(send_frame(&id, channel, &body)).await.is_err() {
                        broken = true;
                    }
                }
                _ = tick.tick() => {
                    let now = Instant::now();
                    for (id, (channel, body, sent)) in outbox.iter_mut() {
                        if now.duration_since(*sent) >= retry_after {
                            if ws.send(send_frame(id, *channel, body)).await.is_err() {
                                broken = true;
                                break;
                            }
                            *sent = now;
                            ledger.lock().unwrap().retries += 1;
                        }
                    }
                }
                _ = ping.tick() => {
                    if ws.send(frame(client_frame::Frame::Ping(Ping { sent_at_ms: 0 }))).await.is_err() {
                        broken = true;
                    }
                }
                _ = stop.changed() => {
                    let _ = ws.close(None).await;
                    return;
                }
            }
        }
    }
}
