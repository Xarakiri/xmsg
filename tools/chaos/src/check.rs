// SPDX-License-Identifier: AGPL-3.0-only
//! Reading the results back (sequencer output, Cassandra, what clients saw) and the checks.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, Message as _, Offset, TopicPartitionList};
use xmsg_proto::Message;
use xmsg_proto::v1::{RejectReason, SequencedEvent, sequenced_event::Payload};

use crate::client::Ledger;

const PERSISTED: &str = "messages.persisted";

/// Violations by kind; any one fails the run.
#[derive(Default)]
pub struct Report {
    pub violations: BTreeMap<&'static str, Vec<String>>,
}

impl Report {
    pub fn fail(&mut self, kind: &'static str, detail: String) {
        self.violations.entry(kind).or_default().push(detail);
    }

    pub fn count(&self) -> usize {
        self.violations.values().map(Vec::len).sum()
    }

    pub fn print(&self) {
        for (kind, list) in &self.violations {
            println!("  ✗ {kind}: {}", list.len());
            for d in list.iter().take(10) {
                println!("      {d}");
            }
            if list.len() > 10 {
                println!("      … {} more", list.len() - 10);
            }
        }
    }
}

/// The sequencer's committed output for our channels, in partition order.
pub struct Persisted {
    consumer: BaseConsumer,
    partitions: usize,
    pub ours: HashSet<i64>,
    pub by_channel: HashMap<i64, Vec<SequencedEvent>>,
    /// Partitions whose end was reached and that had nothing since.
    at_end: HashSet<i32>,
}

impl Persisted {
    /// Reads from the offsets at `since_ms` (the run's start) on every partition.
    pub fn open(bootstrap: &str, since_ms: i64) -> Result<Self, String> {
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", bootstrap)
            .set("group.id", format!("xmsg-chaos-{since_ms}"))
            .set("enable.auto.commit", "false")
            .set("isolation.level", "read_committed")
            .set("enable.partition.eof", "true")
            .create()
            .map_err(|e| e.to_string())?;
        let md = consumer
            .fetch_metadata(Some(PERSISTED), Duration::from_secs(10))
            .map_err(|e| e.to_string())?;
        let partitions = md.topics().first().map_or(0, |t| t.partitions().len());
        if partitions == 0 {
            return Err(format!("{PERSISTED} has no partitions"));
        }
        let mut tpl = TopicPartitionList::new();
        for p in 0..partitions as i32 {
            tpl.add_partition_offset(PERSISTED, p, Offset::Offset(since_ms))
                .map_err(|e| e.to_string())?;
        }
        let mut offsets = consumer
            .offsets_for_times(tpl, Duration::from_secs(10))
            .map_err(|e| format!("offsets_for_times: {e}"))?;
        // Nothing after the timestamp: start at the high watermark, explicitly, so the
        // fetch reports the partition's end.
        for p in 0..partitions as i32 {
            let found = offsets.find_partition(PERSISTED, p).map(|e| e.offset());
            if !matches!(found, Some(Offset::Offset(_))) {
                let (_, high) = consumer
                    .fetch_watermarks(PERSISTED, p, Duration::from_secs(10))
                    .map_err(|e| e.to_string())?;
                offsets
                    .set_partition_offset(PERSISTED, p, Offset::Offset(high))
                    .map_err(|e| e.to_string())?;
            }
        }
        consumer.assign(&offsets).map_err(|e| e.to_string())?;
        Ok(Persisted {
            consumer,
            partitions,
            ours: HashSet::new(),
            by_channel: HashMap::new(),
            at_end: HashSet::new(),
        })
    }

    /// Reads until every partition is at its end (the last stable offset) and nothing
    /// new arrived for a moment. An end is reported once per catch-up, so it's remembered.
    pub fn drain(&mut self) -> Result<(), String> {
        let at_end = &mut self.at_end;
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut quiet_since = Instant::now();
        while at_end.len() < self.partitions || quiet_since.elapsed() < Duration::from_millis(500) {
            if Instant::now() > deadline {
                return Err(format!(
                    "{PERSISTED}: only {}/{} partitions read to the end",
                    at_end.len(),
                    self.partitions
                ));
            }
            match self.consumer.poll(Duration::from_millis(200)) {
                Some(Ok(m)) => {
                    at_end.remove(&m.partition());
                    quiet_since = Instant::now();
                    if let Some(Ok(e)) = m.payload().map(SequencedEvent::decode)
                        && self.ours.contains(&e.channel_id)
                    {
                        self.by_channel.entry(e.channel_id).or_default().push(e);
                    }
                }
                Some(Err(KafkaError::PartitionEOF(p))) => {
                    at_end.insert(p);
                }
                Some(Err(e)) => eprintln!("  persisted reader: {e}"),
                None => {}
            }
        }
        Ok(())
    }

    /// The last (event_seq, msg_seq) the sequencer emitted for a channel.
    pub fn head(&self, channel: i64) -> (i64, i64) {
        let events = self
            .by_channel
            .get(&channel)
            .map(Vec::as_slice)
            .unwrap_or_default();
        (
            events.iter().map(|e| e.event_seq).max().unwrap_or(0),
            events.iter().map(|e| e.msg_seq).max().unwrap_or(0),
        )
    }

    /// Outcome events (a message or a rejection) per client_msg_id.
    pub fn outcomes(&self) -> HashMap<&str, Vec<&SequencedEvent>> {
        let mut out: HashMap<&str, Vec<&SequencedEvent>> = HashMap::new();
        for e in self.by_channel.values().flatten() {
            if matches!(
                e.payload,
                Some(Payload::MessageCreated(_)) | Some(Payload::Rejected(_))
            ) {
                out.entry(e.dedupe_id.as_str()).or_default().push(e);
            }
        }
        out
    }
}

pub struct Cassandra {
    db: scylla::client::session::Session,
}

pub struct StoredMessage {
    pub msg_seq: i64,
    pub event_seq: i64,
    pub author: i64,
    pub client_msg_id: String,
    pub body: String,
}

fn bucket(seq: i64) -> i32 {
    (seq / 10_000) as i32
}

impl Cassandra {
    pub async fn connect(nodes: &str) -> Result<Self, String> {
        let db = scylla::client::session_builder::SessionBuilder::new()
            .known_nodes(nodes.split(','))
            .build()
            .await
            .map_err(|e| format!("connecting to {nodes}: {e}"))?;
        Ok(Cassandra { db })
    }

    pub async fn head(&self, channel: i64) -> Result<Option<(i64, i64)>, String> {
        self.db
            .query_unpaged(
                "SELECT event_seq, msg_seq FROM xmsg.channel_heads WHERE channel_id = ?",
                (channel,),
            )
            .await
            .map_err(|e| e.to_string())?
            .into_rows_result()
            .map_err(|e| e.to_string())?
            .maybe_first_row::<(Option<i64>, Option<i64>)>()
            .map(|r| r.map(|(e, m)| (e.unwrap_or(0), m.unwrap_or(0))))
            .map_err(|e| e.to_string())
    }

    pub async fn messages(&self, channel: i64, max_seq: i64) -> Result<Vec<StoredMessage>, String> {
        let mut out = Vec::new();
        for b in 0..=bucket(max_seq) {
            let rows = self
                .db
                .query_unpaged(
                    "SELECT msg_seq, event_seq, author_id, client_msg_id, body FROM xmsg.messages WHERE channel_id = ? AND bucket = ?",
                    (channel, b),
                )
                .await
                .map_err(|e| e.to_string())?
                .into_rows_result()
                .map_err(|e| e.to_string())?;
            for row in rows
                .rows::<(i64, i64, i64, Option<String>, Option<String>)>()
                .map_err(|e| e.to_string())?
            {
                let (msg_seq, event_seq, author, id, body) = row.map_err(|e| e.to_string())?;
                out.push(StoredMessage {
                    msg_seq,
                    event_seq,
                    author,
                    client_msg_id: id.unwrap_or_default(),
                    body: body.unwrap_or_default(),
                });
            }
        }
        Ok(out)
    }

    pub async fn event_seqs(&self, channel: i64, max_seq: i64) -> Result<Vec<i64>, String> {
        let mut out = Vec::new();
        for b in 0..=bucket(max_seq) {
            let rows = self
                .db
                .query_unpaged(
                    "SELECT event_seq FROM xmsg.channel_events WHERE channel_id = ? AND bucket = ?",
                    (channel, b),
                )
                .await
                .map_err(|e| e.to_string())?
                .into_rows_result()
                .map_err(|e| e.to_string())?;
            for row in rows.rows::<(i64,)>().map_err(|e| e.to_string())? {
                out.push(row.map_err(|e| e.to_string())?.0);
            }
        }
        out.sort_unstable();
        Ok(out)
    }
}

/// The member removed mid-run.
#[derive(Clone, Default)]
pub struct Removal {
    pub channel: i64,
    pub victim: i64,
    /// event_seq of the `member.left`, once the victim's socket received it.
    pub left_seq: Option<i64>,
    pub seen_at: Option<Instant>,
    /// Sends the victim made after seeing its removal.
    pub after: Vec<String>,
}

/// What identifies an event's content, for comparing live and history copies.
pub fn summary(e: &SequencedEvent) -> String {
    match &e.payload {
        Some(Payload::MessageCreated(m)) => {
            format!(
                "message msg_seq={} id={} body={:?}",
                e.msg_seq, e.dedupe_id, m.body
            )
        }
        Some(Payload::ChannelCreated(c)) => format!("created members={}", c.members.len()),
        Some(Payload::MemberJoined(j)) => format!("joined {}", j.user_id),
        Some(Payload::MemberLeft(l)) => format!("left {}", l.user_id),
        Some(Payload::Rejected(r)) => format!("rejected {} {:?}", e.dedupe_id, r.reason),
        Some(Payload::ChannelModeChanged(m)) => format!("mode {:?}", m.mode),
        // History rebuilds an edit from the current row, so the body isn't compared.
        Some(Payload::MessageEdited(m)) => format!("edited msg_seq={}", m.msg_seq),
        Some(Payload::MessageDeleted(m)) => format!("deleted msg_seq={}", m.msg_seq),
        None => "empty".into(),
    }
}

/// (a) Per channel: event_seq 1..E and msg_seq 1..K in partition order; every acked send
/// has exactly one outcome; nothing was sequenced that wasn't sent.
pub fn sequence(p: &Persisted, ledger: &Ledger, removal: &Removal, report: &mut Report) {
    for (&channel, events) in &p.by_channel {
        let (mut last_event, mut last_msg) = (0, 0);
        for e in events.iter().filter(|e| e.event_seq > 0) {
            if e.event_seq != last_event + 1 {
                let kind = if e.event_seq <= last_event {
                    "duplicate event_seq"
                } else {
                    "event_seq gap"
                };
                report.fail(
                    kind,
                    format!(
                        "channel {channel}: event_seq {} after {last_event}",
                        e.event_seq
                    ),
                );
            }
            last_event = last_event.max(e.event_seq);
            if let Some(Payload::MessageCreated(m)) = &e.payload {
                if e.msg_seq != last_msg + 1 {
                    let kind = if e.msg_seq <= last_msg {
                        "duplicate msg_seq"
                    } else {
                        "msg_seq gap"
                    };
                    report.fail(
                        kind,
                        format!(
                            "channel {channel}: msg_seq {} after {last_msg} ({})",
                            e.msg_seq, e.dedupe_id
                        ),
                    );
                }
                last_msg = last_msg.max(e.msg_seq);
                match ledger.sends.get(&e.dedupe_id) {
                    None => report.fail(
                        "sequenced but never sent",
                        format!("channel {channel}: {}", summary(e)),
                    ),
                    Some(s) if s.channel != channel || s.user != e.actor_id || s.body != m.body => {
                        report.fail(
                            "sequenced with the wrong content",
                            format!(
                                "{}: sent by {} to {} {:?}, got {}",
                                e.dedupe_id,
                                s.user,
                                s.channel,
                                s.body,
                                summary(e)
                            ),
                        )
                    }
                    Some(_) => {}
                }
                if channel == removal.channel
                    && e.actor_id == removal.victim
                    && removal.left_seq.is_some_and(|l| e.event_seq > l)
                {
                    report.fail(
                        "removed member's send accepted",
                        format!("channel {channel}: {}", summary(e)),
                    );
                }
            }
        }
    }
    let outcomes = p.outcomes();
    for (id, send) in &ledger.sends {
        if !send.acked {
            report.fail(
                "send never acked",
                format!("{id} by {} to {}", send.user, send.channel),
            );
            continue;
        }
        let got = outcomes
            .get(id.as_str())
            .map(Vec::as_slice)
            .unwrap_or_default();
        match got {
            [] => report.fail(
                "acked send lost",
                format!("{id} by {} to {}", send.user, send.channel),
            ),
            [e] => match &e.payload {
                Some(Payload::MessageCreated(_)) => {
                    let after_removal = send.user == removal.victim
                        && send.channel == removal.channel
                        && removal.seen_at.is_some_and(|t| send.first_sent > t);
                    if after_removal {
                        report.fail(
                            "removed member's send accepted",
                            format!("{id}: {}", summary(e)),
                        );
                    }
                }
                Some(Payload::Rejected(r)) => {
                    let expected = send.user == removal.victim
                        && send.channel == removal.channel
                        && r.reason == RejectReason::NotAMember as i32;
                    if !expected {
                        report.fail(
                            "unexpected rejection",
                            format!("{id} by {} to {}: {}", send.user, send.channel, summary(e)),
                        );
                    }
                }
                _ => {}
            },
            many => report.fail(
                "acked send sequenced more than once",
                format!(
                    "{id}: {}",
                    many.iter()
                        .map(|e| format!("event_seq {} ({})", e.event_seq, summary(e)))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ),
        }
    }
    for id in &removal.after {
        let rejected = outcomes.get(id.as_str()).is_some_and(|o| {
            o.iter().all(|e| matches!(&e.payload, Some(Payload::Rejected(r)) if r.reason == RejectReason::NotAMember as i32))
        });
        if !rejected {
            report.fail(
                "removed member's send not rejected NOT_A_MEMBER",
                id.clone(),
            );
        }
    }
}

/// (c) Cassandra holds exactly what the sequencer emitted, and the heads agree.
pub async fn store(p: &Persisted, db: &Cassandra, report: &mut Report) -> Result<(), String> {
    for (&channel, events) in &p.by_channel {
        let (event_seq, msg_seq) = p.head(channel);
        let head = db.head(channel).await?;
        if head != Some((event_seq, msg_seq)) {
            report.fail(
                "Cassandra head differs from the sequencer",
                format!(
                    "channel {channel}: Cassandra {head:?}, sequencer ({event_seq}, {msg_seq})"
                ),
            );
        }
        let mut expected: Vec<(i64, i64, i64, String, String)> = events
            .iter()
            .filter_map(|e| match &e.payload {
                Some(Payload::MessageCreated(m)) => Some((
                    e.msg_seq,
                    e.event_seq,
                    e.actor_id,
                    e.dedupe_id.clone(),
                    m.body.clone(),
                )),
                _ => None,
            })
            .collect();
        let mut stored: Vec<(i64, i64, i64, String, String)> = db
            .messages(channel, msg_seq)
            .await?
            .into_iter()
            .map(|m| (m.msg_seq, m.event_seq, m.author, m.client_msg_id, m.body))
            .collect();
        expected.sort();
        stored.sort();
        if expected != stored {
            let missing = expected.iter().filter(|m| !stored.contains(m)).count();
            let extra = stored.iter().filter(|m| !expected.contains(m)).count();
            report.fail(
                "Cassandra messages differ from the sequencer",
                format!(
                    "channel {channel}: {missing} missing, {extra} extra of {}",
                    expected.len()
                ),
            );
        }
        let seqs = db.event_seqs(channel, event_seq).await?;
        if seqs != (1..=event_seq).collect::<Vec<_>>() {
            report.fail(
                "Cassandra channel_events differ from the sequencer",
                format!(
                    "channel {channel}: {} rows for event_seq 1..{event_seq}",
                    seqs.len()
                ),
            );
        }
    }
    Ok(())
}

/// Members of a channel after each event_seq (index = event_seq).
pub fn membership(events: &[SequencedEvent]) -> Vec<HashSet<i64>> {
    let mut out = vec![HashSet::new()];
    let mut current = HashSet::new();
    for e in events.iter().filter(|e| e.event_seq > 0) {
        match &e.payload {
            Some(Payload::ChannelCreated(c)) => {
                current = c.members.iter().map(|m| m.user_id).collect()
            }
            Some(Payload::MemberJoined(j)) => {
                current.insert(j.user_id);
            }
            Some(Payload::MemberLeft(l)) => {
                current.remove(&l.user_id);
            }
            _ => {}
        }
        out.push(current.clone());
    }
    out
}

/// Live-delivery numbers for the summary.
#[derive(Default)]
pub struct Delivery {
    pub expected: u64,
    pub live: u64,
    pub redelivered: u64,
    pub caught_up: u64,
    pub stale_after_removal: u64,
}

/// Canonical events per channel, indexed by event_seq - 1.
pub fn canonical(p: &Persisted) -> HashMap<i64, Vec<SequencedEvent>> {
    p.by_channel
        .iter()
        .map(|(&c, events)| {
            let mut v: Vec<SequencedEvent> =
                events.iter().filter(|e| e.event_seq > 0).cloned().collect();
            v.sort_by_key(|e| e.event_seq);
            v.dedup_by_key(|e| e.event_seq);
            (c, v)
        })
        .collect()
}

/// (b) One user's live events: nothing from channels they weren't in at that event,
/// rejections only their own, and every copy identical to the sequencer's event.
/// Returns the event_seqs received per channel.
pub fn live(
    user: i64,
    received: &[(Instant, SequencedEvent)],
    canon: &HashMap<i64, Vec<SequencedEvent>>,
    members: &HashMap<i64, Vec<HashSet<i64>>>,
    removal: &Removal,
    stats: &mut Delivery,
    report: &mut Report,
) -> HashMap<i64, HashSet<i64>> {
    let mut seen: HashMap<i64, HashSet<i64>> = HashMap::new();
    let mut removed_at: Option<Instant> = None;
    for (at, e) in received {
        if e.event_seq <= 0 {
            if !matches!(e.payload, Some(Payload::Rejected(_))) || e.actor_id != user {
                report.fail(
                    "leak",
                    format!("user {user} got someone else's rejection: {}", summary(e)),
                );
            }
            continue;
        }
        let Some(events) = canon.get(&e.channel_id) else {
            report.fail(
                "leak",
                format!(
                    "user {user} got an event of unknown channel {}",
                    e.channel_id
                ),
            );
            continue;
        };
        let Some(c) = events.get(e.event_seq as usize - 1) else {
            report.fail(
                "delivered but never sequenced",
                format!("user {user}: channel {} {}", e.channel_id, summary(e)),
            );
            continue;
        };
        if summary(c) != summary(e) || c.event_seq != e.event_seq {
            report.fail(
                "delivered with the wrong content",
                format!(
                    "user {user} channel {} event_seq {}: got {}, sequenced {}",
                    e.channel_id,
                    e.event_seq,
                    summary(e),
                    summary(c)
                ),
            );
        }
        let allowed = members[&e.channel_id][e.event_seq as usize].contains(&user)
            || matches!(&e.payload, Some(Payload::MemberLeft(l)) if l.user_id == user);
        if !allowed {
            report.fail(
                "leak",
                format!(
                    "user {user} (not a member) got channel {} event_seq {}: {}",
                    e.channel_id,
                    e.event_seq,
                    summary(e)
                ),
            );
        }
        if user == removal.victim && e.channel_id == removal.channel {
            if let Some(t) = removed_at {
                // Redelivered after a fan-out restart, but from before the removal.
                if *at > t && removal.left_seq.is_some_and(|l| e.event_seq <= l) {
                    stats.stale_after_removal += 1;
                }
            }
            if matches!(&e.payload, Some(Payload::MemberLeft(l)) if l.user_id == user)
                && removed_at.is_none()
            {
                removed_at = Some(*at);
            }
        }
        if !seen.entry(e.channel_id).or_default().insert(e.event_seq) {
            stats.redelivered += 1;
        }
    }
    seen
}
