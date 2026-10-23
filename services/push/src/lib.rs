// SPDX-License-Identifier: AGPL-3.0-only
//! Push notifications (plan §5.11): Web Push first (VAPID, RFC 8291), APNs/FCM later.
//!
//! For each new message the push service decides who should be told, from rules that
//! live here (pure and tested):
//! - never the author;
//! - not someone online and active right now (they see it in the app);
//! - by their channel setting: **muted** or **nothing** → no push; **mentions** → only
//!   when mentioned (or `@channel`); **all** → every message;
//! - in channels with more than `LARGE_CHANNEL` members, only mentions (plan §6.7).

use serde_json::{Value, json};

/// Above this many members a channel only pushes mentions.
pub const LARGE_CHANNEL: usize = 1_000;
/// Notification text is cut here (the app has the rest).
pub const PREVIEW_CHARS: usize = 140;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Level {
    All,
    Mentions,
    Nothing,
}

impl Level {
    pub fn parse(s: Option<&str>) -> Level {
        match s {
            Some("mentions") => Level::Mentions,
            Some("nothing") => Level::Nothing,
            _ => Level::All,
        }
    }
}

/// One member's view of a message.
#[derive(Debug, Clone, Copy)]
pub struct Recipient {
    pub level: Level,
    pub muted: bool,
    pub mentioned: bool,
    pub online: bool,
}

/// Whether this member gets a push for this message.
pub fn should_notify(r: Recipient, large_channel: bool) -> bool {
    if r.online || r.muted {
        return false;
    }
    match r.level {
        Level::Nothing => false,
        Level::Mentions => r.mentioned,
        Level::All => r.mentioned || !large_channel,
    }
}

/// The notification payload the service worker shows. `tag` makes the browser replace
/// rather than repeat a notification if the same message is pushed twice (at least once).
pub fn payload(
    channel_id: i64,
    channel_name: &str,
    msg_seq: i64,
    message_id: i64,
    body: &str,
) -> Value {
    let mut text: String = body.chars().take(PREVIEW_CHARS).collect();
    if body.chars().count() > PREVIEW_CHARS {
        text.push('…');
    }
    json!({
        "title": if channel_name.is_empty() { "New message".to_string() } else { format!("#{channel_name}") },
        "body": text,
        "channel_id": channel_id.to_string(),
        "msg_seq": msg_seq,
        "tag": message_id.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(level: Level) -> Recipient {
        Recipient {
            level,
            muted: false,
            mentioned: false,
            online: false,
        }
    }

    #[test]
    fn settings_presence_and_mentions_decide_who_is_told() {
        assert!(should_notify(r(Level::All), false));
        assert!(
            !should_notify(
                Recipient {
                    online: true,
                    ..r(Level::All)
                },
                false
            ),
            "active in the app"
        );
        assert!(!should_notify(
            Recipient {
                muted: true,
                mentioned: true,
                ..r(Level::All)
            },
            false
        ));
        assert!(!should_notify(r(Level::Nothing), false));
        assert!(!should_notify(r(Level::Mentions), false));
        assert!(should_notify(
            Recipient {
                mentioned: true,
                ..r(Level::Mentions)
            },
            false
        ));
        assert!(
            !should_notify(r(Level::All), true),
            "large channels: mentions only"
        );
        assert!(should_notify(
            Recipient {
                mentioned: true,
                ..r(Level::All)
            },
            true
        ));
        assert_eq!(Level::parse(None), Level::All);
        assert_eq!(Level::parse(Some("mentions")), Level::Mentions);
    }

    #[test]
    fn payloads_are_short_and_tagged_by_message() {
        let long = "x".repeat(200);
        let p = payload(7, "design", 3, 500, &long);
        assert_eq!(p["title"], "#design");
        assert_eq!(
            p["body"].as_str().unwrap().chars().count(),
            PREVIEW_CHARS + 1
        );
        assert_eq!(p["tag"], "500");
        assert_eq!(payload(7, "", 3, 500, "hi")["title"], "New message");
    }
}
