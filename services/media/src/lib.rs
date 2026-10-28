// SPDX-License-Identifier: AGPL-3.0-only
//! xmsg-media (ADR-0024): files and images in S3-compatible storage (Garage in the bundle).
//!
//! Clients upload and download through this service, never from the store directly: the
//! store stays private, every download checks that the caller is a member of the file's
//! channel, and nothing the store serves can run as our origin. Images get a thumbnail
//! at upload.

pub mod api;
pub mod cassandra;
pub mod garage;
pub mod lifecycle;
pub mod s3;
pub mod thumbnail;

use axum::body::Body;
use bytes::Bytes;
use serde::Serialize;

/// A stored file. Immutable once written; the object keys derive from it.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FileMeta {
    #[serde(serialize_with = "as_string")]
    pub file_id: i64,
    #[serde(serialize_with = "as_string")]
    pub channel_id: i64,
    #[serde(serialize_with = "as_string")]
    pub uploader_id: i64,
    pub name: String,
    /// What it is, as far as we can tell: sniffed for images, otherwise the uploader's claim.
    pub content_type: String,
    pub size: i64,
    pub sha256: String,
    pub created_at_ms: i64,
    /// Set for images we could decode.
    pub width: Option<i32>,
    pub height: Option<i32>,
    /// `image/jpeg` or `image/png` when there's a thumbnail.
    pub thumbnail_type: Option<String>,
    /// Its message was deleted: gone for everyone now, the bytes after the grace period.
    #[serde(skip)]
    pub deleted: bool,
    /// Whose storage quota it counts against (None: no workspace found).
    #[serde(skip)]
    pub workspace_id: Option<i64>,
}

/// IDs are JSON strings, as everywhere in the API (JavaScript numbers lose precision).
fn as_string<S: serde::Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&v.to_string())
}

impl FileMeta {
    pub fn object_key(&self) -> String {
        format!("{}/{}", self.channel_id, self.file_id)
    }
    pub fn thumbnail_key(&self) -> String {
        format!("{}/{}.thumb", self.channel_id, self.file_id)
    }
    /// Shown inline by browsers: only image formats we decoded ourselves.
    pub fn inline(&self) -> bool {
        INLINE_TYPES.contains(&self.content_type.as_str())
    }
}

/// Image types served inline (as themselves). Everything else is a download.
pub const INLINE_TYPES: [&str; 4] = ["image/png", "image/jpeg", "image/gif", "image/webp"];

/// A new file ID: milliseconds since 2025-01-01 in the high bits, 22 random bits below, so
/// IDs sort by time and can't be guessed from a neighbour's.
pub fn new_id(now_ms: i64) -> i64 {
    const EPOCH_MS: i64 = 1_735_689_600_000;
    ((now_ms - EPOCH_MS).max(0) << 22) | (rand::random::<u32>() & 0x3f_ffff) as i64
}

/// A file name safe to store and to put in a header: no path, no control characters, at
/// most 255 bytes, never empty.
pub fn clean_name(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or("");
    let mut out = String::new();
    for c in base.chars().filter(|c| !c.is_control()) {
        if out.len() + c.len_utf8() > 255 {
            break;
        }
        out.push(c);
    }
    let out = out.trim().trim_start_matches('.').to_string();
    if out.is_empty() { "file".into() } else { out }
}

/// The uploader's Content-Type, kept only if it looks like `type/subtype` (for display;
/// it never decides how the file is served).
pub fn clean_content_type(declared: Option<&str>) -> String {
    let declared = declared
        .and_then(|v| v.split(';').next())
        .map(|v| v.trim().to_ascii_lowercase())
        .unwrap_or_default();
    let token = |s: &str| {
        !s.is_empty()
            && s.len() <= 100
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$&-^_.+".contains(&b))
    };
    match declared.split_once('/') {
        Some((t, sub)) if token(t) && token(sub) => declared,
        _ => "application/octet-stream".into(),
    }
}

/// Where file metadata and channel membership are read (Cassandra in production).
pub trait Store: Send + Sync + 'static {
    fn is_member(
        &self,
        channel_id: i64,
        user_id: i64,
    ) -> impl Future<Output = Result<bool, String>> + Send;
    fn insert(&self, file: &FileMeta) -> impl Future<Output = Result<(), String>> + Send;
    fn get(&self, file_id: i64) -> impl Future<Output = Result<Option<FileMeta>, String>> + Send;

    // Storage quotas (ADR-0024). The defaults: no workspaces, nothing counted.

    /// The workspace a member's channel belongs to.
    fn workspace_of(
        &self,
        _user_id: i64,
        _channel_id: i64,
    ) -> impl Future<Output = Result<Option<i64>, String>> + Send {
        async { Ok(None) }
    }
    /// Bytes the workspace stores now.
    fn used(&self, _workspace_id: i64) -> impl Future<Output = Result<i64, String>> + Send {
        async { Ok(0) }
    }
    /// Adds (or with a negative delta, releases) stored bytes.
    fn add_usage(
        &self,
        _workspace_id: i64,
        _delta: i64,
    ) -> impl Future<Output = Result<(), String>> + Send {
        async { Ok(()) }
    }
    /// The operator's override: `None` uses the installation's default; `Some(0)` is unlimited.
    fn quota_override(
        &self,
        _workspace_id: i64,
    ) -> impl Future<Output = Result<Option<i64>, String>> + Send {
        async { Ok(None) }
    }
    /// The user's role in the workspace (`owner`, `admin`, `member`), if they're in it.
    fn workspace_role(
        &self,
        _workspace_id: i64,
        _user_id: i64,
    ) -> impl Future<Output = Result<Option<String>, String>> + Send {
        async { Ok(None) }
    }
}

/// Where the bytes go (an S3-compatible store in production).
pub trait Blobs: Send + Sync + 'static {
    fn put(
        &self,
        key: &str,
        bytes: Bytes,
        content_type: &str,
    ) -> impl Future<Output = Result<(), String>> + Send;
    /// The object as a streamed body, or None if it doesn't exist.
    fn get(&self, key: &str) -> impl Future<Output = Result<Option<Body>, String>> + Send;
    /// Removes the object; a missing one is fine.
    fn delete(&self, key: &str) -> impl Future<Output = Result<(), String>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_lose_paths_and_control_characters() {
        assert_eq!(clean_name("../../etc/passwd"), "passwd");
        assert_eq!(clean_name("C:\\Users\\me\\report.pdf"), "report.pdf");
        assert_eq!(clean_name("a\r\nb.txt"), "ab.txt");
        assert_eq!(clean_name(".htaccess"), "htaccess");
        assert_eq!(clean_name("  "), "file");
        assert_eq!(clean_name("dir/"), "file");
        let long = "é".repeat(200);
        let cleaned = clean_name(&long);
        assert!(cleaned.len() <= 255 && cleaned.chars().all(|c| c == 'é'));
    }

    #[test]
    fn content_types_are_kept_only_when_well_formed() {
        assert_eq!(
            clean_content_type(Some("Application/PDF; charset=x")),
            "application/pdf"
        );
        assert_eq!(clean_content_type(Some("text/html")), "text/html");
        assert_eq!(
            clean_content_type(Some("nonsense")),
            "application/octet-stream"
        );
        assert_eq!(
            clean_content_type(Some("a/b\r\nx: y")),
            "application/octet-stream"
        );
        assert_eq!(clean_content_type(None), "application/octet-stream");
    }

    #[test]
    fn ids_sort_by_time_and_differ() {
        let a = new_id(1_800_000_000_000);
        let b = new_id(1_800_000_000_001);
        assert!(a > 0 && b > a);
        // The same millisecond still gives different IDs (20 equal draws: 2^-440).
        let same: std::collections::HashSet<i64> =
            (0..20).map(|_| new_id(1_800_000_000_000)).collect();
        assert!(same.len() > 1);
    }
}
