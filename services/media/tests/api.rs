// SPDX-License-Identifier: AGPL-3.0-only
//! The files API with an in-memory store and blobs, and real tokens from the auth
//! service's key code. `s3_round_trip` runs against a real S3 store (Garage in CI).

use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use tokio::sync::Semaphore;
use tower::ServiceExt;
use xmsg_auth::keys::Keys;
use xmsg_media::api::{App, router};
use xmsg_media::{Blobs, FileMeta, Store};

#[derive(Default)]
struct Fake {
    members: HashSet<(i64, i64)>,
    files: Mutex<HashMap<i64, FileMeta>>,
    blobs: Mutex<HashMap<String, Bytes>>,
}

/// Shared by the app and the assertions.
#[derive(Clone)]
struct Shared(Arc<Fake>);

impl Store for Shared {
    async fn is_member(&self, channel_id: i64, user_id: i64) -> Result<bool, String> {
        Ok(self.0.members.contains(&(channel_id, user_id)))
    }
    async fn insert(&self, file: &FileMeta) -> Result<(), String> {
        self.0
            .files
            .lock()
            .unwrap()
            .insert(file.file_id, file.clone());
        Ok(())
    }
    async fn get(&self, file_id: i64) -> Result<Option<FileMeta>, String> {
        Ok(self.0.files.lock().unwrap().get(&file_id).cloned())
    }
}

impl Blobs for Shared {
    async fn put(&self, key: &str, bytes: Bytes, _: &str) -> Result<(), String> {
        self.0.blobs.lock().unwrap().insert(key.into(), bytes);
        Ok(())
    }
    async fn delete(&self, key: &str) -> Result<(), String> {
        self.0.blobs.lock().unwrap().remove(key);
        Ok(())
    }
    async fn get(&self, key: &str) -> Result<Option<Body>, String> {
        Ok(self
            .0
            .blobs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .map(Body::from))
    }
}

const CHANNEL: i64 = 77;
const ALICE: i64 = 1;
const BOB: i64 = 2;
const MALLORY: i64 = 3;

struct Harness {
    app: Router,
    keys: Arc<Keys>,
    fake: Arc<Fake>,
}

async fn harness() -> Harness {
    let keys = Arc::new(Keys::generate());
    let jwks = keys.jwks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let server = Router::new().route(
        "/jwks",
        axum::routing::get(move || async move { axum::Json(jwks) }),
    );
    tokio::spawn(async move { axum::serve(listener, server).await.unwrap() });
    let fake = Arc::new(Fake {
        members: [(CHANNEL, ALICE), (CHANNEL, BOB)].into(),
        ..Default::default()
    });
    let app = router(Arc::new(App {
        store: Shared(fake.clone()),
        blobs: Shared(fake.clone()),
        verifier: xmsg_authn::Verifier::new(url),
        max_bytes: 1024 * 1024,
        uploads: Semaphore::new(2),
    }));
    Harness { app, keys, fake }
}

impl Harness {
    fn token(&self, user: i64) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        format!("Bearer {}", self.keys.issue(user, 1, now, 900))
    }

    async fn send(
        &self,
        user: i64,
        method: &str,
        uri: &str,
        content_type: Option<&str>,
        body: Vec<u8>,
    ) -> (StatusCode, axum::http::HeaderMap, Bytes) {
        let mut request = Request::builder()
            .method(method)
            .uri(uri)
            .header("authorization", self.token(user));
        if let Some(t) = content_type {
            request = request.header("content-type", t);
        }
        let response = self
            .app
            .clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let (parts, body) = response.into_parts();
        (
            parts.status,
            parts.headers,
            body.collect().await.unwrap().to_bytes(),
        )
    }

    async fn upload(
        &self,
        user: i64,
        name: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> serde_json::Value {
        let (status, _, reply) = self
            .send(
                user,
                "POST",
                &format!("/v1/channels/{CHANNEL}/files?name={name}"),
                Some(content_type),
                body,
            )
            .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "{}",
            String::from_utf8_lossy(&reply)
        );
        serde_json::from_slice(&reply).unwrap()
    }
}

fn png(width: u32, height: u32) -> Vec<u8> {
    let mut out = Cursor::new(Vec::new());
    image::DynamicImage::ImageRgb8(image::RgbImage::new(width, height))
        .write_to(&mut out, image::ImageFormat::Png)
        .unwrap();
    out.into_inner()
}

#[tokio::test]
async fn members_upload_and_download_others_see_nothing() {
    let h = harness().await;
    let file = h
        .upload(ALICE, "notes.txt", "text/plain", b"hello".to_vec())
        .await;
    assert_eq!(file["name"], "notes.txt");
    assert_eq!(file["content_type"], "text/plain");
    assert_eq!(file["size"], 5);
    assert_eq!(file["channel_id"], CHANNEL.to_string());
    let id = file["file_id"].as_str().unwrap();

    // Bob (a member) downloads it, as an attachment.
    let (status, headers, body) = h
        .send(BOB, "GET", &format!("/v1/files/{id}"), None, vec![])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(&body[..], b"hello");
    assert_eq!(headers["content-type"], "application/octet-stream");
    assert!(
        headers["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("attachment; ")
    );
    assert_eq!(headers["x-content-type-options"], "nosniff");
    assert!(
        headers["content-security-policy"]
            .to_str()
            .unwrap()
            .contains("sandbox")
    );

    // Mallory isn't a member: the same 404 as for a file that doesn't exist.
    for path in [
        format!("/v1/files/{id}"),
        format!("/v1/files/{id}/meta"),
        "/v1/files/123".into(),
    ] {
        let (status, _, _) = h.send(MALLORY, "GET", &path, None, vec![]).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}");
    }
    let (status, _, _) = h
        .send(
            MALLORY,
            "POST",
            &format!("/v1/channels/{CHANNEL}/files"),
            None,
            b"x".to_vec(),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // No token, no file.
    let response = h
        .app
        .clone()
        .oneshot(
            Request::get(format!("/v1/files/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn images_are_sniffed_measured_and_thumbnailed() {
    let h = harness().await;
    // Declared as something else: the bytes decide.
    let file = h
        .upload(ALICE, "photo", "application/x-whatever", png(1200, 600))
        .await;
    assert_eq!(file["content_type"], "image/png");
    assert_eq!(
        (file["width"].as_i64(), file["height"].as_i64()),
        (Some(1200), Some(600))
    );
    assert_eq!(file["thumbnail_type"], "image/jpeg");
    let id = file["file_id"].as_str().unwrap();

    let (status, headers, _) = h
        .send(BOB, "GET", &format!("/v1/files/{id}"), None, vec![])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "image/png");
    assert!(
        headers["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("inline; ")
    );

    let (status, headers, thumb) = h
        .send(
            BOB,
            "GET",
            &format!("/v1/files/{id}/thumbnail"),
            None,
            vec![],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "image/jpeg");
    let t = image::load_from_memory(&thumb).unwrap();
    assert_eq!((t.width(), t.height()), (400, 200));
}

#[tokio::test]
async fn claimed_images_that_are_not_are_downloads() {
    let h = harness().await;
    let html = b"<html><script>alert(document.cookie)</script></html>".to_vec();
    let file = h.upload(ALICE, "cat.png", "image/png", html).await;
    assert_eq!(file["content_type"], "application/octet-stream");
    assert!(file["thumbnail_type"].is_null());
    let id = file["file_id"].as_str().unwrap();
    let (_, headers, _) = h
        .send(BOB, "GET", &format!("/v1/files/{id}"), None, vec![])
        .await;
    assert_eq!(headers["content-type"], "application/octet-stream");
    assert!(
        headers["content-disposition"]
            .to_str()
            .unwrap()
            .starts_with("attachment; ")
    );
    let (status, _, _) = h
        .send(
            BOB,
            "GET",
            &format!("/v1/files/{id}/thumbnail"),
            None,
            vec![],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn too_large_and_empty_uploads_are_refused_and_store_nothing() {
    let h = harness().await;
    let uri = format!("/v1/channels/{CHANNEL}/files?name=big.bin");
    let (status, _, _) = h
        .send(ALICE, "POST", &uri, None, vec![0; 1024 * 1024 + 1])
        .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let (status, _, _) = h.send(ALICE, "POST", &uri, None, vec![]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(h.fake.files.lock().unwrap().is_empty());
    assert!(h.fake.blobs.lock().unwrap().is_empty());
}

#[tokio::test]
async fn names_are_cleaned() {
    let h = harness().await;
    let file = h
        .upload(ALICE, "..%2F..%2Fetc%2Fpasswd", "text/plain", b"x".to_vec())
        .await;
    assert_eq!(file["name"], "passwd");
}

/// Needs a real S3 store: XMSG_IT_S3_ENDPOINT, XMSG_IT_S3_BUCKET, XMSG_IT_S3_KEY_ID,
/// XMSG_IT_S3_SECRET (CI sets them up with `xmsg-media garage-init`).
#[tokio::test]
#[ignore]
async fn s3_round_trip() {
    let var = |n: &str| std::env::var(n).unwrap_or_else(|_| panic!("{n} must be set"));
    let s3 = xmsg_media::s3::S3::new(
        &var("XMSG_IT_S3_ENDPOINT"),
        &var("XMSG_IT_S3_BUCKET"),
        &std::env::var("XMSG_IT_S3_REGION").unwrap_or_else(|_| "garage".into()),
        &var("XMSG_IT_S3_KEY_ID"),
        &var("XMSG_IT_S3_SECRET"),
    );
    let key = format!("it/{}/a file.bin", std::process::id());
    let data = Bytes::from((0..300_000u32).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    s3.put(&key, data.clone(), "application/octet-stream")
        .await
        .unwrap();
    let body = s3.get(&key).await.unwrap().expect("the object");
    assert_eq!(body.collect().await.unwrap().to_bytes(), data);
    assert!(s3.get("it/no-such-object").await.unwrap().is_none());

    // A wrong secret is refused, not ignored.
    let wrong = xmsg_media::s3::S3::new(
        &var("XMSG_IT_S3_ENDPOINT"),
        &var("XMSG_IT_S3_BUCKET"),
        "garage",
        &var("XMSG_IT_S3_KEY_ID"),
        "0000000000000000000000000000000000000000000000000000000000000000",
    );
    assert!(
        wrong
            .put(&key, data, "application/octet-stream")
            .await
            .is_err()
    );
}
