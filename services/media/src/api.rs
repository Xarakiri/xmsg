// SPDX-License-Identifier: AGPL-3.0-only
//! HTTP API. Bearer tokens as everywhere (`xmsg-authn`); IDs are JSON strings.
//!
//! - `POST /v1/channels/{channel_id}/files?name=NAME`, the file as the body: members only.
//! - `GET /v1/files/{file_id}`, `.../thumbnail`, `.../meta`: members of the file's channel
//!   only. "No such file" and "not a member" look the same (404), so IDs don't leak.
//!
//! Served files can't run as our origin: only images we decoded go out inline, as their
//! sniffed type; everything else is a download (`application/octet-stream`,
//! `attachment`); and every response has `nosniff` and a sandboxing CSP.

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use xmsg_authn::{Error as AuthError, Verifier};

use crate::{Blobs, FileMeta, Store, clean_content_type, clean_name, new_id, thumbnail};

pub struct App<S, B> {
    pub store: S,
    pub blobs: B,
    pub verifier: Verifier,
    /// Largest upload, in bytes.
    pub max_bytes: usize,
    /// Uploads are held in memory while they're checked and stored: this bounds how many.
    pub uploads: Semaphore,
}

pub fn router<S: Store, B: Blobs>(app: Arc<App<S, B>>) -> Router {
    Router::new()
        .route("/v1/channels/{channel_id}/files", post(upload::<S, B>))
        .route("/v1/files/{file_id}", get(download::<S, B>))
        .route("/v1/files/{file_id}/thumbnail", get(thumbnail::<S, B>))
        .route("/v1/files/{file_id}/meta", get(meta::<S, B>))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(app)
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

// Handlers answer with the error response as is; it never travels far.
#[allow(clippy::result_large_err)]
async fn caller(verifier: &Verifier, headers: &HeaderMap) -> Result<i64, Response> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| error(StatusCode::UNAUTHORIZED, "invalid or missing access token"))?;
    match verifier.verify(auth).await {
        Ok(p) => Ok(p.user_id),
        Err(AuthError::KeysUnavailable) => {
            Err(error(StatusCode::SERVICE_UNAVAILABLE, "try again later"))
        }
        Err(_) => Err(error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing access token",
        )),
    }
}

fn not_found() -> Response {
    error(StatusCode::NOT_FOUND, "unknown file")
}

fn unavailable(what: &str, e: &str) -> Response {
    tracing::error!(error = %e, "{what} failed");
    error(StatusCode::SERVICE_UNAVAILABLE, "try again later")
}

#[derive(Deserialize)]
pub struct UploadQuery {
    name: Option<String>,
}

async fn upload<S: Store, B: Blobs>(
    State(app): State<Arc<App<S, B>>>,
    headers: HeaderMap,
    Path(channel_id): Path<i64>,
    Query(q): Query<UploadQuery>,
    body: Body,
) -> Response {
    let user = match caller(&app.verifier, &headers).await {
        Ok(u) => u,
        Err(r) => return r,
    };
    match app.store.is_member(channel_id, user).await {
        Ok(true) => {}
        Ok(false) => return error(StatusCode::NOT_FOUND, "unknown channel"),
        Err(e) => return unavailable("membership read", &e),
    }
    let _permit = app.uploads.acquire().await.expect("never closed");
    let bytes: Bytes = match axum::body::to_bytes(body, app.max_bytes).await {
        Ok(b) => b,
        Err(_) => {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("files are limited to {} bytes", app.max_bytes),
            );
        }
    };
    if bytes.is_empty() {
        return error(StatusCode::BAD_REQUEST, "the file is empty");
    }

    let inspected = {
        let bytes = bytes.clone();
        tokio::task::spawn_blocking(move || {
            let image = thumbnail::inspect(&bytes);
            (image, hex::encode(Sha256::digest(&bytes)))
        })
        .await
        .expect("inspect")
    };
    let (image, sha256) = inspected;
    let now = chrono::Utc::now().timestamp_millis();
    let declared = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let mut file = FileMeta {
        file_id: new_id(now),
        channel_id,
        uploader_id: user,
        name: clean_name(q.name.as_deref().unwrap_or("file")),
        content_type: clean_content_type(declared),
        size: bytes.len() as i64,
        sha256,
        created_at_ms: now,
        width: None,
        height: None,
        thumbnail_type: None,
    };
    let mut thumb = None;
    if let Some(image) = image {
        file.content_type = image.content_type.into();
        if image.width > 0 {
            file.width = Some(image.width as i32);
            file.height = Some(image.height as i32);
        }
        if let Some((bytes, kind)) = image.thumbnail {
            file.thumbnail_type = Some(kind.into());
            thumb = Some(Bytes::from(bytes));
        }
    } else if crate::INLINE_TYPES.contains(&file.content_type.as_str()) {
        // Claims to be an image we'd show inline, but isn't one.
        file.content_type = "application/octet-stream".into();
    }

    // Objects first, then the row: a row never points at a missing object.
    if let Err(e) = app
        .blobs
        .put(&file.object_key(), bytes, "application/octet-stream")
        .await
    {
        return unavailable("storing the file", &e);
    }
    if let (Some(bytes), Some(kind)) = (thumb, &file.thumbnail_type)
        && let Err(e) = app.blobs.put(&file.thumbnail_key(), bytes, kind).await
    {
        return unavailable("storing the thumbnail", &e);
    }
    if let Err(e) = app.store.insert(&file).await {
        return unavailable("recording the file", &e);
    }
    (StatusCode::CREATED, Json(&file)).into_response()
}

/// The file, if it exists and the caller may see it.
#[allow(clippy::result_large_err)]
async fn visible<S: Store, B: Blobs>(
    app: &App<S, B>,
    headers: &HeaderMap,
    file_id: i64,
) -> Result<FileMeta, Response> {
    let user = caller(&app.verifier, headers).await?;
    let file = match app.store.get(file_id).await {
        Ok(Some(f)) => f,
        Ok(None) => return Err(not_found()),
        Err(e) => return Err(unavailable("file read", &e)),
    };
    match app.store.is_member(file.channel_id, user).await {
        Ok(true) => Ok(file),
        Ok(false) => Err(not_found()),
        Err(e) => Err(unavailable("membership read", &e)),
    }
}

async fn meta<S: Store, B: Blobs>(
    State(app): State<Arc<App<S, B>>>,
    headers: HeaderMap,
    Path(file_id): Path<i64>,
) -> Response {
    match visible(&app, &headers, file_id).await {
        Ok(file) => Json(&file).into_response(),
        Err(r) => r,
    }
}

async fn download<S: Store, B: Blobs>(
    State(app): State<Arc<App<S, B>>>,
    headers: HeaderMap,
    Path(file_id): Path<i64>,
) -> Response {
    let file = match visible(&app, &headers, file_id).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let content_type = if file.inline() {
        file.content_type.as_str()
    } else {
        "application/octet-stream"
    };
    serve(&app, &file, &file.object_key(), content_type, file.inline()).await
}

async fn thumbnail<S: Store, B: Blobs>(
    State(app): State<Arc<App<S, B>>>,
    headers: HeaderMap,
    Path(file_id): Path<i64>,
) -> Response {
    let file = match visible(&app, &headers, file_id).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    match file.thumbnail_type.clone() {
        Some(kind) => serve(&app, &file, &file.thumbnail_key(), &kind, true).await,
        None => not_found(),
    }
}

async fn serve<S: Store, B: Blobs>(
    app: &App<S, B>,
    file: &FileMeta,
    key: &str,
    content_type: &str,
    inline: bool,
) -> Response {
    let body = match app.blobs.get(key).await {
        Ok(Some(body)) => body,
        Ok(None) => {
            tracing::error!(key, "a recorded file's object is missing");
            return not_found();
        }
        Err(e) => return unavailable("reading the file", &e),
    };
    let mut response = Response::new(body);
    let h = response.headers_mut();
    let value = |s: &str| {
        HeaderValue::from_str(s).unwrap_or(HeaderValue::from_static("application/octet-stream"))
    };
    h.insert(header::CONTENT_TYPE, value(content_type));
    h.insert(header::CONTENT_DISPOSITION, disposition(inline, &file.name));
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'none'; sandbox"),
    );
    // Immutable, but only for this user's browser.
    h.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    if let Ok(etag) = HeaderValue::from_str(&format!("\"{}\"", file.sha256)) {
        h.insert(header::ETAG, etag);
    }
    response
}

/// `inline`/`attachment` with an ASCII fallback name and the exact one (RFC 6266, 5987).
fn disposition(inline: bool, name: &str) -> HeaderValue {
    let ascii: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_graphic() && c != '"' && c != '\\' || c == ' ' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let mut exact = String::new();
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b"!#$&+-.^_`|~".contains(&b) {
            exact.push(b as char);
        } else {
            exact.push_str(&format!("%{b:02X}"));
        }
    }
    let kind = if inline { "inline" } else { "attachment" };
    HeaderValue::from_str(&format!(
        "{kind}; filename=\"{ascii}\"; filename*=UTF-8''{exact}"
    ))
    .unwrap_or(HeaderValue::from_static("attachment"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispositions_are_safe_header_values() {
        let d = disposition(false, "отчёт \"final\".pdf");
        assert_eq!(
            d.to_str().unwrap(),
            "attachment; filename=\"_____ _final_.pdf\"; filename*=UTF-8''%D0%BE%D1%82%D1%87%D1%91%D1%82%20%22final%22.pdf"
        );
        assert!(
            disposition(true, "cat.png")
                .to_str()
                .unwrap()
                .starts_with("inline; ")
        );
    }
}
