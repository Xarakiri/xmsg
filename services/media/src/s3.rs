// SPDX-License-Identifier: AGPL-3.0-only
//! The S3 calls this service needs (PUT and GET of one object), path-style, signed with
//! AWS Signature Version 4. Works with any S3-compatible store (ADR-0020): Garage in the
//! bundle, Ceph RGW, Amazon S3, GCS's S3 mode.

use axum::body::Body;
use bytes::Bytes;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::Blobs;

pub struct S3 {
    http: reqwest::Client,
    /// e.g. `http://garage:3900`, without a trailing slash.
    endpoint: String,
    bucket: String,
    region: String,
    key_id: String,
    secret: String,
}

impl S3 {
    pub fn new(endpoint: &str, bucket: &str, region: &str, key_id: &str, secret: &str) -> Self {
        S3 {
            http: reqwest::Client::new(),
            endpoint: endpoint.trim_end_matches('/').to_string(),
            bucket: bucket.into(),
            region: region.into(),
            key_id: key_id.into(),
            secret: secret.into(),
        }
    }

    fn request(
        &self,
        method: reqwest::Method,
        key: &str,
        body_sha256: &str,
    ) -> reqwest::RequestBuilder {
        let path = format!(
            "/{}/{}",
            encode(&self.bucket),
            key.split('/').map(encode).collect::<Vec<_>>().join("/")
        );
        let url = format!("{}{path}", self.endpoint);
        let host = host_of(&self.endpoint);
        let now = chrono::Utc::now();
        let headers = sign(&Signing {
            method: method.as_str(),
            host: &host,
            path: &path,
            body_sha256,
            amz_date: &now.format("%Y%m%dT%H%M%SZ").to_string(),
            region: &self.region,
            key_id: &self.key_id,
            secret: &self.secret,
        });
        let mut request = self.http.request(method, url);
        for (name, value) in headers {
            request = request.header(name, value);
        }
        request
    }
}

impl Blobs for S3 {
    async fn put(&self, key: &str, bytes: Bytes, content_type: &str) -> Result<(), String> {
        let hash = hex::encode(Sha256::digest(&bytes));
        let response = self
            .request(reqwest::Method::PUT, key, &hash)
            .header("content-type", content_type)
            .body(bytes)
            .send()
            .await
            .map_err(|e| format!("S3 PUT {key}: {e}"))?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            Err(format!(
                "S3 PUT {key}: {status} {}",
                response.text().await.unwrap_or_default()
            ))
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Body>, String> {
        let response = self
            .request(reqwest::Method::GET, key, EMPTY_SHA256)
            .send()
            .await
            .map_err(|e| format!("S3 GET {key}: {e}"))?;
        match response.status().as_u16() {
            200 => Ok(Some(Body::from_stream(response.bytes_stream()))),
            404 => Ok(None),
            status => Err(format!(
                "S3 GET {key}: {status} {}",
                response.text().await.unwrap_or_default()
            )),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), String> {
        let response = self
            .request(reqwest::Method::DELETE, key, EMPTY_SHA256)
            .send()
            .await
            .map_err(|e| format!("S3 DELETE {key}: {e}"))?;
        // S3 answers 204 whether or not the object existed; some stores answer 404.
        match response.status().as_u16() {
            200 | 204 | 404 => Ok(()),
            status => Err(format!(
                "S3 DELETE {key}: {status} {}",
                response.text().await.unwrap_or_default()
            )),
        }
    }
}

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// `host[:port]` as the client sends it in the Host header (and so as it's signed).
fn host_of(endpoint: &str) -> String {
    let rest = endpoint.split_once("://").map_or(endpoint, |(_, r)| r);
    rest.split('/').next().unwrap_or(rest).to_string()
}

/// RFC 3986 percent-encoding of one path segment, as SigV4 wants it.
fn encode(segment: &str) -> String {
    let mut out = String::new();
    for b in segment.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub struct Signing<'a> {
    pub method: &'a str,
    pub host: &'a str,
    /// Already encoded.
    pub path: &'a str,
    pub body_sha256: &'a str,
    /// `YYYYMMDDTHHMMSSZ`
    pub amz_date: &'a str,
    pub region: &'a str,
    pub key_id: &'a str,
    pub secret: &'a str,
}

/// The headers that sign a request without a query string (SigV4, service `s3`).
pub fn sign(s: &Signing) -> Vec<(&'static str, String)> {
    let date = &s.amz_date[..8];
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let canonical = format!(
        "{}\n{}\n\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n\n{signed_headers}\n{}",
        s.method, s.path, s.host, s.body_sha256, s.amz_date, s.body_sha256
    );
    let scope = format!("{date}/{}/s3/aws4_request", s.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        s.amz_date,
        hex::encode(Sha256::digest(canonical.as_bytes()))
    );
    let mut key = hmac(format!("AWS4{}", s.secret).as_bytes(), date.as_bytes());
    for part in [s.region, "s3", "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    let signature = hex::encode(hmac(&key, to_sign.as_bytes()));
    vec![
        ("x-amz-date", s.amz_date.to_string()),
        ("x-amz-content-sha256", s.body_sha256.to_string()),
        (
            "authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={signature}",
                s.key_id
            ),
        ),
    ]
}

fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_and_hosts() {
        assert_eq!(encode("a b/c"), "a%20b%2Fc");
        assert_eq!(encode("12.thumb"), "12.thumb");
        assert_eq!(host_of("http://garage:3900"), "garage:3900");
        assert_eq!(host_of("https://s3.example.com/"), "s3.example.com");
    }

    #[test]
    fn signing_is_deterministic_and_scoped() {
        let s = Signing {
            method: "PUT",
            host: "garage:3900",
            path: "/xmsg-files/1/2",
            body_sha256: EMPTY_SHA256,
            amz_date: "20261027T120000Z",
            region: "garage",
            key_id: "GKexample",
            secret: "secret",
        };
        let a = sign(&s);
        assert_eq!(a, sign(&s));
        assert!(a[2].1.starts_with("AWS4-HMAC-SHA256 Credential=GKexample/20261027/garage/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature="));
    }
}
