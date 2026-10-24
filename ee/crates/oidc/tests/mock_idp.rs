// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! OIDC against an in-process mock identity provider (ES256-signed ID tokens).

use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Form, Json, Router};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use xmsg_auth::sso::IdentityProvider;
use xmsg_ee_oidc::{Config, Oidc};

/// A throwaway P-256 key for this test only.
const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgnMMUR3+vNYdyWRed
SCdNIKzqAvewuxRyHRk5ZmEo1AShRANCAARUeu/1IaABjXkqZudv5cFQ+ZiVDjrv
Qt4cnL44J9qSrvsT+I5eh8ZuvAeDmZ+FoIZnPbZRwiSNbO/1en+KQuT3
-----END PRIVATE KEY-----";
const X: &str = "VHrv9SGgAY15Kmbnb-XBUPmYlQ4670LeHJy-OCfakq4";
const Y: &str = "-xP4jl6Hxm68B4OZn4Wghmc9tlHCJI1s7_V6f4pC5Pc";

#[derive(Default)]
struct Idp {
    issuer: String,
    /// The last token request, to check PKCE and the redirect URI.
    last: Mutex<Vec<(String, String)>>,
    /// What the next ID token says (aud, nonce, email_verified).
    aud: Mutex<String>,
    nonce: Mutex<String>,
}

async fn start_idp() -> Arc<Idp> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let idp = Arc::new(Idp {
        issuer: issuer.clone(),
        aud: Mutex::new("xmsg".into()),
        ..Default::default()
    });
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(|State(idp): State<Arc<Idp>>| async move {
                Json(serde_json::json!({
                    "issuer": idp.issuer,
                    "authorization_endpoint": format!("{}/authorize", idp.issuer),
                    "token_endpoint": format!("{}/token", idp.issuer),
                    "jwks_uri": format!("{}/jwks", idp.issuer),
                }))
            }),
        )
        .route(
            "/jwks",
            get(|| async { Json(serde_json::json!({ "keys": [{ "kty": "EC", "crv": "P-256", "x": X, "y": Y, "kid": "k1", "alg": "ES256", "use": "sig" }] })) }),
        )
        .route(
            "/token",
            post(|State(idp): State<Arc<Idp>>, Form(form): Form<Vec<(String, String)>>| async move {
                *idp.last.lock().unwrap() = form;
                let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
                let claims = serde_json::json!({
                    "iss": idp.issuer, "aud": *idp.aud.lock().unwrap(), "sub": "okta|42",
                    "email": "ann@example.org", "email_verified": true, "name": "Ann Lee",
                    "nonce": *idp.nonce.lock().unwrap(), "iat": now, "exp": now + 300,
                });
                let mut header = Header::new(Algorithm::ES256);
                header.kid = Some("k1".into());
                let id_token = encode(&header, &claims, &EncodingKey::from_ec_pem(KEY_PEM.as_bytes()).unwrap()).unwrap();
                Json(serde_json::json!({ "id_token": id_token, "access_token": "x", "token_type": "Bearer" }))
            }),
        )
        .with_state(idp.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    idp
}

fn config(issuer: &str) -> Config {
    Config {
        id: "okta".into(),
        display_name: "Okta".into(),
        issuer: issuer.into(),
        client_id: "xmsg".into(),
        client_secret: "s3cret".into(),
        trust_unverified_email: false,
    }
}

#[tokio::test]
async fn a_valid_id_token_becomes_a_verified_identity() {
    let idp = start_idp().await;
    let oidc = Oidc::discover(config(&idp.issuer)).await.unwrap();
    let url = oidc.authorize_url(
        "st",
        "n-1",
        "challenge",
        "https://chat.example/auth/v1/sso/okta/callback",
    );
    assert!(url.starts_with(&format!(
        "{}/authorize?response_type=code&scope=openid%20email%20profile&client_id=xmsg",
        idp.issuer
    )));
    assert!(url.contains("code_challenge=challenge&code_challenge_method=S256"));

    *idp.nonce.lock().unwrap() = "n-1".into();
    let identity = oidc
        .finish("code-1", "verifier-1", "n-1", "https://chat.example/cb")
        .await
        .unwrap();
    assert_eq!(identity.subject, "okta|42");
    assert_eq!(identity.email, "ann@example.org");
    assert!(identity.email_verified);
    let sent = idp.last.lock().unwrap().clone();
    for (k, v) in [
        ("code", "code-1"),
        ("code_verifier", "verifier-1"),
        ("redirect_uri", "https://chat.example/cb"),
        ("client_secret", "s3cret"),
    ] {
        assert!(
            sent.contains(&(k.to_string(), v.to_string())),
            "token request lacks {k}={v}: {sent:?}"
        );
    }
}

#[tokio::test]
async fn a_replayed_nonce_or_another_audience_is_refused() {
    let idp = start_idp().await;
    let oidc = Oidc::discover(config(&idp.issuer)).await.unwrap();
    *idp.nonce.lock().unwrap() = "n-old".into();
    assert!(
        oidc.finish("c", "v", "n-new", "r")
            .await
            .unwrap_err()
            .contains("nonce")
    );
    *idp.nonce.lock().unwrap() = "n-2".into();
    *idp.aud.lock().unwrap() = "another-app".into();
    assert!(
        oidc.finish("c", "v", "n-2", "r").await.is_err(),
        "a token minted for another client"
    );
}

#[tokio::test]
async fn a_discovery_document_for_another_issuer_is_refused() {
    let idp = start_idp().await;
    let mut c = config(&idp.issuer);
    c.issuer = format!("{}/", idp.issuer); // the same, with a slash: fine
    assert!(Oidc::discover(c).await.is_ok());
    let mut c = config("http://127.0.0.1:1");
    c.issuer = "http://127.0.0.1:1".into();
    assert!(Oidc::discover(c).await.is_err(), "unreachable issuer");
}
