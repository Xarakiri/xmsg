// SPDX-License-Identifier: LicenseRef-xmsg-EE
//! xmsg-license: the vendor's licence tool. Never shipped to customers.
//!
//!   xmsg-license keygen
//!       prints a new signing key (keep it secret) and its public half (goes into
//!       `xmsg_ee_license::VENDOR_KEYS`)
//!   xmsg-license sign KEY CUSTOMER EXPIRES(YYYY-MM-DD) USERS FEATURES(sso,scim,admin,compliance)
//!       prints a licence
//!   xmsg-license verify PUBLIC_KEY < licence
//!       prints the licence's contents if it verifies

use std::process::ExitCode;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{NaiveDate, Utc};
use xmsg_ee_license::{Feature, Licence, public_key, sign, verify_with};

fn usage() -> ExitCode {
    eprintln!(
        "usage: xmsg-license keygen | sign KEY CUSTOMER EXPIRES USERS FEATURES | verify PUBLIC_KEY < licence"
    );
    ExitCode::from(2)
}

fn signing_key(b64: &str) -> Option<ed25519_dalek::SigningKey> {
    let bytes: [u8; 32] = URL_SAFE_NO_PAD.decode(b64).ok()?.try_into().ok()?;
    Some(ed25519_dalek::SigningKey::from_bytes(&bytes))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("keygen") => {
            let mut seed = [0u8; 32];
            getrandom::fill(&mut seed).expect("OS random");
            let key = ed25519_dalek::SigningKey::from_bytes(&seed);
            println!("signing key (secret): {}", URL_SAFE_NO_PAD.encode(seed));
            println!("public key:           {}", public_key(&key));
            ExitCode::SUCCESS
        }
        Some("sign") if args.len() == 6 => {
            let Some(key) = signing_key(&args[1]) else {
                return usage();
            };
            let Ok(expires) = NaiveDate::parse_from_str(&args[3], "%Y-%m-%d") else {
                return usage();
            };
            let Ok(users) = args[4].parse() else {
                return usage();
            };
            let mut features = Vec::new();
            for f in args[5].split(',') {
                features.push(match f.trim() {
                    "sso" => Feature::Sso,
                    "scim" => Feature::Scim,
                    "admin" => Feature::Admin,
                    "compliance" => Feature::Compliance,
                    _ => return usage(),
                });
            }
            let now = Utc::now();
            let licence = Licence {
                licence_id: format!("lic-{}", now.timestamp()),
                customer: args[2].clone(),
                issued_at: now,
                expires_at: expires.and_hms_opt(23, 59, 59).expect("time").and_utc(),
                users,
                features,
            };
            println!("{}", sign(&licence, &key));
            ExitCode::SUCCESS
        }
        Some("verify") if args.len() == 2 => {
            let mut text = String::new();
            let _ = std::io::Read::read_to_string(&mut std::io::stdin(), &mut text);
            match verify_with(&text, &[args[1].clone()]) {
                Ok(l) => {
                    println!("{l:#?}");
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("{e}");
                    ExitCode::FAILURE
                }
            }
        }
        _ => usage(),
    }
}
