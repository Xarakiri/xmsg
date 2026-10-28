#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Installs (or updates the configuration of) a Small-tier xmsg on this machine (plan
# §12.2): generates the secrets and .env, sets up TLS, starts the Docker Compose bundle
# and waits until it answers. Safe to run again: existing secrets are kept.
#
#   ./install.sh                     # asks what it needs
#   ./install.sh --domain chat.example.com --tls acme --email admin@example.com --yes
#
# Options:
#   --domain NAME        the name people use (default: localhost)
#   --tls MODE           acme (Let's Encrypt), cert (your own), or internal (evaluation)
#   --email ADDRESS      for Let's Encrypt (--tls acme)
#   --cert FILE --key FILE   your certificate and key (--tls cert), copied to ./certs
#   --registry R --version V   use published images R/<name>:V instead of building
#   --partitions N       Kafka partitions (default 16; fixed forever, ADR-0021)
#   --search             also run search (OpenSearch and the indexer; +1.5 GB of memory)
#   --calls              also run voice and video calls (LiveKit and TURN; open UDP 7882,
#                        TCP 7881, and 3478 UDP/TCP plus UDP 49160-49200 for TURN)
#   --no-start           write the configuration only
#   --yes                no questions; defaults for anything not given
#
# From an air-gapped bundle (make-bundle.sh), the images come from images/ next to this
# script: nothing is pulled, and the bundle's version is installed.
set -euo pipefail
cd "$(dirname "$0")"

domain="" tls="" email="" cert="" key="" registry="" version="" partitions="" search="" calls="" start=1 ask=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --domain) domain="$2"; shift 2 ;;
    --tls) tls="$2"; shift 2 ;;
    --email) email="$2"; shift 2 ;;
    --cert) cert="$2"; shift 2 ;;
    --key) key="$2"; shift 2 ;;
    --registry) registry="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --partitions) partitions="$2"; shift 2 ;;
    --search) search=1; shift ;;
    --calls) calls=1; shift ;;
    --no-start) start=0; shift ;;
    --yes|-y) ask=0; shift ;;
    -h|--help) sed -n '3,23p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
  esac
done

say() { printf '\033[1m%s\033[0m\n' "$*"; }
fail() { echo "error: $*" >&2; exit 1; }
# ask VAR "question" default: interactive only when a terminal is attached and --yes isn't given.
ask() {
  local var=$1 question=$2 default=$3 answer
  if [[ -n "${!var}" ]]; then return; fi
  if [[ $ask -eq 1 && -t 0 ]]; then
    read -r -p "$question [$default]: " answer
    printf -v "$var" '%s' "${answer:-$default}"
  else
    printf -v "$var" '%s' "$default"
  fi
}

command -v openssl >/dev/null || fail "openssl is required"

# ---- air-gapped bundle -----------------------------------------------------------------
bundle=0
if [[ -f images/images.tar.gz && -f bundle.env ]]; then
  bundle=1
  # The bundle's images win over the existing configuration: unpacking a newer bundle
  # and running this is the upgrade.
  : "${registry:=$(sed -n 's/^XMSG_REGISTRY=//p' bundle.env)}"
  : "${version:=$(sed -n 's/^XMSG_VERSION=//p' bundle.env)}"
fi

# ---- existing configuration ----------------------------------------------------------
# Re-runs keep the secrets: new ones would lock everyone out (signing key) or no longer
# open the database (its password is set once, when the volume is created).
if [[ -f .env ]]; then
  say "Updating the existing installation (its secrets are reused)"
fi
# A value from the existing .env (empty if none); bash 3 compatible (macOS).
existing() { [[ -f .env ]] && sed -n "s/^$1=//p" .env | tail -n 1 || true; }

: "${domain:=$(existing XMSG_DOMAIN)}"
: "${partitions:=$(existing XMSG_PARTITIONS)}"
# Search stays on once installed with it.
# Search and calls stay on once installed with them.
[[ ",$(existing COMPOSE_PROFILES)," == *,search,* ]] && : "${search:=1}"
[[ ",$(existing COMPOSE_PROFILES)," == *,calls,* ]] && : "${calls:=1}"
if [[ $bundle -eq 1 && -n "$search" && "$(sed -n 's/^XMSG_BUNDLE_SEARCH=//p' bundle.env)" != 1 ]]; then
  fail "search is on, but this bundle was made without it (make-bundle.sh --no-search)"
fi
: "${registry:=$(existing XMSG_REGISTRY)}"
: "${version:=$(existing XMSG_VERSION)}"
ask domain "Domain people will use (e.g. chat.example.com)" "localhost"
if [[ -z "$tls" ]]; then
  previous=$(existing XMSG_TLS)
  case "$previous" in
    "") ;;
    internal) tls=internal ;;
    /certs/*) tls=cert ;;
    *) tls=acme; : "${email:=$previous}" ;;
  esac
fi
if [[ "$domain" == "localhost" ]]; then default_tls=internal; else default_tls=acme; fi
ask tls "TLS: acme (Let's Encrypt), cert (your own), or internal (evaluation only)" "$default_tls"

case "$tls" in
  acme)
    [[ "$domain" == "localhost" ]] && fail "Let's Encrypt needs a public domain name; use --tls internal for localhost"
    ask email "Email for Let's Encrypt notices" ""
    [[ "$email" == *@* ]] || fail "--tls acme needs --email"
    tls_args="$email" ;;
  cert)
    mkdir -p certs
    if [[ -n "$cert" || -n "$key" ]]; then
      [[ -f "$cert" && -f "$key" ]] || fail "--cert and --key must both be readable files"
      cp "$cert" certs/cert.pem && cp "$key" certs/key.pem && chmod 600 certs/key.pem
    fi
    [[ -f certs/cert.pem && -f certs/key.pem ]] || fail "--tls cert needs --cert FILE --key FILE (or certs/cert.pem and certs/key.pem)"
    tls_args="/certs/cert.pem /certs/key.pem" ;;
  internal)
    [[ "$domain" != "localhost" ]] && echo "note: an internal certificate isn't trusted by browsers; use it for evaluation only" >&2
    tls_args="internal" ;;
  *) fail "--tls must be acme, cert or internal" ;;
esac

: "${partitions:=16}"
[[ "$partitions" =~ ^[0-9]+$ ]] || fail "--partitions must be a number"
if [[ -n "$(existing XMSG_PARTITIONS)" && "$partitions" != "$(existing XMSG_PARTITIONS)" ]]; then
  fail "the partition count is fixed once set (ADR-0021); moving tiers is 'xmsgctl migrate-partitions'"
fi

# ---- secrets (generated once) --------------------------------------------------------
secret() { local v; v=$(existing "$1"); [[ -n "$v" ]] && echo "$v" || eval "$2"; }
auth_key=$(secret XMSG_AUTH_KEY "openssl rand -base64 32")
postgres_password=$(secret XMSG_POSTGRES_PASSWORD "openssl rand -hex 24")
kafka_cluster_id=$(secret XMSG_KAFKA_CLUSTER_ID "openssl rand -base64 16 | tr '+/' '-_' | tr -d '=\n'")
# Web Push (VAPID): the raw 32-byte P-256 private key (bytes 8-39 of the SEC1 DER),
# base64url (see services/push). Through a file and dd, not `tail | head`: head exiting
# early would kill tail with SIGPIPE, which pipefail turns into a failed install.
vapid_key() {
  local der
  der=$(mktemp)
  openssl ecparam -genkey -name prime256v1 -noout | openssl ec -outform DER -out "$der" 2>/dev/null
  dd if="$der" bs=1 skip=7 count=32 2>/dev/null | base64 | tr '/+' '_-' | tr -d '=\n'
  rm -f "$der"
}
vapid=$(secret XMSG_PUSH_VAPID_PRIVATE_KEY vapid_key)
# Files (ADR-0024): Garage's cluster secret and admin token, and the media service's key
# (Garage wants "GK" and 24 hex digits for an imported key ID, 64 for its secret).
garage_rpc=$(secret XMSG_GARAGE_RPC_SECRET "openssl rand -hex 32")
garage_admin=$(secret XMSG_GARAGE_ADMIN_TOKEN "openssl rand -hex 32")
media_key_id=$(secret XMSG_MEDIA_S3_KEY_ID "echo GK\$(openssl rand -hex 12)")
media_secret=$(secret XMSG_MEDIA_S3_SECRET "openssl rand -hex 32")

# Your own settings on top (resource limits, service tuning) survive re-runs and upgrades.
compose_local=""
[[ -f docker-compose.local.yml ]] && compose_local=":docker-compose.local.yml"

umask 077
cat > .env.new <<EOF
# Written by install.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ). Keep this file private: it holds the
# installation's secrets. Re-running install.sh keeps them.
COMPOSE_FILE=docker-compose.yml:docker-compose.app.yml$compose_local
XMSG_DOMAIN=$domain
XMSG_TLS=$tls_args
XMSG_PARTITIONS=$partitions
XMSG_REGISTRY=${registry:-xmsg}
XMSG_VERSION=${version:-dev}
XMSG_AUTH_KEY=$auth_key
XMSG_POSTGRES_PASSWORD=$postgres_password
XMSG_KAFKA_CLUSTER_ID=$kafka_cluster_id
XMSG_PUSH_VAPID_PRIVATE_KEY=$vapid
XMSG_GARAGE_RPC_SECRET=$garage_rpc
XMSG_GARAGE_ADMIN_TOKEN=$garage_admin
XMSG_MEDIA_S3_KEY_ID=$media_key_id
XMSG_MEDIA_S3_SECRET=$media_secret
XMSG_PUSH_SUBJECT=mailto:${email:-admin@$domain}
EOF
profiles=""
if [[ -n "$search" ]]; then
  profiles="search"
  printf 'XMSG_SEARCH_URL=http://opensearch:9200\n' >> .env.new
fi
if [[ -n "$calls" ]]; then
  profiles="${profiles:+$profiles,}calls"
  # LiveKit wants an API secret of at least 32 characters.
  printf 'XMSG_LIVEKIT_API_SECRET=%s\nXMSG_TURN_SECRET=%s\nXMSG_CALLS_UPSTREAM=calls:8088\n' \
    "$(secret XMSG_LIVEKIT_API_SECRET "openssl rand -hex 32")" "$(secret XMSG_TURN_SECRET "openssl rand -hex 24")" >> .env.new
fi
[[ -n "$profiles" ]] && printf 'COMPOSE_PROFILES=%s\n' "$profiles" >> .env.new
mv .env.new .env
say "Configuration written to $(pwd)/.env"
[[ $start -eq 1 ]] || exit 0

# ---- start ---------------------------------------------------------------------------
command -v docker >/dev/null || fail "Docker is required (https://docs.docker.com/engine/install/)"
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is required (the 'docker compose' plugin)"
command -v curl >/dev/null || fail "curl is required"
if [[ $bundle -eq 1 ]]; then
  missing=0
  while read -r image; do
    docker image inspect "$image" >/dev/null 2>&1 || missing=1
  done < images/images.txt
  if [[ $missing -eq 1 ]]; then
    if command -v sha256sum >/dev/null; then
      say "Checking the bundle"
      sha256sum --quiet -c SHA256SUMS || fail "the bundle is damaged (SHA256SUMS doesn't match)"
    fi
    say "Loading images from the bundle (version $version)"
    gunzip -c images/images.tar.gz | docker load --quiet >/dev/null
  fi
elif [[ -n "$registry" ]]; then
  say "Pulling images from $registry (version ${version:-dev})"
  if ! docker compose pull --quiet --ignore-buildable; then
    # Images loaded by hand (an offline bundle, CI) are fine as they are.
    docker image inspect "$registry/services:${version:-dev}" >/dev/null 2>&1 ||
      fail "can't pull $registry images and they aren't on this machine"
    echo "using the images already on this machine"
  fi
else
  say "Building images from this checkout (this takes a while the first time)"
  docker compose build
fi
say "Starting xmsg"
docker compose up -d --wait --wait-timeout 600 || {
  docker compose ps -a
  fail "some services didn't become healthy; see 'docker compose logs'"
}

# The whole path, as a browser takes it: Caddy (HTTPS) -> web -> auth.
say "Checking https://$domain/"
for _ in $(seq 1 60); do
  if curl -sfk --resolve "$domain:${XMSG_HTTPS_PORT:-443}:127.0.0.1" \
      "https://$domain:${XMSG_HTTPS_PORT:-443}/auth/.well-known/jwks.json" | grep -q '"keys"'; then
    say "xmsg is running at https://$domain/"
    echo "Create the first account there; back up with 'xmsgctl backup', check with 'xmsgctl health'."
    exit 0
  fi
  sleep 2
done
fail "https://$domain/ didn't answer; see 'docker compose logs caddy web auth'"
