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
#   --no-start           write the configuration only
#   --yes                no questions; defaults for anything not given
set -euo pipefail
cd "$(dirname "$0")"

domain="" tls="" email="" cert="" key="" registry="" version="" partitions="" search="" start=1 ask=1
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
[[ -n "$(existing COMPOSE_PROFILES)" ]] && : "${search:=1}"
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
# Web Push (VAPID): the raw 32-byte P-256 private key, base64url (see services/push).
vapid=$(secret XMSG_PUSH_VAPID_PRIVATE_KEY \
  "openssl ecparam -genkey -name prime256v1 -noout | openssl ec -outform DER 2>/dev/null | tail -c +8 | head -c 32 | base64 | tr '/+' '_-' | tr -d '=\n'")

umask 077
cat > .env.new <<EOF
# Written by install.sh on $(date -u +%Y-%m-%dT%H:%M:%SZ). Keep this file private: it holds the
# installation's secrets. Re-running install.sh keeps them.
COMPOSE_FILE=docker-compose.yml:docker-compose.app.yml
XMSG_DOMAIN=$domain
XMSG_TLS=$tls_args
XMSG_PARTITIONS=$partitions
XMSG_REGISTRY=${registry:-xmsg}
XMSG_VERSION=${version:-dev}
XMSG_AUTH_KEY=$auth_key
XMSG_POSTGRES_PASSWORD=$postgres_password
XMSG_KAFKA_CLUSTER_ID=$kafka_cluster_id
XMSG_PUSH_VAPID_PRIVATE_KEY=$vapid
XMSG_PUSH_SUBJECT=mailto:${email:-admin@$domain}
EOF
if [[ -n "$search" ]]; then
  printf 'COMPOSE_PROFILES=search\nXMSG_SEARCH_URL=http://opensearch:9200\n' >> .env.new
fi
mv .env.new .env
say "Configuration written to $(pwd)/.env"
[[ $start -eq 1 ]] || exit 0

# ---- start ---------------------------------------------------------------------------
command -v docker >/dev/null || fail "Docker is required (https://docs.docker.com/engine/install/)"
docker compose version >/dev/null 2>&1 || fail "Docker Compose v2 is required (the 'docker compose' plugin)"
command -v curl >/dev/null || fail "curl is required"
if [[ -n "$registry" ]]; then
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
