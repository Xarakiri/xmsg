#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
#
# Builds the air-gapped Small-tier bundle (plan §12.2): every image the installation runs
# plus the compose files, install.sh and (optionally) xmsgctl, in one archive. On a
# machine without registry access, unpack it and run ./install.sh: it loads the images
# from the bundle instead of pulling.
#
#   ./make-bundle.sh --version 0.5.0 --registry ghcr.io/xmsg   # published images (pulled if missing)
#   ./make-bundle.sh --version dev --build                     # images built from this checkout
#
# Options:
#   --registry R --version V   the xmsg images to bundle: R/<name>:V (default xmsg, dev)
#   --build              build the xmsg images from this checkout first
#   --no-search          leave out OpenSearch and the indexer (about 1 GB smaller; search
#                        can't be turned on later without registry access)
#   --xmsgctl FILE       include this xmsgctl binary (built for the target machine)
#   --out DIR            where to write the archive (default: the current directory)
set -euo pipefail

registry=xmsg version=dev build=0 search=1 xmsgctl="" out=$PWD
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry) registry="$2"; shift 2 ;;
    --version) version="$2"; shift 2 ;;
    --build) build=1; shift ;;
    --no-search) search=0; shift ;;
    --xmsgctl) xmsgctl="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    -h|--help) sed -n '3,18p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown option: $1 (see --help)" >&2; exit 2 ;;
  esac
done
say() { printf '\033[1m%s\033[0m\n' "$*"; }
fail() { echo "error: $*" >&2; exit 1; }
[[ -z "$xmsgctl" || -x "$xmsgctl" ]] || fail "--xmsgctl must be an executable file"
mkdir -p "$out" && out=$(cd "$out" && pwd)
[[ -n "$xmsgctl" ]] && xmsgctl=$(cd "$(dirname "$xmsgctl")" && pwd)/$(basename "$xmsgctl")
cd "$(dirname "$0")"

# Compose with this bundle's images; the secrets only need to be set for `config`.
compose() {
  env COMPOSE_FILE=docker-compose.yml:docker-compose.app.yml \
    COMPOSE_PROFILES="$([[ $search -eq 1 ]] && echo search)" \
    XMSG_REGISTRY="$registry" XMSG_VERSION="$version" \
    XMSG_AUTH_KEY=- XMSG_POSTGRES_PASSWORD=- XMSG_PUSH_VAPID_PRIVATE_KEY=- \
    XMSG_GARAGE_ADMIN_TOKEN=- XMSG_MEDIA_S3_KEY_ID=- XMSG_MEDIA_S3_SECRET=- \
    docker compose --env-file /dev/null "$@"
}

if [[ $build -eq 1 ]]; then
  say "Building the xmsg images ($registry/*:$version)"
  compose build
fi
# Every image a service uses (xmsgctl's helper is postgres, already among them).
images=$(compose config --images | sort -u)
for image in $images; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    say "Pulling $image"
    docker pull --quiet "$image" >/dev/null || fail "$image isn't on this machine and can't be pulled"
  fi
done
arch=$(docker image inspect --format '{{.Architecture}}' "$registry/services:$version")

name="xmsg-small-$version-linux-$arch"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
stage="$work/$name"
mkdir -p "$stage/images" "$stage/certs"
cp README.md install.sh docker-compose.yml docker-compose.app.yml "$stage/"
cp -R caddy init "$stage/"
cp certs/.gitignore "$stage/certs/"
[[ -n "$xmsgctl" ]] && cp "$xmsgctl" "$stage/xmsgctl"

say "Saving $(echo "$images" | wc -l | tr -d ' ') images"
# shellcheck disable=SC2086 # one argument per image
docker save $images | gzip -1 > "$stage/images/images.tar.gz"
echo "$images" > "$stage/images/images.txt"
cat > "$stage/bundle.env" <<EOF
# The images in images/images.tar.gz; install.sh loads them and uses these.
XMSG_REGISTRY=$registry
XMSG_VERSION=$version
XMSG_BUNDLE_SEARCH=$search
EOF
(cd "$stage" && find . -type f ! -name SHA256SUMS | sort | xargs sha256sum > SHA256SUMS 2>/dev/null ||
  find . -type f ! -name SHA256SUMS | sort | xargs shasum -a 256 > SHA256SUMS)

say "Writing $out/$name.tar"
# Already compressed inside; the outer archive is a plain tar.
tar -C "$work" -cf "$out/$name.tar" "$name"
ls -lh "$out/$name.tar" | awk '{print $5, $NF}'
echo "Install offline: tar -xf $name.tar && cd $name && ./install.sh"
