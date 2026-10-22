#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Registers (or updates) the outbox connector with the Kafka Connect REST API.
set -euo pipefail
url=${XMSG_CONNECT_URL:-http://127.0.0.1:8093}
dir=$(cd "$(dirname "$0")" && pwd)
config=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$dir/outbox-connector.json")
curl -fsS -X PUT -H 'Content-Type: application/json' --data "$config" "$url/connectors/xmsg-outbox/config" >/dev/null
for _ in $(seq 1 60); do
  state=$(curl -fsS "$url/connectors/xmsg-outbox/status" | python3 -c 'import json,sys; s=json.load(sys.stdin); t=s["tasks"]; print(s["connector"]["state"], t[0]["state"] if t else "NONE")')
  case "$state" in
    "RUNNING RUNNING") echo "xmsg-outbox connector running"; exit 0 ;;
    *FAILED*) curl -fsS "$url/connectors/xmsg-outbox/status"; echo; exit 1 ;;
  esac
  sleep 1
done
echo "xmsg-outbox connector not running: $state"; exit 1
