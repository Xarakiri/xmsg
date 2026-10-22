#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Registers (or updates) the outbox connector with the Kafka Connect REST API.
set -euo pipefail
url=${XMSG_CONNECT_URL:-http://127.0.0.1:8093}
dir=$(cd "$(dirname "$0")" && pwd)
config=$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$dir/outbox-connector.json")
curl -fsS -X PUT -H 'Content-Type: application/json' --data "$config" "$url/connectors/xmsg-outbox/config" >/dev/null
for _ in $(seq 1 60); do
  # Right after the PUT the status can still be 404 (not written yet): keep waiting.
  status=$(curl -fsS "$url/connectors/xmsg-outbox/status" 2>/dev/null || true)
  state=$(printf '%s' "$status" | python3 -c 'import json,sys
try:
    s = json.load(sys.stdin); t = s["tasks"]
    print(s["connector"]["state"], t[0]["state"] if t else "NONE")
except Exception:
    print("PENDING")')
  case "$state" in
    "RUNNING RUNNING") echo "xmsg-outbox connector running"; exit 0 ;;
    *FAILED*) curl -fsS "$url/connectors/xmsg-outbox/status"; echo; exit 1 ;;
  esac
  sleep 1
done
echo "xmsg-outbox connector not running: $state"; exit 1
