#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Creates xmsg topics. N is fixed per installation (ADR-0021); never change it in place.
set -euo pipefail

N="${XMSG_PARTITIONS:?}"
KT=/opt/kafka/bin/kafka-topics.sh
DAY=$((24 * 3600 * 1000))

create() { # name partitions [config...]
  local name=$1 parts=$2; shift 2
  local args=()
  for c in "$@"; do args+=(--config "$c"); done
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor "${REPLICATION_FACTOR:-1}" "${args[@]}"
}

check_partitions() { # name expected
  local actual
  actual=$("$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$1" | awk -F'PartitionCount: ' 'NR==1{split($2,a,"\t"); print a[1]}')
  if [[ "$actual" != "$2" ]]; then
    echo "ERROR: topic $1 has $actual partitions, installation expects $2. Use xmsgctl migrate-partitions." >&2
    exit 1
  fi
}

# Channel-keyed topics: N partitions, co-partitioned.
create messages.in "$N" "retention.ms=$((3 * DAY))"
create messages.persisted "$N" "retention.ms=$((7 * DAY))"
create reactions.in "$N" "retention.ms=$DAY"
for t in messages.in messages.persisted reactions.in; do check_partitions "$t" "$N"; done

# Other topics (sizes for the Small tier).
create commands.results 4 "retention.ms=3600000"
create sessions.events 8 "cleanup.policy=compact"
create readstate.events 8 "retention.ms=$DAY"
create member.settings 8 "cleanup.policy=compact"
create channels.membership 8 "cleanup.policy=compact"
create users.events 8 "retention.ms=$((7 * DAY))"
create channels.meta 8 "retention.ms=$((7 * DAY))"
create auth.events 4 "retention.ms=$((7 * DAY))"
create persister.quarantine 1 "retention.ms=$((30 * DAY))"
create audit.events 4 "retention.ms=$((30 * DAY))"

# Sequencer changelogs and repartition topics are created by Kafka Streams itself,
# named <application.id>-<store>-changelog (services/sequencer Names.kt).
echo "xmsg topics ready (N=$N)"
