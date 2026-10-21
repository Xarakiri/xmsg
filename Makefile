# SPDX-License-Identifier: AGPL-3.0-only
.PHONY: test test-rust test-ts test-elixir test-kotlin test-integration lint proto-lint vectors-check up down ps

JAVA_HOME ?= $(shell /usr/libexec/java_home -v 21 2>/dev/null || echo /opt/homebrew/opt/openjdk@21)
export JAVA_HOME

test: test-rust test-ts test-elixir test-kotlin

test-rust:
	cargo test --workspace

test-ts:
	cd clients && npm ci && npm test --workspace @xmsg/client-core && npm run build --workspace @xmsg/web

test-elixir:
	cd services/gateway && mix test

test-kotlin:
	cd services/sequencer && ./gradlew test

# Needs Docker (Testcontainers starts a real Kafka). KILLS sets crash-test intensity.
KILLS ?= 12
test-integration:
	cd services/sequencer && ./gradlew integrationTest -Dxmsg.crash.kills=$(KILLS)

lint: proto-lint vectors-check
	cargo fmt --check
	cargo clippy --workspace --all-targets -- -D warnings
	cd services/gateway && mix format --check-formatted
	tools/check-spdx.sh

proto-lint:
	cd proto && buf lint

# The committed vectors must match what the generator produces.
vectors-check:
	python3 -I tools/gen-partition-vectors.py /tmp/xmsg-vectors.csv >/dev/null
	diff -u spec/partitioning/vectors.csv /tmp/xmsg-vectors.csv

up:
	cd deploy/compose && docker compose up -d

down:
	cd deploy/compose && docker compose down

ps:
	cd deploy/compose && docker compose ps
