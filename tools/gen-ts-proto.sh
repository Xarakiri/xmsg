#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Generates TypeScript-typed JavaScript (protoc-gen-es, js + .d.ts) for proto/ into
# clients/core/src/gen. JS rather than TS: Node runs the client tests by stripping
# types, which can't handle TS enums. Needs protoc and `npm install` in clients/.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

out=clients/core/src/gen
rm -rf "$out" && mkdir -p "$out"
protoc -I proto \
  --plugin=protoc-gen-es=clients/node_modules/.bin/protoc-gen-es \
  --es_out="$out" --es_opt=target=js+dts,import_extension=js \
  proto/xmsg/v1/*.proto

for f in $(find "$out" -name '*.js' -o -name '*.d.ts'); do
  { echo "// SPDX-License-Identifier: Apache-2.0"; echo "// Generated from proto/ by tools/gen-ts-proto.sh. Do not edit."; cat "$f"; } > "$f.tmp"
  mv "$f.tmp" "$f"
done
echo "generated $(find "$out" -name '*.js' | wc -l | tr -d ' ') modules in $out"
