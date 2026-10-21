#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Generates Elixir modules for proto/ into services/gateway/lib/xmsg_gateway/proto.
# Needs protoc and protoc-gen-elixir (mix escript.install hex protobuf <version>).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export PATH="$HOME/.mix/escripts:$PATH"

out=services/gateway/lib/xmsg_gateway/proto
rm -rf "$out" && mkdir -p "$out"
protoc -I proto --elixir_out="$out" proto/xmsg/v1/*.proto

for f in $(find "$out" -name '*.pb.ex'); do
  { echo "# SPDX-License-Identifier: AGPL-3.0-only"; echo "# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit."; cat "$f"; } > "$f.tmp"
  mv "$f.tmp" "$f"
done
echo "generated $(find "$out" -name '*.pb.ex' | wc -l | tr -d ' ') files in $out"
