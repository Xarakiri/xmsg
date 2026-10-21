# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.EventPage do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.EventPage",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :events, 1, repeated: true, type: Xmsg.V1.SequencedEvent
  field :has_more, 2, type: :bool, json_name: "hasMore"
  field :head_event_seq, 3, type: :int64, json_name: "headEventSeq"
  field :truncated, 4, type: :bool
end
