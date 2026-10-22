# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ReadStateEvent do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReadStateEvent",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :change, 0

  field :user_id, 1, type: :int64, json_name: "userId"
  field :channel_id, 2, type: :int64, json_name: "channelId"
  field :at_ms, 3, type: :int64, json_name: "atMs"
  field :read_up_to, 10, type: :int64, json_name: "readUpTo", oneof: 0
  field :unread_from, 11, type: :int64, json_name: "unreadFrom", oneof: 0
end
