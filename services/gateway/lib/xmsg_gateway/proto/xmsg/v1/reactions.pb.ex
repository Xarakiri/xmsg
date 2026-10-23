# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ReactionCommand do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReactionCommand",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :message_id, 2, type: :int64, json_name: "messageId"
  field :msg_seq, 3, type: :int64, json_name: "msgSeq"
  field :user_id, 4, type: :int64, json_name: "userId"
  field :emoji, 5, type: :string
  field :add, 6, type: :bool
  field :counter, 7, type: :int64
end
