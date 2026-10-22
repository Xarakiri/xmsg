# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ChannelMembers do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ChannelMembers",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :membership_version, 2, type: :int64, json_name: "membershipVersion"
  field :user_ids, 3, repeated: true, type: :int64, json_name: "userIds"
end
