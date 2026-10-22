# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.NotifyLevel do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.NotifyLevel",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :NOTIFY_LEVEL_UNSPECIFIED, 0
  field :NOTIFY_LEVEL_ALL, 1
  field :NOTIFY_LEVEL_MENTIONS, 2
  field :NOTIFY_LEVEL_NOTHING, 3
end

defmodule Xmsg.V1.MemberSettings do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberSettings",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :channel_id, 2, type: :int64, json_name: "channelId"
  field :notify_level, 3, type: Xmsg.V1.NotifyLevel, json_name: "notifyLevel", enum: true
  field :muted, 4, type: :bool
  field :updated_at_ms, 5, type: :int64, json_name: "updatedAtMs"
end
