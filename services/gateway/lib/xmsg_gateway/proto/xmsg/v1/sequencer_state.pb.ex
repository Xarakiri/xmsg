# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ChannelHeadState do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ChannelHeadState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :format_version, 1, type: :uint32, json_name: "formatVersion"
  field :workspace_id, 2, type: :int64, json_name: "workspaceId"
  field :type, 3, type: Xmsg.V1.ChannelType, enum: true
  field :event_seq, 4, type: :int64, json_name: "eventSeq"
  field :msg_seq, 5, type: :int64, json_name: "msgSeq"
  field :membership_version, 6, type: :int64, json_name: "membershipVersion"
  field :fanout_mode, 7, type: Xmsg.V1.FanoutMode, json_name: "fanoutMode", enum: true
  field :member_count, 8, type: :int64, json_name: "memberCount"
end

defmodule Xmsg.V1.MemberState do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :format_version, 1, type: :uint32, json_name: "formatVersion"
  field :role, 2, type: Xmsg.V1.MemberRole, enum: true
end

defmodule Xmsg.V1.PartitionState do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.PartitionState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :format_version, 1, type: :uint32, json_name: "formatVersion"
  field :last_message_id, 2, type: :int64, json_name: "lastMessageId"
end

defmodule Xmsg.V1.ReactionState do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReactionState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :format_version, 1, type: :uint32, json_name: "formatVersion"
  field :counter, 2, type: :int64
  field :present, 3, type: :bool
end

defmodule Xmsg.V1.ReactionCountState do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReactionCountState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :format_version, 1, type: :uint32, json_name: "formatVersion"
  field :count, 2, type: :int64
end

defmodule Xmsg.V1.ThreadState do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ThreadState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :format_version, 1, type: :uint32, json_name: "formatVersion"
  field :root_message_id, 2, type: :int64, json_name: "rootMessageId"
  field :reply_count, 3, type: :int64, json_name: "replyCount"
end
