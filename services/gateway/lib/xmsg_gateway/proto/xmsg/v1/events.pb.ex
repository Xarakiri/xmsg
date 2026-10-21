# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.InboundEvent do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.InboundEvent",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :payload, 0

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :workspace_id, 2, type: :int64, json_name: "workspaceId"
  field :actor_id, 3, type: :int64, json_name: "actorId"
  field :dedupe_id, 4, type: :string, json_name: "dedupeId"
  field :request_id, 5, type: :string, json_name: "requestId"
  field :send_message, 10, type: Xmsg.V1.SendMessage, json_name: "sendMessage", oneof: 0
  field :channel_created, 11, type: Xmsg.V1.ChannelCreated, json_name: "channelCreated", oneof: 0

  field :member_join_requested, 12,
    type: Xmsg.V1.MemberJoinRequested,
    json_name: "memberJoinRequested",
    oneof: 0

  field :member_kick_requested, 13,
    type: Xmsg.V1.MemberKickRequested,
    json_name: "memberKickRequested",
    oneof: 0
end

defmodule Xmsg.V1.SendMessage do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SendMessage",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :body, 1, type: :string
end

defmodule Xmsg.V1.ChannelCreated do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ChannelCreated",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :type, 1, type: Xmsg.V1.ChannelType, enum: true
  field :initial_member_ids, 2, repeated: true, type: :int64, json_name: "initialMemberIds"
end

defmodule Xmsg.V1.MemberJoinRequested do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberJoinRequested",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
end

defmodule Xmsg.V1.MemberKickRequested do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberKickRequested",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
end

defmodule Xmsg.V1.SequencedEvent do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SequencedEvent",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :payload, 0

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :workspace_id, 2, type: :int64, json_name: "workspaceId"
  field :actor_id, 3, type: :int64, json_name: "actorId"
  field :event_seq, 4, type: :int64, json_name: "eventSeq"
  field :msg_seq, 5, type: :int64, json_name: "msgSeq"
  field :membership_version, 6, type: :int64, json_name: "membershipVersion"
  field :fanout_mode, 7, type: Xmsg.V1.FanoutMode, json_name: "fanoutMode", enum: true
  field :dedupe_id, 8, type: :string, json_name: "dedupeId"
  field :request_id, 9, type: :string, json_name: "requestId"
  field :message_created, 10, type: Xmsg.V1.MessageCreated, json_name: "messageCreated", oneof: 0
  field :channel_created, 11, type: Xmsg.V1.ChannelCreated, json_name: "channelCreated", oneof: 0
  field :member_joined, 12, type: Xmsg.V1.MemberJoined, json_name: "memberJoined", oneof: 0
  field :member_left, 13, type: Xmsg.V1.MemberLeft, json_name: "memberLeft", oneof: 0
  field :rejected, 14, type: Xmsg.V1.Rejected, oneof: 0

  field :channel_mode_changed, 15,
    type: Xmsg.V1.ChannelModeChanged,
    json_name: "channelModeChanged",
    oneof: 0
end

defmodule Xmsg.V1.MessageCreated do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MessageCreated",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :message_id, 1, type: :int64, json_name: "messageId"
  field :body, 2, type: :string
end

defmodule Xmsg.V1.MemberJoined do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberJoined",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :role, 2, type: Xmsg.V1.MemberRole, enum: true
end

defmodule Xmsg.V1.MemberLeft do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberLeft",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
end

defmodule Xmsg.V1.ChannelModeChanged do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ChannelModeChanged",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :mode, 1, type: Xmsg.V1.FanoutMode, enum: true
end

defmodule Xmsg.V1.Rejected do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.Rejected",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :reason, 1, type: Xmsg.V1.RejectReason, enum: true
  field :detail, 2, type: :string
end

defmodule Xmsg.V1.CommandResult do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.CommandResult",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :request_id, 1, type: :string, json_name: "requestId"
  field :channel_id, 2, type: :int64, json_name: "channelId"
  field :accepted, 3, type: :bool
  field :reason, 4, type: Xmsg.V1.RejectReason, enum: true
  field :event_seq, 5, type: :int64, json_name: "eventSeq"
end

defmodule Xmsg.V1.SendResult do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SendResult",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :accepted, 1, type: :bool
  field :reason, 2, type: Xmsg.V1.RejectReason, enum: true
  field :detail, 3, type: :string
end
