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

  field :reaction_batch, 16, type: Xmsg.V1.ReactionBatch, json_name: "reactionBatch", oneof: 0
  field :edit_message, 14, type: Xmsg.V1.EditMessage, json_name: "editMessage", oneof: 0
  field :delete_message, 15, type: Xmsg.V1.DeleteMessage, json_name: "deleteMessage", oneof: 0

  field :member_role_change_requested, 17,
    type: Xmsg.V1.MemberRoleChangeRequested,
    json_name: "memberRoleChangeRequested",
    oneof: 0
end

defmodule Xmsg.V1.EditMessage do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.EditMessage",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :msg_seq, 1, type: :int64, json_name: "msgSeq"
  field :message_id, 2, type: :int64, json_name: "messageId"
  field :body, 3, type: :string
end

defmodule Xmsg.V1.DeleteMessage do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.DeleteMessage",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :msg_seq, 1, type: :int64, json_name: "msgSeq"
  field :message_id, 2, type: :int64, json_name: "messageId"
  field :as_admin, 3, type: :bool, json_name: "asAdmin"
end

defmodule Xmsg.V1.ReactionBatch do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReactionBatch",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :message_id, 1, type: :int64, json_name: "messageId"
  field :msg_seq, 2, type: :int64, json_name: "msgSeq"
  field :emoji, 3, type: :string
  field :ops, 4, repeated: true, type: Xmsg.V1.ReactionOp
end

defmodule Xmsg.V1.ReactionOp do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReactionOp",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :add, 2, type: :bool
  field :counter, 3, type: :int64
end

defmodule Xmsg.V1.ThreadRoot do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ThreadRoot",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :msg_seq, 1, type: :int64, json_name: "msgSeq"
  field :message_id, 2, type: :int64, json_name: "messageId"
end

defmodule Xmsg.V1.SendMessage do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SendMessage",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :body, 1, type: :string
  field :thread_root, 2, type: Xmsg.V1.ThreadRoot, json_name: "threadRoot"
  field :file_ids, 3, repeated: true, type: :int64, json_name: "fileIds"
  field :attachments, 4, repeated: true, type: Xmsg.V1.Attachment
end

defmodule Xmsg.V1.Attachment do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.Attachment",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :file_id, 1, type: :int64, json_name: "fileId"
  field :name, 2, type: :string
  field :content_type, 3, type: :string, json_name: "contentType"
  field :size, 4, type: :int64
  field :width, 5, type: :int32
  field :height, 6, type: :int32
  field :has_thumbnail, 7, type: :bool, json_name: "hasThumbnail"
end

defmodule Xmsg.V1.Attachments do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.Attachments",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :items, 1, repeated: true, type: Xmsg.V1.Attachment
end

defmodule Xmsg.V1.ChannelCreated do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ChannelCreated",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :type, 1, type: Xmsg.V1.ChannelType, enum: true
  field :initial_member_ids, 2, repeated: true, type: :int64, json_name: "initialMemberIds"
  field :members, 3, repeated: true, type: Xmsg.V1.MemberJoined
  field :name, 4, type: :string
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

defmodule Xmsg.V1.MemberRoleChangeRequested do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberRoleChangeRequested",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :role, 2, type: Xmsg.V1.MemberRole, enum: true
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
  field :channel_type, 16, type: Xmsg.V1.ChannelType, json_name: "channelType", enum: true
  field :message_created, 10, type: Xmsg.V1.MessageCreated, json_name: "messageCreated", oneof: 0
  field :channel_created, 11, type: Xmsg.V1.ChannelCreated, json_name: "channelCreated", oneof: 0
  field :member_joined, 12, type: Xmsg.V1.MemberJoined, json_name: "memberJoined", oneof: 0
  field :member_left, 13, type: Xmsg.V1.MemberLeft, json_name: "memberLeft", oneof: 0
  field :rejected, 14, type: Xmsg.V1.Rejected, oneof: 0

  field :channel_mode_changed, 15,
    type: Xmsg.V1.ChannelModeChanged,
    json_name: "channelModeChanged",
    oneof: 0

  field :reactions_changed, 19,
    type: Xmsg.V1.ReactionsChanged,
    json_name: "reactionsChanged",
    oneof: 0

  field :thread_reply, 20, type: Xmsg.V1.ThreadReply, json_name: "threadReply", oneof: 0
  field :message_edited, 17, type: Xmsg.V1.MessageEdited, json_name: "messageEdited", oneof: 0
  field :message_deleted, 18, type: Xmsg.V1.MessageDeleted, json_name: "messageDeleted", oneof: 0

  field :member_role_changed, 21,
    type: Xmsg.V1.MemberRoleChanged,
    json_name: "memberRoleChanged",
    oneof: 0
end

defmodule Xmsg.V1.MessageEdited do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MessageEdited",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :msg_seq, 1, type: :int64, json_name: "msgSeq"
  field :message_id, 2, type: :int64, json_name: "messageId"
  field :body, 3, type: :string
  field :edited_at_ms, 4, type: :int64, json_name: "editedAtMs"
end

defmodule Xmsg.V1.MessageDeleted do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MessageDeleted",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :msg_seq, 1, type: :int64, json_name: "msgSeq"
  field :message_id, 2, type: :int64, json_name: "messageId"
end

defmodule Xmsg.V1.MessageCreated do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MessageCreated",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :message_id, 1, type: :int64, json_name: "messageId"
  field :body, 2, type: :string
  field :mentioned_user_ids, 3, repeated: true, type: :int64, json_name: "mentionedUserIds"
  field :mentions_channel, 4, type: :bool, json_name: "mentionsChannel"
  field :attachments, 5, repeated: true, type: Xmsg.V1.Attachment
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

defmodule Xmsg.V1.MemberRoleChanged do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.MemberRoleChanged",
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

defmodule Xmsg.V1.ThreadReply do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ThreadReply",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :message_id, 1, type: :int64, json_name: "messageId"
  field :body, 2, type: :string
  field :root_msg_seq, 3, type: :int64, json_name: "rootMsgSeq"
  field :root_message_id, 4, type: :int64, json_name: "rootMessageId"
  field :reply_seq, 5, type: :int64, json_name: "replySeq"
  field :reply_count, 6, type: :int64, json_name: "replyCount"
  field :mentioned_user_ids, 7, repeated: true, type: :int64, json_name: "mentionedUserIds"
  field :attachments, 8, repeated: true, type: Xmsg.V1.Attachment
end

defmodule Xmsg.V1.ReactionsChanged do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReactionsChanged",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :message_id, 1, type: :int64, json_name: "messageId"
  field :msg_seq, 2, type: :int64, json_name: "msgSeq"
  field :emoji, 3, type: :string
  field :count, 4, type: :int64
  field :added, 5, repeated: true, type: :int64
  field :removed, 6, repeated: true, type: :int64
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
  field :retry_after_ms, 4, type: :uint32, json_name: "retryAfterMs"
end
