# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ClientFrame do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ClientFrame",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :frame, 0

  field :connect, 1, type: Xmsg.V1.Connect, oneof: 0
  field :send_message, 2, type: Xmsg.V1.SendMessageRequest, json_name: "sendMessage", oneof: 0
  field :ping, 3, type: Xmsg.V1.Ping, oneof: 0
  field :typing, 4, type: Xmsg.V1.Typing, oneof: 0
  field :watch_presence, 5, type: Xmsg.V1.WatchPresence, json_name: "watchPresence", oneof: 0
  field :activity, 6, type: Xmsg.V1.Activity, oneof: 0
  field :receipt, 7, type: Xmsg.V1.Receipt, oneof: 0
  field :view_channel, 8, type: Xmsg.V1.ViewChannel, json_name: "viewChannel", oneof: 0
end

defmodule Xmsg.V1.Connect do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Connect", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :access_token, 1, type: :string, json_name: "accessToken"
  field :protocol_version, 2, type: :uint32, json_name: "protocolVersion"
  field :sync_token, 3, type: :string, json_name: "syncToken"
  field :resume_token, 4, type: :string, json_name: "resumeToken"
end

defmodule Xmsg.V1.SendMessageRequest do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SendMessageRequest",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :client_msg_id, 2, type: :string, json_name: "clientMsgId"
  field :body, 3, type: :string
  field :thread_root, 4, type: Xmsg.V1.ThreadRoot, json_name: "threadRoot"
  field :file_ids, 5, repeated: true, type: :int64, json_name: "fileIds"
end

defmodule Xmsg.V1.Typing do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Typing", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
end

defmodule Xmsg.V1.WatchPresence do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.WatchPresence",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_ids, 1, repeated: true, type: :int64, json_name: "userIds"
end

defmodule Xmsg.V1.Receipt do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Receipt", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :msg_seq, 2, type: :int64, json_name: "msgSeq"
end

defmodule Xmsg.V1.Activity do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.Activity",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :active, 1, type: :bool
end

defmodule Xmsg.V1.ViewChannel do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ViewChannel",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
end

defmodule Xmsg.V1.Ping do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Ping", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :sent_at_ms, 1, type: :int64, json_name: "sentAtMs"
end

defmodule Xmsg.V1.ServerFrame do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ServerFrame",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :frame, 0

  field :welcome, 1, type: Xmsg.V1.Welcome, oneof: 0
  field :ack, 2, type: Xmsg.V1.SendAck, oneof: 0
  field :event, 3, type: Xmsg.V1.SequencedEvent, oneof: 0
  field :resync, 4, type: Xmsg.V1.Resync, oneof: 0
  field :pong, 5, type: Xmsg.V1.Pong, oneof: 0
  field :send_rejected, 6, type: Xmsg.V1.SendRejected, json_name: "sendRejected", oneof: 0
  field :typing, 7, type: Xmsg.V1.TypingIndicator, oneof: 0
  field :presence, 8, type: Xmsg.V1.PresenceUpdates, oneof: 0
  field :receipt, 9, type: Xmsg.V1.ReadReceipt, oneof: 0
  field :bump, 10, type: Xmsg.V1.ChannelBump, oneof: 0
end

defmodule Xmsg.V1.ChannelBump do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ChannelBump",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :event_seq, 2, type: :int64, json_name: "eventSeq"
  field :msg_seq, 3, type: :int64, json_name: "msgSeq"
end

defmodule Xmsg.V1.Welcome do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Welcome", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :resume_token, 1, type: :string, json_name: "resumeToken"
  field :partition_count, 2, type: :uint32, json_name: "partitionCount"
end

defmodule Xmsg.V1.TypingIndicator do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.TypingIndicator",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :user_id, 2, type: :int64, json_name: "userId"
end

defmodule Xmsg.V1.ReadReceipt do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.ReadReceipt",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :channel_id, 1, type: :int64, json_name: "channelId"
  field :user_id, 2, type: :int64, json_name: "userId"
  field :msg_seq, 3, type: :int64, json_name: "msgSeq"
end

defmodule Xmsg.V1.PresenceUpdates do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.PresenceUpdates",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :updates, 1, repeated: true, type: Xmsg.V1.PresenceUpdate
end

defmodule Xmsg.V1.SendRejected do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SendRejected",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :client_msg_id, 1, type: :string, json_name: "clientMsgId"
  field :reason, 2, type: Xmsg.V1.RejectReason, enum: true
  field :detail, 3, type: :string
  field :retry_after_ms, 4, type: :uint32, json_name: "retryAfterMs"
end

defmodule Xmsg.V1.SendAck do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.SendAck", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :client_msg_id, 1, type: :string, json_name: "clientMsgId"
end

defmodule Xmsg.V1.Resync do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Resync", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :partition, 1, proto3_optional: true, type: :uint32
end

defmodule Xmsg.V1.Pong do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Pong", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :sent_at_ms, 1, type: :int64, json_name: "sentAtMs"
end
