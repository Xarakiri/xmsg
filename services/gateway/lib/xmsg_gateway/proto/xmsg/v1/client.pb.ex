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
end

defmodule Xmsg.V1.Welcome do
  @moduledoc false

  use Protobuf, full_name: "xmsg.v1.Welcome", protoc_gen_elixir_version: "0.17.1", syntax: :proto3

  field :resume_token, 1, type: :string, json_name: "resumeToken"
  field :partition_count, 2, type: :uint32, json_name: "partitionCount"
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
