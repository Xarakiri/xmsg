# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ConnectionState do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.ConnectionState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :CONNECTION_STATE_UNSPECIFIED, 0
  field :CONNECTION_STATE_ACTIVE, 1
  field :CONNECTION_STATE_IDLE, 2
  field :CONNECTION_STATE_GONE, 3
end

defmodule Xmsg.V1.PresencePing do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.PresencePing",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :conn_id, 2, type: :string, json_name: "connId"
  field :state, 3, type: Xmsg.V1.ConnectionState, enum: true
end

defmodule Xmsg.V1.PresenceUpdate do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.PresenceUpdate",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :status, 2, type: Xmsg.V1.PresenceStatus, enum: true
end

defmodule Xmsg.V1.PresenceQuery do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.PresenceQuery",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_ids, 1, repeated: true, type: :int64, json_name: "userIds"
end

defmodule Xmsg.V1.PresenceReply do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.PresenceReply",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :updates, 1, repeated: true, type: Xmsg.V1.PresenceUpdate
end
