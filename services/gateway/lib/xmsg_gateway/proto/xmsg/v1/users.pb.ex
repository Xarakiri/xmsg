# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.UsersEvent do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.UsersEvent",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :event, 0

  field :member_removed, 1,
    type: Xmsg.V1.WorkspaceMemberRemoved,
    json_name: "memberRemoved",
    oneof: 0

  field :removal_completed, 2,
    type: Xmsg.V1.WorkspaceMemberRemovalCompleted,
    json_name: "removalCompleted",
    oneof: 0
end

defmodule Xmsg.V1.WorkspaceMemberRemoved do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.WorkspaceMemberRemoved",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :workspace_id, 1, type: :int64, json_name: "workspaceId"
  field :user_id, 2, type: :int64, json_name: "userId"
  field :removed_by, 3, type: :int64, json_name: "removedBy"
  field :removed_at_ms, 4, type: :int64, json_name: "removedAtMs"
end

defmodule Xmsg.V1.WorkspaceMemberRemovalCompleted do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.WorkspaceMemberRemovalCompleted",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :workspace_id, 1, type: :int64, json_name: "workspaceId"
  field :user_id, 2, type: :int64, json_name: "userId"
  field :channels, 3, type: :int32
  field :completed_at_ms, 4, type: :int64, json_name: "completedAtMs"
end
