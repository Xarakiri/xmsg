# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.AuthEvent do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.AuthEvent",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  oneof :event, 0

  field :session_revoked, 1, type: Xmsg.V1.SessionRevoked, json_name: "sessionRevoked", oneof: 0

  field :workspace_member_removed, 2,
    type: Xmsg.V1.WorkspaceMemberRemoved,
    json_name: "workspaceMemberRemoved",
    oneof: 0
end

defmodule Xmsg.V1.SessionRevoked do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SessionRevoked",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_id, 1, type: :int64, json_name: "userId"
  field :session_id, 2, type: :int64, json_name: "sessionId"
  field :revoked_at_ms, 3, type: :int64, json_name: "revokedAtMs"
  field :refuse_until_ms, 4, type: :int64, json_name: "refuseUntilMs"
  field :reason, 5, type: :string
end
