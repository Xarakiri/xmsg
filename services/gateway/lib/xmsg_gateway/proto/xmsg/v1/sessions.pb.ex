# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.AppState do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.AppState",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :APP_STATE_UNSPECIFIED, 0
  field :APP_STATE_FOREGROUND, 1
  field :APP_STATE_BACKGROUND, 2
end

defmodule Xmsg.V1.SessionEvent do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.SessionEvent",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :conn_id, 1, type: :string, json_name: "connId"
  field :user_id, 2, type: :int64, json_name: "userId"
  field :gateway_id, 3, type: :string, json_name: "gatewayId"
  field :app_state, 4, type: Xmsg.V1.AppState, json_name: "appState", enum: true
  field :connected_at_ms, 5, type: :int64, json_name: "connectedAtMs"
end
