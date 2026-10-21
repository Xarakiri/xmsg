# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.GatewayBatch do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.GatewayBatch",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :partition, 1, type: :uint32
  field :owner_epoch, 2, type: :uint64, json_name: "ownerEpoch"
  field :seq, 3, type: :uint64
  field :deliveries, 4, repeated: true, type: Xmsg.V1.Delivery
end

defmodule Xmsg.V1.Delivery do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.Delivery",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :user_ids, 1, repeated: true, type: :int64, json_name: "userIds"
  field :server_frame, 2, type: :bytes, json_name: "serverFrame"
end

defmodule Xmsg.V1.StreamHeads do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.StreamHeads",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :heads, 1, repeated: true, type: Xmsg.V1.StreamHead
end

defmodule Xmsg.V1.StreamHead do
  @moduledoc false

  use Protobuf,
    full_name: "xmsg.v1.StreamHead",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :partition, 1, type: :uint32
  field :owner_epoch, 2, type: :uint64, json_name: "ownerEpoch"
  field :seq, 3, type: :uint64
end
