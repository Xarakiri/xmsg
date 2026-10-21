# SPDX-License-Identifier: AGPL-3.0-only
# Generated from proto/ by tools/gen-elixir-proto.sh. Do not edit.
defmodule Xmsg.V1.ChannelType do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.ChannelType",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :CHANNEL_TYPE_UNSPECIFIED, 0
  field :CHANNEL_TYPE_PUBLIC, 1
  field :CHANNEL_TYPE_PRIVATE, 2
  field :CHANNEL_TYPE_DM, 3
  field :CHANNEL_TYPE_GROUP_DM, 4
  field :CHANNEL_TYPE_BROADCAST, 5
end

defmodule Xmsg.V1.FanoutMode do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.FanoutMode",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :FANOUT_MODE_UNSPECIFIED, 0
  field :FANOUT_MODE_PUSH, 1
  field :FANOUT_MODE_PULL, 2
end

defmodule Xmsg.V1.MemberRole do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.MemberRole",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :MEMBER_ROLE_UNSPECIFIED, 0
  field :MEMBER_ROLE_MEMBER, 1
  field :MEMBER_ROLE_ADMIN, 2
  field :MEMBER_ROLE_POSTER, 3
end

defmodule Xmsg.V1.RejectReason do
  @moduledoc false

  use Protobuf,
    enum: true,
    full_name: "xmsg.v1.RejectReason",
    protoc_gen_elixir_version: "0.17.1",
    syntax: :proto3

  field :REJECT_REASON_UNSPECIFIED, 0
  field :REJECT_REASON_NOT_A_MEMBER, 1
  field :REJECT_REASON_UNKNOWN_CHANNEL, 2
  field :REJECT_REASON_FORBIDDEN, 3
  field :REJECT_REASON_INVALID, 4
end
