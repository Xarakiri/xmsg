# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Config do
  @moduledoc "Gateway settings (config/*.exs, overridden from the environment in runtime.exs)."

  defp get(key), do: Application.fetch_env!(:xmsg_gateway, key)

  def gateway_id, do: get(:gateway_id)
  def port, do: get(:port)
  def partition_count, do: get(:partition_count)
  def auth_timeout_ms, do: get(:auth_timeout_ms)
  def request_timeout_ms, do: get(:request_timeout_ms)
  def dev_auth?, do: get(:dev_auth)
  def jwks_url, do: Application.get_env(:xmsg_gateway, :jwks_url)
  def bus, do: get(:bus)
  def nats, do: get(:nats)
  def sessions, do: get(:sessions)
  def kafka_bootstrap, do: get(:kafka_bootstrap)
  def revocations_replay?, do: Application.get_env(:xmsg_gateway, :revocations_replay, false)

  # Admission control and per-connection limits (XmsgGateway.Admission, XmsgGateway.Socket).
  def admission_rate, do: Application.get_env(:xmsg_gateway, :admission_rate, 2_000)
  def max_connections, do: Application.get_env(:xmsg_gateway, :max_connections, 0)
  def preauth_per_ip, do: Application.get_env(:xmsg_gateway, :preauth_per_ip, 100)
  def proxy_hops, do: Application.get_env(:xmsg_gateway, :proxy_hops, 0)
  @doc "Client frames per second per connection, and the burst allowed above it."
  def frame_rate, do: Application.get_env(:xmsg_gateway, :frame_rate, 30)
  def frame_burst, do: Application.get_env(:xmsg_gateway, :frame_burst, 100)
  @doc "How long a shutdown spreads closing this gateway's connections (XmsgGateway.Drain)."
  def drain_seconds, do: Application.get_env(:xmsg_gateway, :drain_seconds, 300)

  @doc "`kafka_bootstrap` as brod endpoints."
  def kafka_endpoints do
    kafka_bootstrap()
    |> String.split(",", trim: true)
    |> Enum.map(fn hostport ->
      [host, port] = String.split(hostport, ":")
      {String.to_charlist(host), String.to_integer(port)}
    end)
  end

  def sessions_topic, do: "sessions.events"

  @doc "NATS subject carrying fan-out batches for this gateway (ADR-0009)."
  def delivery_subject, do: "gw.#{gateway_id()}"
  def stream_heads_subject, do: "stream_heads.#{gateway_id()}"

  @doc """
  Best-effort frames for this gateway (typing): plain `Delivery` messages outside the
  sequenced stream, so losing one is never a gap.
  """
  def ephemeral_subject, do: "gw.#{gateway_id()}.ephemeral"

  @doc "Pull-mode subscription changes from fan-out for this gateway (XmsgGateway.Pull)."
  def control_subject, do: "gw.#{gateway_id()}.control"

  @doc "The channels service, for each user's pull-mode channels; nil = pull mode off here."
  def channels_url, do: Application.get_env(:xmsg_gateway, :channels_url)

  @doc "Where typing goes: the fan-out instance owning the channel's partition (plan §5.9)."
  def typing_subject(channel_id),
    do: "typing.#{XmsgGateway.Partition.partition(channel_id, partition_count())}"

  @doc "Read receipts (ADR-0012): to the fan-out owning the channel's partition, like typing."
  def receipts_subject(channel_id),
    do: "receipts.#{XmsgGateway.Partition.partition(channel_id, partition_count())}"

  @doc "Presence (plan §5.9): per-connection pings, and status queries."
  def presence_ping_subject, do: "xmsg.svc.presence.ping"
  def presence_query_subject, do: "xmsg.svc.presence.get"

  @doc "NATS request/reply subject of the messages service."
  def send_subject, do: "xmsg.svc.messages.send"
end
