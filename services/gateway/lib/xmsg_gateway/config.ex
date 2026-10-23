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

  @doc "Where typing goes: the fan-out instance owning the channel's partition (plan §5.9)."
  def typing_subject(channel_id),
    do: "typing.#{XmsgGateway.Partition.partition(channel_id, partition_count())}"

  @doc "NATS request/reply subject of the messages service."
  def send_subject, do: "xmsg.svc.messages.send"
end
