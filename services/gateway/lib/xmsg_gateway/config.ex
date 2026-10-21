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
  def bus, do: get(:bus)
  def nats, do: get(:nats)

  @doc "NATS subject carrying fan-out batches for this gateway (ADR-0009)."
  def delivery_subject, do: "gw.#{gateway_id()}"
  def stream_heads_subject, do: "stream_heads.#{gateway_id()}"

  @doc "NATS request/reply subject of the messages service."
  def send_subject, do: "xmsg.svc.messages.send"
end
