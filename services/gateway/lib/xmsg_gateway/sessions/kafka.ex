# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Sessions.Kafka do
  @moduledoc "Session events to Kafka through brod (async, acks=all)."
  @behaviour XmsgGateway.Sessions

  require Logger

  alias XmsgGateway.Config
  alias Xmsg.V1.SessionEvent

  @client :xmsg_brod

  @impl true
  def child_specs do
    endpoints =
      Config.kafka_bootstrap()
      |> String.split(",", trim: true)
      |> Enum.map(fn hostport ->
        [host, port] = String.split(hostport, ":")
        {String.to_charlist(host), String.to_integer(port)}
      end)

    client_config = [
      auto_start_producers: true,
      reconnect_cool_down_seconds: 2,
      default_producer_config: [required_acks: -1]
    ]

    [%{id: @client, start: {:brod, :start_link_client, [endpoints, @client, client_config]}}]
  end

  @impl true
  def connected(conn_id, user_id) do
    event = %SessionEvent{
      conn_id: conn_id,
      user_id: user_id,
      gateway_id: Config.gateway_id(),
      app_state: :APP_STATE_FOREGROUND,
      connected_at_ms: System.system_time(:millisecond)
    }

    produce(conn_id, SessionEvent.encode(event))
  end

  # A tombstone: compaction then drops the connection's key entirely.
  @impl true
  def disconnected(conn_id), do: produce(conn_id, :undefined)

  # produce_no_ack: the producer still waits for acks=all, but no reply message is
  # sent back to the calling process (a socket or the session tracker).
  defp produce(key, value) do
    case :brod.produce_no_ack(@client, Config.sessions_topic(), :hash, key, value) do
      :ok ->
        :ok

      {:error, reason} ->
        # Fan-out also drops a silent gateway's sessions after its heartbeat stops (plan §5.8).
        Logger.warning("session event for #{key} not published: #{inspect(reason)}")
        :ok
    end
  end
end
