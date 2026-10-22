# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Bus.Nats do
  @moduledoc "NATS core through Gnat (at-most-once; gaps are detected by `XmsgGateway.Delivery`)."
  @behaviour XmsgGateway.Bus

  @conn :xmsg_gnat

  @impl true
  def child_specs do
    nats = XmsgGateway.Config.nats()

    [
      {Gnat.ConnectionSupervisor,
       %{
         name: @conn,
         backoff_period: 1_000,
         connection_settings: [%{host: nats.host, port: nats.port}]
       }}
    ]
  end

  @impl true
  def subscribe(subject) do
    case Process.whereis(@conn) do
      nil ->
        {:error, :not_connected}

      pid ->
        case Gnat.sub(pid, self(), subject) do
          {:ok, _sid} -> {:ok, pid}
          error -> error
        end
    end
  end

  @impl true
  def request(subject, body, timeout_ms) do
    case Gnat.request(@conn, subject, body, receive_timeout: timeout_ms) do
      {:ok, %{body: reply}} -> {:ok, reply}
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  # While NATS is away the connection process doesn't exist and Gnat.pub exits; callers
  # (the heartbeat) get an error instead of crashing, or repeated crashes would take the
  # whole gateway down with every socket on it.
  @impl true
  def publish(subject, body) do
    Gnat.pub(@conn, subject, body)
  catch
    :exit, reason -> {:error, reason}
  end

  @doc "Gnat delivers `{:msg, map}`; normalize it to the bus message format."
  def normalize({:msg, %{topic: subject, body: body}}), do: {:bus_msg, subject, body}
  def normalize(other), do: other
end
