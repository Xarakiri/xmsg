# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Heartbeat do
  @moduledoc """
  Publishes `gateway.alive.{gateway_id}` on NATS every 2 s, so fan-out can tell
  a crashed gateway from a quiet one (plan §5.8).
  """
  use GenServer

  @every_ms 2_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    send(self(), :beat)
    {:ok, nil}
  end

  @impl true
  def handle_info(:beat, state) do
    _ = XmsgGateway.Config.bus().publish("gateway.alive.#{XmsgGateway.Config.gateway_id()}", <<>>)
    Process.send_after(self(), :beat, @every_ms)
    {:noreply, state}
  end
end
