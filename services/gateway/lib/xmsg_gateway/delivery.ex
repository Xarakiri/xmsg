# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Delivery do
  @moduledoc """
  Receives fan-out batches for this gateway and routes them to delivery
  workers (plan §5.1, ADR-0009).

  One process can't dispatch more than ~100k frames/s (Phase 0 loadgen
  measurement), so batches are routed by stream partition to a fixed pool of
  `XmsgGateway.Delivery.Worker` processes. A partition always goes to the same
  worker, which keeps per-stream ordering and gap tracking correct.

  This process owns the NATS subscription: losing it (a reconnect) means
  anything could have been lost, so every local client gets a full `Resync`.
  """
  use GenServer

  require Logger

  alias XmsgGateway.Config
  alias XmsgGateway.Delivery.Worker
  alias Xmsg.V1.{GatewayBatch, StreamHeads}

  @retry_ms 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Number of delivery workers (one per scheduler by default)."
  def worker_count,
    do: Application.get_env(:xmsg_gateway, :delivery_workers, System.schedulers_online())

  @doc "Current stream positions across all workers, for tests and diagnostics."
  def streams do
    0..(worker_count() - 1)
    |> Enum.map(&Worker.streams/1)
    |> Enum.reduce(%{}, &Map.merge/2)
  end

  @impl true
  def init(_opts) do
    send(self(), :subscribe)
    {:ok, %{monitor: nil, subscribed_once?: false, workers: worker_count()}}
  end

  @impl true
  def handle_info(:subscribe, state) do
    bus = Config.bus()

    with {:ok, conn} <- bus.subscribe(Config.delivery_subject()),
         {:ok, ^conn} <- bus.subscribe(Config.stream_heads_subject()) do
      if state.subscribed_once? do
        for i <- 0..(state.workers - 1), do: Worker.reset(i)
        Worker.broadcast_resync(nil)
      end

      {:noreply, %{state | monitor: Process.monitor(conn), subscribed_once?: true}}
    else
      error ->
        Logger.warning("NATS subscribe failed (#{inspect(error)}), retrying")
        Process.send_after(self(), :subscribe, @retry_ms)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor: ref} = state) do
    Process.send_after(self(), :subscribe, @retry_ms)
    {:noreply, %{state | monitor: nil}}
  end

  def handle_info({:msg, _} = msg, state),
    do: handle_info(XmsgGateway.Bus.Nats.normalize(msg), state)

  def handle_info({:bus_msg, subject, body}, state) do
    cond do
      subject == Config.delivery_subject() ->
        batch = GatewayBatch.decode(body)
        Worker.batch(rem(batch.partition, state.workers), batch)

      subject == Config.stream_heads_subject() ->
        %StreamHeads{heads: heads} = StreamHeads.decode(body)

        heads
        |> Enum.group_by(&rem(&1.partition, state.workers))
        |> Enum.each(fn {worker, worker_heads} -> Worker.heads(worker, worker_heads) end)

      true ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}
end
