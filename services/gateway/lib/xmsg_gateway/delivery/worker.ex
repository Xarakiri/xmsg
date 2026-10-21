# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Delivery.Worker do
  @moduledoc """
  Dispatches batches for the partitions routed to it and detects lost
  deliveries on those streams (ADR-0009):
  - each stream `(partition, owner_epoch)` must arrive with consecutive `seq`;
  - `StreamHead`s reveal a lost *last* batch;
  - a new `owner_epoch` resets tracking instead of counting as a gap.
  On a gap, local clients get `Resync{partition}`.
  """
  use GenServer

  require Logger

  alias Xmsg.V1.{GatewayBatch, Resync, ServerFrame}

  def child_spec(index) do
    %{
      id: {__MODULE__, index},
      start: {GenServer, :start_link, [__MODULE__, index, [name: name(index)]]}
    }
  end

  def name(index), do: :"#{__MODULE__}.#{index}"

  def batch(index, %GatewayBatch{} = batch), do: GenServer.cast(name(index), {:batch, batch})
  def heads(index, heads), do: GenServer.cast(name(index), {:heads, heads})
  def reset(index), do: GenServer.cast(name(index), :reset)
  def streams(index), do: GenServer.call(name(index), :streams)

  @doc "Sends `Resync{partition}` (nil = everything) to every local socket, encoded once."
  def broadcast_resync(partition) do
    frame = ServerFrame.encode(%ServerFrame{frame: {:resync, %Resync{partition: partition}}})

    XmsgGateway.Users
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.uniq()
    |> Enum.each(&send(&1, {:deliver, frame}))
  end

  @impl true
  def init(_index), do: {:ok, %{}}

  @impl true
  def handle_call(:streams, _from, streams), do: {:reply, streams, streams}

  @impl true
  def handle_cast({:batch, %GatewayBatch{} = batch}, streams) do
    streams = track(streams, batch.partition, batch.owner_epoch, batch.seq, :batch)

    for delivery <- batch.deliveries, user_id <- Enum.uniq(delivery.user_ids) do
      # The same binary goes to every socket: encoded once by fan-out.
      Registry.dispatch(XmsgGateway.Users, user_id, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:deliver, delivery.server_frame})
      end)
    end

    {:noreply, streams}
  end

  def handle_cast({:heads, heads}, streams) do
    {:noreply,
     Enum.reduce(heads, streams, &track(&2, &1.partition, &1.owner_epoch, &1.seq, :head))}
  end

  def handle_cast(:reset, _streams), do: {:noreply, %{}}

  defp track(streams, partition, epoch, seq, kind) do
    case Map.get(streams, partition) do
      {^epoch, last} when kind == :batch and seq > last + 1 ->
        gap(partition, last, seq)
        Map.put(streams, partition, {epoch, seq})

      {^epoch, last} when kind == :head and seq > last ->
        gap(partition, last, seq)
        Map.put(streams, partition, {epoch, seq})

      {^epoch, last} when seq <= last ->
        streams

      _ ->
        # First sight of this stream, or a new owner epoch: start tracking from here.
        Map.put(streams, partition, {epoch, seq})
    end
  end

  defp gap(partition, last, seq) do
    Logger.info("delivery gap on partition #{partition}: had #{last}, saw #{seq}; resyncing")
    broadcast_resync(partition)
  end
end
