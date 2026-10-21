# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Delivery do
  @moduledoc """
  Receives fan-out batches for this gateway and hands each pre-encoded frame
  to the local sockets of its recipients (plan §5.1, ADR-0009).

  NATS core is at-most-once, so this process also detects lost deliveries:
  - each stream `(partition, owner_epoch)` must arrive with consecutive `seq`;
  - `StreamHeads` (every second from fan-out) reveal a lost *last* batch;
  - a new `owner_epoch` resets tracking instead of counting as a gap;
  - losing the NATS subscription (reconnect) means anything could be lost.

  On a gap, clients get `Resync{partition}` and sync only that partition's
  channels; after a reconnect, everyone gets a full `Resync`.
  """
  use GenServer

  require Logger

  alias XmsgGateway.Config
  alias Xmsg.V1.{GatewayBatch, Resync, ServerFrame, StreamHeads}

  @retry_ms 1_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Current stream positions, for tests and diagnostics."
  def streams, do: GenServer.call(__MODULE__, :streams)

  @impl true
  def init(_opts) do
    send(self(), :subscribe)
    {:ok, %{streams: %{}, monitor: nil, subscribed_once?: false}}
  end

  @impl true
  def handle_call(:streams, _from, state), do: {:reply, state.streams, state}

  @impl true
  def handle_info(:subscribe, state) do
    bus = Config.bus()

    with {:ok, conn} <- bus.subscribe(Config.delivery_subject()),
         {:ok, ^conn} <- bus.subscribe(Config.stream_heads_subject()) do
      # A resubscription means NATS reconnected: anything may have been lost.
      if state.subscribed_once?, do: broadcast_resync(nil)
      {:noreply, %{state | monitor: Process.monitor(conn), subscribed_once?: true, streams: %{}}}
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
        {:noreply, handle_batch(GatewayBatch.decode(body), state)}

      subject == Config.stream_heads_subject() ->
        {:noreply, handle_heads(StreamHeads.decode(body), state)}

      true ->
        {:noreply, state}
    end
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp handle_batch(%GatewayBatch{} = batch, state) do
    state = track(state, batch.partition, batch.owner_epoch, batch.seq, :batch)

    for delivery <- batch.deliveries, user_id <- Enum.uniq(delivery.user_ids) do
      # The same binary goes to every socket: encoded once by fan-out.
      Registry.dispatch(XmsgGateway.Users, user_id, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:deliver, delivery.server_frame})
      end)
    end

    state
  end

  defp handle_heads(%StreamHeads{heads: heads}, state) do
    Enum.reduce(heads, state, fn head, acc ->
      track(acc, head.partition, head.owner_epoch, head.seq, :head)
    end)
  end

  # Gap rules (ADR-0009). Returns the state with the stream's new position.
  defp track(state, partition, epoch, seq, kind) do
    case Map.get(state.streams, partition) do
      {^epoch, last} when kind == :batch and seq != last + 1 and seq > last ->
        gap(partition, last, seq)
        put_stream(state, partition, epoch, seq)

      {^epoch, last} when kind == :head and seq > last ->
        gap(partition, last, seq)
        put_stream(state, partition, epoch, seq)

      {^epoch, last} when seq <= last ->
        state

      _ ->
        # First sight of this stream, or a new owner epoch: start tracking from here.
        put_stream(state, partition, epoch, seq)
    end
  end

  defp put_stream(state, partition, epoch, seq),
    do: put_in(state.streams[partition], {epoch, seq})

  defp gap(partition, last, seq) do
    Logger.info("delivery gap on partition #{partition}: had #{last}, saw #{seq}; resyncing")
    broadcast_resync(partition)
  end

  defp broadcast_resync(partition) do
    frame = ServerFrame.encode(%ServerFrame{frame: {:resync, %Resync{partition: partition}}})

    XmsgGateway.Users
    |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
    |> Enum.uniq()
    |> Enum.each(&send(&1, {:deliver, frame}))
  end
end
