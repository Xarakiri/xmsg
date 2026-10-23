# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Presence do
  @moduledoc """
  Presence (plan §5.9) at the gateway.

  - Each connection reports its state to the presence service (`ping/3`): on connect,
    on activity changes, every 30 s, and when it closes (the session tracker sends the
    last one, so a crashed socket still says goodbye).
  - A socket watches the users on its screen by registering them in the
    `XmsgGateway.PresenceWatchers` registry. This process listens to every status change
    on `presence.*` and forwards each to the sockets watching that user. (Per-user,
    refcounted NATS subscriptions come later; status changes are rare.)
  """
  use GenServer

  require Logger

  alias XmsgGateway.Config

  alias Xmsg.V1.{
    PresencePing,
    PresenceQuery,
    PresenceReply,
    PresenceUpdate,
    PresenceUpdates,
    ServerFrame
  }

  @updates "presence.*"
  @retry_ms 1_000

  def registry, do: XmsgGateway.PresenceWatchers
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Reports one connection's state: `:CONNECTION_STATE_ACTIVE`, `_IDLE` or `_GONE`."
  def ping(user_id, conn_id, state) do
    ping = %PresencePing{user_id: user_id, conn_id: conn_id, state: state}
    Config.bus().publish(Config.presence_ping_subject(), PresencePing.encode(ping))
  end

  @doc "Current statuses of `user_ids` (offline when the service doesn't answer)."
  def query(user_ids) do
    body = PresenceQuery.encode(%PresenceQuery{user_ids: user_ids})

    case Config.bus().request(Config.presence_query_subject(), body, Config.request_timeout_ms()) do
      {:ok, reply} ->
        PresenceReply.decode(reply).updates

      {:error, _} ->
        Enum.map(user_ids, &%PresenceUpdate{user_id: &1, status: :PRESENCE_STATUS_OFFLINE})
    end
  rescue
    _ -> []
  end

  @doc "Makes the calling socket watch exactly `user_ids` (replacing what it watched)."
  def watch(user_ids) do
    me = self()
    current = Registry.keys(registry(), me) |> MapSet.new()
    wanted = MapSet.new(user_ids)
    for gone <- MapSet.difference(current, wanted), do: Registry.unregister(registry(), gone)
    for new <- MapSet.difference(wanted, current), do: Registry.register(registry(), new, nil)
    :ok
  end

  @impl true
  def init(_opts) do
    send(self(), :subscribe)
    {:ok, %{monitor: nil}}
  end

  @impl true
  def handle_info(:subscribe, state) do
    case Config.bus().subscribe(@updates) do
      {:ok, conn} ->
        {:noreply, %{state | monitor: Process.monitor(conn)}}

      error ->
        Logger.warning("NATS subscribe to #{@updates} failed (#{inspect(error)}), retrying")
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

  def handle_info({:bus_msg, "presence." <> _, body}, state) do
    case safe_decode(body) do
      {:ok, %PresenceUpdate{user_id: user_id} = update} ->
        frame =
          ServerFrame.encode(%ServerFrame{
            frame: {:presence, %PresenceUpdates{updates: [update]}}
          })

        Registry.dispatch(registry(), user_id, fn entries ->
          for {pid, _} <- entries, do: send(pid, {:deliver, frame})
        end)

      _ ->
        :ok
    end

    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp safe_decode(body) do
    {:ok, PresenceUpdate.decode(body)}
  rescue
    _ -> :error
  end
end
