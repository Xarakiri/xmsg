# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Pull do
  @moduledoc """
  Pull-mode channels (ADR-0008): large channels aren't pushed to each member's gateway;
  fan-out publishes each event once on `ch.{channel_id}`, and every gateway with a local
  member subscribes and delivers it to them.

  - **Who's local:** sockets register in `members_registry/0` under the pull-mode channels
    of their user. At connect a socket fetches that list from the channels service
    (`GET /v1/me/channels?mode=pull` with the user's own token), and once more a few
    seconds later, in case a join raced the first read.
  - **Changes after that** come from fan-out on `gw.{gateway_id}.control` as
    `PullControl` (joined, left, or the channel switched modes), passed on to the user's
    sockets here.
  - **Workspace removals:** a registration remembers its channel's workspace. When the
    user is removed from it (`workspace.member_removed` on `auth.revocations`), their
    sockets drop that workspace's channels at once and refuse them from then on, before
    the removal job has taken them out of each channel (plan §6.5).
  - **Subscriptions:** this process subscribes to `ch.{id}` when the first local member
    registers, and drops subscriptions without members every `@sweep_ms`. After a NATS
    reconnect it subscribes everything again.

  Sockets get full events only for the channel their client has open (`ViewChannel`), and
  membership changes always; for the others, at most one `ChannelBump` per channel per
  5 s (plan §6.7).

  Frames on `ch.{id}` carry no stream sequence: clients notice a gap by the channel's
  `event_seq` and resync (ADR-0011), and drop duplicates the same way (both paths are
  used for a while around a mode switch).
  """
  use GenServer

  require Logger

  alias XmsgGateway.Config
  alias Xmsg.V1.{PullControl, SequencedEvent, ServerFrame}

  @membership [
    :channel_created,
    :member_joined,
    :member_left,
    :member_role_changed,
    :channel_mode_changed
  ]

  @sweep_ms 30_000
  @retry_ms 1_000
  @refetch_ms 5_000

  def members_registry, do: XmsgGateway.PullMembers

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Channels this gateway is subscribed to (tests, diagnostics)."
  def subscribed, do: GenServer.call(__MODULE__, :subscribed)

  @doc """
  Called by a socket: deliver `channel_id` (of `workspace_id`) pull-mode events to it
  (idempotent per socket).
  """
  def join(channel_id, workspace_id) do
    if Registry.lookup(members_registry(), channel_id)
       |> Enum.all?(fn {pid, _} -> pid != self() end) do
      {:ok, _} = Registry.register(members_registry(), channel_id, workspace_id)
    end

    GenServer.cast(__MODULE__, {:ensure, channel_id})
  end

  @doc "Called by a socket: stop delivering `channel_id` to it."
  def leave(channel_id), do: Registry.unregister(members_registry(), channel_id)

  @doc """
  Fetches the user's pull-mode channels for `socket` (now, and again after a few
  seconds); the socket gets `{:pull_channels, [{channel_id, workspace_id}]}`. Does nothing without
  `XMSG_CHANNELS_URL`.
  """
  def fetch_for(socket, token) do
    case Config.channels_url() do
      nil ->
        :ok

      url ->
        Task.start(fn ->
          send_channels(socket, url, token)
          Process.sleep(@refetch_ms)
          if Process.alive?(socket), do: send_channels(socket, url, token)
        end)

        :ok
    end
  end

  defp send_channels(socket, url, token) do
    case pull_channels(url, token) do
      {:ok, channels} -> send(socket, {:pull_channels, channels})
      {:error, reason} -> Logger.warning("pull-mode channels unavailable: #{inspect(reason)}")
    end
  end

  @doc false
  def pull_channels(url, token) do
    request =
      {String.to_charlist(String.trim_trailing(url, "/") <> "/v1/me/channels?mode=pull"),
       [{~c"authorization", String.to_charlist("Bearer " <> token)}]}

    with {:ok, {{_, 200, _}, _headers, body}} <-
           :httpc.request(:get, request, [timeout: 3_000, connect_timeout: 2_000],
             body_format: :binary
           ),
         {:ok, %{"channels" => list}} when is_list(list) <- JSON.decode(body) do
      channels =
        Enum.map(list, fn %{"channel_id" => c, "workspace_id" => w} ->
          {String.to_integer(c), String.to_integer(w)}
        end)

      {:ok, channels}
    else
      {:ok, {{_, status, _}, _headers, _body}} -> {:error, {:status, status}}
      other -> {:error, other}
    end
  end

  # ---- the process ------------------------------------------------------------------

  @impl true
  def init(_opts) do
    send(self(), :connect)
    Process.send_after(self(), :sweep, @sweep_ms)
    {:ok, %{subs: %{}, monitor: nil}}
  end

  @impl true
  def handle_call(:subscribed, _from, state), do: {:reply, Map.keys(state.subs), state}

  @impl true
  def handle_cast({:ensure, channel_id}, %{subs: subs} = state) do
    if Map.has_key?(subs, channel_id) or state.monitor == nil do
      # Not connected: everything is subscribed on reconnect (members are registered).
      {:noreply, state}
    else
      {:noreply, subscribe(state, channel_id)}
    end
  end

  @impl true
  def handle_info(:connect, state) do
    case Config.bus().subscribe_ref(Config.control_subject()) do
      {:ok, conn, _ref} ->
        state = %{state | monitor: Process.monitor(conn), subs: %{}}
        # Everything local members need, after a (re)connect.
        channels =
          Registry.select(members_registry(), [{{:"$1", :_, :_}, [], [:"$1"]}]) |> Enum.uniq()

        {:noreply, Enum.reduce(channels, state, &subscribe(&2, &1))}

      error ->
        Logger.warning("pull-mode control subscription failed (#{inspect(error)}), retrying")
        Process.send_after(self(), :connect, @retry_ms)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor: ref} = state) do
    Process.send_after(self(), :connect, @retry_ms)
    {:noreply, %{state | monitor: nil, subs: %{}}}
  end

  def handle_info(:sweep, state) do
    Process.send_after(self(), :sweep, @sweep_ms)

    {gone, kept} =
      Enum.split_with(state.subs, fn {channel_id, _ref} ->
        Registry.lookup(members_registry(), channel_id) == []
      end)

    for {_channel_id, ref} <- gone, do: Config.bus().unsubscribe(ref)
    {:noreply, %{state | subs: Map.new(kept)}}
  end

  def handle_info({:msg, _} = msg, state),
    do: handle_info(XmsgGateway.Bus.Nats.normalize(msg), state)

  def handle_info({:bus_msg, subject, body}, state) do
    control = Config.control_subject()

    case subject do
      ^control ->
        relay(PullControl.decode(body))

      "ch." <> id ->
        message = pull_event(String.to_integer(id), body)

        Registry.dispatch(members_registry(), String.to_integer(id), fn entries ->
          for {pid, _} <- entries, do: send(pid, message)
        end)
    end

    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  # Decoded once per gateway: sockets need the positions for bumps, and whether the event
  # changes membership (those reach every member in full: their client must apply them).
  defp pull_event(channel_id, body) do
    case ServerFrame.decode(body) do
      %ServerFrame{frame: {:event, %SequencedEvent{} = e}} ->
        always = match?({kind, _} when kind in @membership, e.payload)
        {:pull_event, channel_id, body, e.event_seq, e.msg_seq, always}

      _ ->
        {:pull_event, channel_id, body, 0, 0, true}
    end
  rescue
    _ -> {:pull_event, channel_id, body, 0, 0, true}
  end

  defp subscribe(state, channel_id) do
    case Config.bus().subscribe_ref("ch.#{channel_id}") do
      {:ok, _conn, ref} ->
        %{state | subs: Map.put(state.subs, channel_id, ref)}

      error ->
        # The next :connect (or the next join) tries again.
        Logger.warning("subscribing to ch.#{channel_id} failed: #{inspect(error)}")
        state
    end
  end

  # Fan-out says a connected user joined or left a pull-mode channel (or it switched modes).
  defp relay(%PullControl{} = c) do
    for {pid, _} <- Registry.lookup(XmsgGateway.Users, c.user_id) do
      send(pid, {:pull, c.channel_id, c.workspace_id, c.subscribe})
    end
  end
end
