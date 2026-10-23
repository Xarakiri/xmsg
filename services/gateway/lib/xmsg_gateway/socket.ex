# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Socket do
  @moduledoc """
  One process per WebSocket connection (plan §5.1). The first frame must be
  `Connect` with a valid token within the auth timeout. Server frames from
  fan-out arrive already encoded and are pushed unchanged (encode once).
  """
  @behaviour WebSock

  require Logger

  alias XmsgGateway.Config

  alias Xmsg.V1.{
    ClientFrame,
    Connect,
    InboundEvent,
    Ping,
    Pong,
    SendAck,
    SendMessage,
    SendMessageRequest,
    SendRejected,
    SendResult,
    ServerFrame,
    Typing,
    TypingIndicator,
    Activity,
    PresenceUpdates,
    WatchPresence,
    Welcome
  }

  @close_policy 1008
  # At most one typing notice per channel per connection in this many ms (plan §5.9).
  @typing_interval_ms 3_000
  # Presence pings at least this often; the service's TTL is 75 s (plan §5.9).
  @presence_every_ms 30_000
  @max_watched 200

  @impl true
  def init(_opts) do
    Process.send_after(self(), :auth_timeout, Config.auth_timeout_ms())
    {:ok, %{user_id: nil, conn_id: nil, session_id: 0, typing_at: %{}, active: true}}
  end

  @impl true
  def handle_in({data, opcode: :binary}, state) do
    case decode(data) do
      {:ok, %ClientFrame{frame: frame}} -> handle_frame(frame, state)
      :error -> {:stop, :normal, {@close_policy, "malformed frame"}, state}
    end
  end

  def handle_in({_data, opcode: :text}, state) do
    {:stop, :normal, {@close_policy, "binary protobuf frames only"}, state}
  end

  @impl true
  def handle_info({:deliver, frame}, %{user_id: id} = state) when not is_nil(id) do
    {:push, {:binary, frame}, state}
  end

  def handle_info(:session_revoked, state) do
    {:stop, :normal, {@close_policy, "session revoked"}, state}
  end

  def handle_info(:auth_timeout, %{user_id: nil} = state) do
    {:stop, :normal, {@close_policy, "authentication timeout"}, state}
  end

  # Presence heartbeat: the service counts a connection gone after its TTL (75 s).
  def handle_info(:presence_beat, %{user_id: id} = state) when not is_nil(id) do
    _ = XmsgGateway.Presence.ping(id, state.conn_id, connection_state(state.active))
    Process.send_after(self(), :presence_beat, @presence_every_ms)
    {:ok, state}
  end

  def handle_info(_message, state), do: {:ok, state}

  # Before authentication only Connect is accepted.
  defp handle_frame({:connect, %Connect{access_token: token}}, %{user_id: nil} = state) do
    with {:ok, %{user_id: user_id, session_id: sid}} <- XmsgGateway.Auth.authenticate(token),
         false <- XmsgGateway.Revocations.revoked?(sid),
         {:ok, _} <- register_session(sid),
         # Checked again once registered: a revocation that landed in between is not missed.
         false <- XmsgGateway.Revocations.revoked?(sid) do
      {:ok, _} = Registry.register(XmsgGateway.Users, user_id, nil)
      conn_id = XmsgGateway.Sessions.new_conn_id()
      XmsgGateway.Sessions.Tracker.track(conn_id, user_id)
      _ = XmsgGateway.Presence.ping(user_id, conn_id, :CONNECTION_STATE_ACTIVE)
      Process.send_after(self(), :presence_beat, @presence_every_ms)

      welcome = %Welcome{
        resume_token: Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false),
        partition_count: Config.partition_count()
      }

      {:push, {:binary, encode(%ServerFrame{frame: {:welcome, welcome}})},
       %{state | user_id: user_id, session_id: sid, conn_id: conn_id}}
    else
      {:error, _} -> {:stop, :normal, {@close_policy, "invalid token"}, state}
      true -> {:stop, :normal, {@close_policy, "session revoked"}, state}
    end
  end

  defp handle_frame(_frame, %{user_id: nil} = state) do
    {:stop, :normal, {@close_policy, "connect first"}, state}
  end

  defp handle_frame({:ping, %Ping{sent_at_ms: t}}, state) do
    {:push, {:binary, encode(%ServerFrame{frame: {:pong, %Pong{sent_at_ms: t}}})}, state}
  end

  defp handle_frame({:send_message, %SendMessageRequest{} = req}, state) do
    event = %InboundEvent{
      channel_id: req.channel_id,
      actor_id: state.user_id,
      dedupe_id: req.client_msg_id,
      payload: {:send_message, %SendMessage{body: req.body, thread_root: req.thread_root}}
    }

    bus = Config.bus()

    case bus.request(
           Config.send_subject(),
           InboundEvent.encode(event),
           Config.request_timeout_ms()
         ) do
      {:ok, reply} ->
        case decode_result(reply) do
          %SendResult{accepted: true} ->
            ack = %ServerFrame{frame: {:ack, %SendAck{client_msg_id: req.client_msg_id}}}
            {:push, {:binary, encode(ack)}, state}

          # Temporarily unavailable: stay silent so the client retries from its outbox (ADR-0011).
          %SendResult{reason: :REJECT_REASON_UNAVAILABLE} ->
            {:ok, state}

          %SendResult{reason: reason, detail: detail} ->
            rejected = %SendRejected{
              client_msg_id: req.client_msg_id,
              reason: reason,
              detail: detail
            }

            {:push, {:binary, encode(%ServerFrame{frame: {:send_rejected, rejected}})}, state}
        end

      {:error, reason} ->
        # No ack: the client keeps the message in its outbox and retries (ADR-0011).
        Logger.debug("send failed for user #{state.user_id}: #{inspect(reason)}")
        {:ok, state}
    end
  end

  # Typing is best-effort (plan §5.9): throttled per channel, published without waiting,
  # and the fan-out drops it unless the user is a member of the channel.
  defp handle_frame({:typing, %Typing{channel_id: channel_id}}, state) when channel_id > 0 do
    now = System.monotonic_time(:millisecond)

    case state.typing_at do
      %{^channel_id => last} when now - last < @typing_interval_ms ->
        {:ok, state}

      typing_at ->
        notice = %TypingIndicator{channel_id: channel_id, user_id: state.user_id}

        _ =
          Config.bus().publish(Config.typing_subject(channel_id), TypingIndicator.encode(notice))

        {:ok, %{state | typing_at: prune(Map.put(typing_at, channel_id, now), now)}}
    end
  end

  defp handle_frame({:activity, %Activity{active: active}}, state) do
    if active != state.active do
      _ = XmsgGateway.Presence.ping(state.user_id, state.conn_id, connection_state(active))
    end

    {:ok, %{state | active: active}}
  end

  # The users this client shows now: watched from here on, current statuses at once.
  defp handle_frame({:watch_presence, %WatchPresence{user_ids: ids}}, state) do
    ids = ids |> Enum.filter(&(&1 > 0)) |> Enum.uniq() |> Enum.take(@max_watched)
    :ok = XmsgGateway.Presence.watch(ids)

    case ids do
      [] ->
        {:ok, state}

      _ ->
        updates = %PresenceUpdates{updates: XmsgGateway.Presence.query(ids)}
        {:push, {:binary, encode(%ServerFrame{frame: {:presence, updates}})}, state}
    end
  end

  defp handle_frame({:connect, _}, state) do
    {:stop, :normal, {@close_policy, "already connected"}, state}
  end

  defp handle_frame(_unknown, state), do: {:ok, state}

  # Forget channels not typed in for a while, so the map stays small.
  defp prune(typing_at, now) when map_size(typing_at) > 32,
    do: Map.filter(typing_at, fn {_, at} -> now - at < @typing_interval_ms end)

  defp prune(typing_at, _now), do: typing_at

  defp connection_state(true), do: :CONNECTION_STATE_ACTIVE
  defp connection_state(false), do: :CONNECTION_STATE_IDLE

  defp decode_result(reply) do
    SendResult.decode(reply)
  rescue
    _ -> %SendResult{accepted: false, reason: :REJECT_REASON_UNAVAILABLE}
  end

  defp decode(data) do
    {:ok, ClientFrame.decode(data)}
  rescue
    _ -> :error
  end

  defp encode(frame), do: ServerFrame.encode(frame)

  # Dev tokens (session 0) are never revoked, so they aren't tracked.
  defp register_session(0), do: {:ok, nil}
  defp register_session(sid), do: Registry.register(XmsgGateway.Revocations.registry(), sid, nil)
end
