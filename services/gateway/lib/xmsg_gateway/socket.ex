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
    Welcome
  }

  @close_policy 1008

  @impl true
  def init(_opts) do
    Process.send_after(self(), :auth_timeout, Config.auth_timeout_ms())
    {:ok, %{user_id: nil}}
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

  def handle_info(:auth_timeout, %{user_id: nil} = state) do
    {:stop, :normal, {@close_policy, "authentication timeout"}, state}
  end

  def handle_info(_message, state), do: {:ok, state}

  # Before authentication only Connect is accepted.
  defp handle_frame({:connect, %Connect{access_token: token}}, %{user_id: nil} = state) do
    case XmsgGateway.Auth.verify(token) do
      {:ok, user_id} ->
        {:ok, _} = Registry.register(XmsgGateway.Users, user_id, nil)

        welcome = %Welcome{
          resume_token: Base.url_encode64(:crypto.strong_rand_bytes(18), padding: false),
          partition_count: Config.partition_count()
        }

        {:push, {:binary, encode(%ServerFrame{frame: {:welcome, welcome}})},
         %{state | user_id: user_id}}

      {:error, _} ->
        {:stop, :normal, {@close_policy, "invalid token"}, state}
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
      payload: {:send_message, %SendMessage{body: req.body}}
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

  defp handle_frame({:connect, _}, state) do
    {:stop, :normal, {@close_policy, "already connected"}, state}
  end

  defp handle_frame(_unknown, state), do: {:ok, state}

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
end
