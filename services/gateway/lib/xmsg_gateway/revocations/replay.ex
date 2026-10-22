# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Revocations.Replay do
  @moduledoc """
  Rebuilds the revocation deny set after a (re)start from Kafka `auth.events`
  (the outbox copy of every revocation, 7-day retention). Only the last
  `@window_ms` matter: anything older has `refuse_until_ms` in the past, because
  no access token outlives 15 minutes plus leeway.

  Without this, a restarted gateway would let a session revoked minutes ago
  reconnect with its still-valid access token.
  """
  require Logger
  require Record

  Record.defrecordp(
    :kafka_message,
    Record.extract(:kafka_message, from_lib: "kafka_protocol/include/kpro_public.hrl")
  )

  alias Xmsg.V1.{AuthEvent, SessionRevoked}

  @topic "auth.events"
  # Longer than the access-token lifetime (15 min) plus verify leeway.
  @window_ms 16 * 60_000

  @doc "Applies every still-live revocation with `apply_fun`; returns how many were applied."
  @spec run([{charlist(), non_neg_integer()}], (SessionRevoked.t() -> any())) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def run(endpoints, apply_fun) do
    since = System.os_time(:millisecond) - @window_ms

    with {:ok, partitions} <- partitions(endpoints) do
      applied =
        Enum.reduce(0..(partitions - 1), 0, fn p, acc ->
          acc + replay_partition(endpoints, p, since, apply_fun)
        end)

      {:ok, applied}
    end
  end

  defp partitions(endpoints) do
    with {:ok, %{topics: topics}} <- :brod.get_metadata(endpoints, [@topic]),
         %{partitions: [_ | _] = ps} <- Enum.find(topics, &(&1[:name] == @topic)) do
      {:ok, length(ps)}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :no_such_topic}
    end
  end

  defp replay_partition(endpoints, partition, since, apply_fun) do
    case :brod.resolve_offset(endpoints, @topic, partition, since) do
      # -1: no record at or after `since` in this partition.
      {:ok, offset} when offset >= 0 -> fetch_from(endpoints, partition, offset, apply_fun, 0)
      {:ok, _} -> 0
      {:error, reason} -> warn(partition, reason)
    end
  end

  defp fetch_from(endpoints, partition, offset, apply_fun, applied) do
    case :brod.fetch(endpoints, @topic, partition, offset, %{max_wait_time: 100}) do
      {:ok, {_hw, []}} ->
        applied

      {:ok, {hw, messages}} ->
        now = System.os_time(:millisecond)
        applied = applied + Enum.count(messages, &apply_message(&1, now, apply_fun))
        next = kafka_message(List.last(messages), :offset) + 1

        if next < hw,
          do: fetch_from(endpoints, partition, next, apply_fun, applied),
          else: applied

      {:error, reason} ->
        warn(partition, reason) + applied
    end
  end

  defp apply_message(message, now, apply_fun) do
    with value when is_binary(value) <- kafka_message(message, :value),
         {:ok, %AuthEvent{event: {:session_revoked, %SessionRevoked{} = r}}} <- decode(value),
         true <- r.refuse_until_ms > now do
      apply_fun.(r)
      true
    else
      _ -> false
    end
  end

  defp decode(value) do
    {:ok, AuthEvent.decode(value)}
  rescue
    _ -> :error
  end

  defp warn(partition, reason) do
    Logger.warning("revocation replay of #{@topic}/#{partition} failed: #{inspect(reason)}")
    0
  end
end
