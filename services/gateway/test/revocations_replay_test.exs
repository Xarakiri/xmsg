# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.RevocationsReplayTest do
  # Needs a real Kafka with the `auth.events` topic:
  #   XMSG_IT_KAFKA=127.0.0.1:9092 mix test --only kafka
  use ExUnit.Case, async: false

  @moduletag :kafka

  alias XmsgGateway.Revocations.Replay
  alias Xmsg.V1.{AuthEvent, SessionRevoked}

  setup_all do
    endpoints =
      System.get_env("XMSG_IT_KAFKA", "127.0.0.1:9092")
      |> String.split(",")
      |> Enum.map(fn hp ->
        [h, p] = String.split(hp, ":")
        {String.to_charlist(h), String.to_integer(p)}
      end)

    :ok = :brod.start_client(endpoints, :replay_it, auto_start_producers: true)
    on_exit(fn -> :brod.stop_client(:replay_it) end)
    %{endpoints: endpoints}
  end

  defp publish(sid, refuse_until_ms) do
    event = %AuthEvent{
      event:
        {:session_revoked,
         %SessionRevoked{
           user_id: 1,
           session_id: sid,
           refuse_until_ms: refuse_until_ms,
           reason: "it"
         }}
    }

    {:ok, _} =
      :brod.produce_sync_offset(
        :replay_it,
        "auth.events",
        :hash,
        Integer.to_string(sid),
        AuthEvent.encode(event)
      )
  end

  test "a restart replays revocations that are still live, and only those", %{
    endpoints: endpoints
  } do
    base = System.unique_integer([:positive]) * 10 + 5_000_000_000
    {live, expired} = {base + 1, base + 2}
    now = System.os_time(:millisecond)
    publish(live, now + 600_000)
    publish(expired, now - 1)

    me = self()
    assert {:ok, n} = Replay.run(endpoints, &send(me, {:replayed, &1.session_id}))
    assert n >= 1
    assert_received {:replayed, ^live}
    refute_received {:replayed, ^expired}
  end
end
