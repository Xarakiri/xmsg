# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Bus.NatsTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.Bus.Nats

  # No Gnat connection is running in tests, as while NATS is down: callers such as the
  # heartbeat must get an error, not an exit that crashes them (and, repeated, the gateway).
  test "publishing without a NATS connection returns an error instead of exiting" do
    assert {:error, _} = Nats.publish("gateway.alive.test", <<>>)
  end

  test "requesting without a NATS connection returns an error" do
    assert {:error, _} = Nats.request("some.subject", <<>>, 100)
  end

  # A connection process that exists but can't reach NATS: Gnat exits the caller with
  # :econnrefused. Subscribers (presence, pull mode, revocations) retry on an error; an
  # exit crashed them until the supervisor gave up and stopped the gateway (chaos test).
  describe "subscribing while the connection can't reach NATS" do
    setup do
      pid =
        spawn(fn ->
          receive do
            {:"$gen_call", _from, _request} -> exit(:econnrefused)
          end
        end)

      Process.register(pid, :xmsg_gnat)
      on_exit(fn -> Process.exit(pid, :kill) end)
      :ok
    end

    test "subscribe returns an error instead of exiting" do
      assert {:error, _} = Nats.subscribe("presence.*")
    end

    test "subscribe_ref returns an error instead of exiting" do
      assert {:error, _} = Nats.subscribe_ref("gw.test.control")
    end
  end
end
