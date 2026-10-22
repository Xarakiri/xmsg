# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Bus.NatsTest do
  use ExUnit.Case, async: true

  alias XmsgGateway.Bus.Nats

  # No Gnat connection is running in tests, as while NATS is down: callers such as the
  # heartbeat must get an error, not an exit that crashes them (and, repeated, the gateway).
  test "publishing without a NATS connection returns an error instead of exiting" do
    assert {:error, _} = Nats.publish("gateway.alive.test", <<>>)
  end

  test "requesting without a NATS connection returns an error" do
    assert {:error, _} = Nats.request("some.subject", <<>>, 100)
  end
end
