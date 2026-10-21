# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Bus do
  @moduledoc """
  The gateway's view of NATS: subscriptions that deliver
  `{:bus_msg, subject, body}` to the subscriber, and request/reply.
  `XmsgGateway.Bus.Nats` is the real one; tests use `XmsgGateway.Bus.Local`.
  """

  @doc "Children to start under the application supervisor."
  @callback child_specs() :: [Supervisor.child_spec() | {module(), term()} | module()]

  @doc """
  Subscribes the calling process to `subject`. Returns a pid the caller can
  monitor: when it goes down, the subscription is gone (for NATS, a reconnect).
  """
  @callback subscribe(subject :: String.t()) :: {:ok, pid()} | {:error, term()}

  @callback request(subject :: String.t(), body :: binary(), timeout_ms :: pos_integer()) ::
              {:ok, binary()} | {:error, term()}

  @callback publish(subject :: String.t(), body :: binary()) :: :ok | {:error, term()}
end
