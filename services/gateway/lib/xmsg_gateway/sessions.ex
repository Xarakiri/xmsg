# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Sessions do
  @moduledoc """
  Publishes who is connected where (plan §5.1): one `SessionEvent` per
  authenticated connection on Kafka `sessions.events` (compacted, key =
  conn_id), and a tombstone when it closes. Fan-out rebuilds its session map
  from this topic. `XmsgGateway.Sessions.Kafka` is the real implementation;
  tests use `XmsgGateway.Sessions.Local`.
  """

  @callback child_specs() :: [Supervisor.child_spec() | {module(), term()} | module()]
  @callback connected(conn_id :: String.t(), user_id :: integer()) :: :ok
  @callback disconnected(conn_id :: String.t()) :: :ok

  def impl, do: XmsgGateway.Config.sessions()

  def new_conn_id, do: Base.encode16(:crypto.strong_rand_bytes(12), case: :lower)
end
