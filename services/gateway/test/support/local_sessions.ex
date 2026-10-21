# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Sessions.Local do
  @moduledoc "Records session events in memory for tests."
  @behaviour XmsgGateway.Sessions

  @impl true
  def child_specs,
    do: [%{id: __MODULE__, start: {Agent, :start_link, [fn -> [] end, [name: __MODULE__]]}}]

  @impl true
  def connected(conn_id, user_id),
    do: Agent.update(__MODULE__, &[{:connected, conn_id, user_id} | &1])

  @impl true
  def disconnected(conn_id), do: Agent.update(__MODULE__, &[{:disconnected, conn_id} | &1])

  def events, do: Agent.get(__MODULE__, &Enum.reverse/1)
end
