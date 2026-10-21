# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Sessions.Tracker do
  @moduledoc """
  Monitors each authenticated socket process and publishes the session
  tombstone when it goes down, however it ends: a clean close, an abrupt
  disconnect (which skips `WebSock.terminate/2`), or a crash.
  """
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc "Publishes the session and watches `pid` (the calling socket by default)."
  def track(conn_id, user_id, pid \\ self()) do
    :ok = XmsgGateway.Sessions.impl().connected(conn_id, user_id)
    GenServer.cast(__MODULE__, {:track, conn_id, pid})
  end

  @impl true
  def init(_opts), do: {:ok, %{}}

  @impl true
  def handle_cast({:track, conn_id, pid}, refs) do
    {:noreply, Map.put(refs, Process.monitor(pid), conn_id)}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, refs) do
    {conn_id, refs} = Map.pop(refs, ref)
    if conn_id, do: XmsgGateway.Sessions.impl().disconnected(conn_id)
    {:noreply, refs}
  end

  def handle_info(_other, refs), do: {:noreply, refs}
end
