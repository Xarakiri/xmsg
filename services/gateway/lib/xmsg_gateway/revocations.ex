# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Revocations do
  @moduledoc """
  Revoked sessions (plan §5.1, §6.4). The auth service publishes `SessionRevoked` on NATS
  `auth.revocations` after a logout or a detected token theft (fast path; meta-relay
  repeats it from the outbox). The gateway then:

  - closes every socket of that session (`sid`) at once, and
  - refuses the session's access tokens, which stay cryptographically valid until they
    expire, until `refuse_until_ms`.

  The deny set lives in a public ETS table read by every connect; entries are dropped
  once their tokens have expired. A revocation that arrives twice changes nothing.
  After a (re)start the set is rebuilt from Kafka `auth.events`
  (`XmsgGateway.Revocations.Replay`) when `revocations_replay` is on.
  """
  use GenServer
  require Logger

  alias XmsgGateway.Config
  alias Xmsg.V1.{AuthEvent, SessionRevoked}

  @table :xmsg_revoked_sessions
  @registry XmsgGateway.SessionSockets
  @subject "auth.revocations"
  @retry_ms 1_000
  @purge_ms 60_000

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def registry, do: @registry

  @doc "Whether `session_id`'s tokens are refused right now. Session 0 (dev) never is."
  @spec revoked?(integer()) :: boolean()
  def revoked?(0), do: false

  def revoked?(session_id) do
    case :ets.lookup(@table, session_id) do
      [{_, until_ms}] -> System.os_time(:millisecond) < until_ms
      [] -> false
    end
  end

  @doc "Applies a revocation (also used by tests in place of NATS)."
  def apply(%SessionRevoked{session_id: sid, refuse_until_ms: until}) when sid != 0 do
    # Keep the later deadline if the same session is revoked twice.
    case :ets.lookup(@table, sid) do
      [{_, existing}] when existing >= until -> :ok
      _ -> :ets.insert(@table, {sid, until})
    end

    Registry.dispatch(@registry, sid, fn sockets ->
      for {pid, _} <- sockets, do: send(pid, :session_revoked)
    end)

    :ok
  end

  def apply(_), do: :ok

  @impl true
  def init(_opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])
    send(self(), :subscribe)
    Process.send_after(self(), :purge, @purge_ms)
    {:ok, %{monitor: nil}, {:continue, :replay}}
  end

  @impl true
  def handle_continue(:replay, state) do
    if Config.revocations_replay?() do
      # Subscribed first (the :subscribe message is already queued ahead of nothing that
      # could be lost: NATS delivers to us from now on), then the past is read back.
      case __MODULE__.Replay.run(Config.kafka_endpoints(), &apply/1) do
        {:ok, n} -> Logger.info("revocations replayed: #{n} still live")
        {:error, reason} -> Logger.warning("revocation replay failed: #{inspect(reason)}")
      end
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:subscribe, state) do
    case Config.bus().subscribe(@subject) do
      {:ok, conn} ->
        {:noreply, %{state | monitor: Process.monitor(conn)}}

      error ->
        Logger.warning("NATS subscribe to #{@subject} failed (#{inspect(error)}), retrying")
        Process.send_after(self(), :subscribe, @retry_ms)
        {:noreply, state}
    end
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{monitor: ref} = state) do
    Process.send_after(self(), :subscribe, @retry_ms)
    {:noreply, %{state | monitor: nil}}
  end

  def handle_info({:msg, _} = msg, state),
    do: handle_info(XmsgGateway.Bus.Nats.normalize(msg), state)

  def handle_info({:bus_msg, @subject, body}, state) do
    case safe_decode(body) do
      {:ok, %AuthEvent{event: {:session_revoked, revoked}}} ->
        apply(revoked)

      {:ok, _other} ->
        :ok

      :error ->
        Logger.warning("undecodable message on #{@subject}")
    end

    {:noreply, state}
  end

  def handle_info(:purge, state) do
    now = System.os_time(:millisecond)
    :ets.select_delete(@table, [{{:_, :"$1"}, [{:<, :"$1", now}], [true]}])
    Process.send_after(self(), :purge, @purge_ms)
    {:noreply, state}
  end

  def handle_info(_other, state), do: {:noreply, state}

  defp safe_decode(body) do
    {:ok, AuthEvent.decode(body)}
  rescue
    _ -> :error
  end
end
