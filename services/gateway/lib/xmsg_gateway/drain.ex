# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Drain do
  @moduledoc """
  Draining on shutdown (plan §5.1): a deploy shouldn't send every client of a pod to the
  others at the same instant. On SIGTERM the gateway

  1. stops taking connections: `/healthz` answers 503 (the readiness probe takes the pod
     out of its Service) and upgrades are refused with `Retry-After`;
  2. closes its connections spread at random over `XMSG_DRAIN_SECONDS` (default 300),
     each with `1012 service restart`, so clients reconnect elsewhere a few at a time
     and resume through sync (ADR-0011);
  3. returns once no connection is left (or the window plus a few seconds has passed),
     and the application stops.

  It runs in `XmsgGateway.Application.prep_stop/1`, while the HTTP server still serves.
  """

  require Logger

  @key {__MODULE__, :draining}

  def draining?, do: :persistent_term.get(@key, false)

  @doc "Drains over `window_ms` and returns when done. Blocks the caller."
  def run(window_ms) do
    :persistent_term.put(@key, true)
    sockets = sockets()
    Logger.info("draining #{length(sockets)} connections over #{div(window_ms, 1000)} s")

    for pid <- sockets do
      Process.send_after(pid, :drain, if(window_ms > 0, do: :rand.uniform(window_ms), else: 0))
    end

    deadline = System.monotonic_time(:millisecond) + window_ms + 5_000
    wait_until_empty(deadline)
  end

  @doc "Takes connections again (tests; a pod never un-drains)."
  def reset, do: :persistent_term.put(@key, false)

  defp sockets do
    registries = [XmsgGateway.Users, XmsgGateway.Admission.preauth_registry()]

    registries
    |> Enum.flat_map(&registered/1)
    |> Enum.uniq()
  end

  # A registry already gone (the application is stopping after a crash) holds no sockets.
  defp registered(registry) do
    Registry.select(registry, [{{:_, :"$1", :_}, [], [:"$1"]}])
  rescue
    ArgumentError -> []
  end

  defp wait_until_empty(deadline) do
    left = length(sockets())

    cond do
      left == 0 ->
        Logger.info("drained")
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        Logger.warning("drain window over with #{left} connections left")
        :timeout

      true ->
        Process.sleep(200)
        wait_until_empty(deadline)
    end
  end
end
