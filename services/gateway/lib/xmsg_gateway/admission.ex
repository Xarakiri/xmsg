# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Admission do
  @moduledoc """
  Admission control (plan §5.1, §5.13): limits on what this gateway takes on, checked
  before the WebSocket upgrade so a reconnect storm spreads out instead of piling up.

  - **New sessions per second** (`XMSG_ADMISSION_RATE`, default 2,000): beyond it the
    upgrade gets `503` with a jittered `Retry-After`, and clients back off and retry
    (they do anyway, ADR-0011).
  - **Connections** (`XMSG_MAX_CONNECTIONS`, default off): a ceiling per pod.
  - **Unauthenticated connections per address** (`XMSG_PREAUTH_PER_IP`, default 100, 0 =
    off): sockets that haven't sent a valid `Connect` yet are cheap to open and hold.

  The per-second count is a lock-free window in `:atomics`; connection counts come from
  the registries the sockets are in, so a closed socket stops counting by itself.
  """

  alias XmsgGateway.Config

  @key {__MODULE__, :window}

  @doc "The registry unauthenticated sockets are in, keyed by client address."
  def preauth_registry, do: XmsgGateway.PreAuth

  @doc "Called once at start."
  def init, do: :persistent_term.put(@key, :atomics.new(2, signed: true))

  @doc "`:ok`, or `{:refuse, reason}` for a new connection from `ip` (nil if unknown)."
  def check(ip) do
    cond do
      not under_rate?() -> {:refuse, :rate}
      full?() -> {:refuse, :full}
      over_preauth?(ip) -> {:refuse, :preauth}
      true -> :ok
    end
  end

  @doc "Seconds a refused client should wait: 1 to 5, so retries spread out."
  def retry_after, do: Enum.random(1..5)

  defp under_rate? do
    case Config.admission_rate() do
      rate when rate <= 0 ->
        true

      rate ->
        ref = :persistent_term.get(@key)
        now = System.monotonic_time(:second)
        window = :atomics.get(ref, 1)

        # The first one into a new second resets the count; losing that race is harmless.
        if window != now and :atomics.compare_exchange(ref, 1, window, now) == :ok do
          :atomics.put(ref, 2, 0)
        end

        :atomics.add_get(ref, 2, 1) <= rate
    end
  end

  defp full? do
    case Config.max_connections() do
      max when max <= 0 -> false
      max -> Registry.count(XmsgGateway.Users) + Registry.count(preauth_registry()) >= max
    end
  end

  defp over_preauth?(nil), do: false

  defp over_preauth?(ip) do
    case Config.preauth_per_ip() do
      max when max <= 0 -> false
      max -> length(Registry.lookup(preauth_registry(), ip)) >= max
    end
  end
end
