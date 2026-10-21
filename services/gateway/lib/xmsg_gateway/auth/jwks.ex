# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Auth.Jwks do
  @moduledoc """
  The auth service's signing keys, fetched from its JWKS URL at startup and every
  few minutes. Keys live in `:persistent_term` (read on every connect, written
  only when the set changes). An unknown `kid` triggers an early refresh, at most
  once per `@min_refresh_ms`, so a newly rotated key is picked up without waiting.

  Without a JWKS URL (tests, dev-only setups) the key set is whatever `put_keys/1`
  last stored.
  """
  use GenServer
  require Logger

  @key {__MODULE__, :keys}
  @refresh_ms 300_000
  @retry_ms 2_000
  @min_refresh_ms 30_000

  def start_link(url), do: GenServer.start_link(__MODULE__, url, name: __MODULE__)

  @doc "The Ed25519 public key for `kid`, refreshing the set once if it's unknown."
  @spec key(String.t()) :: {:ok, binary()} | {:error, :unknown_key}
  def key(kid) do
    case Map.fetch(keys(), kid) do
      {:ok, key} ->
        {:ok, key}

      :error ->
        refreshed = GenServer.whereis(__MODULE__) && GenServer.call(__MODULE__, :refresh_if_stale)

        if refreshed == :ok,
          do: Map.fetch(keys(), kid) |> ok_or_unknown(),
          else: {:error, :unknown_key}
    end
  catch
    :exit, _ -> {:error, :unknown_key}
  end

  defp ok_or_unknown({:ok, key}), do: {:ok, key}
  defp ok_or_unknown(:error), do: {:error, :unknown_key}

  def keys, do: :persistent_term.get(@key, %{})

  @doc "Points the fetcher at another JWKS URL and fetches it now (tests)."
  def set_url(url), do: GenServer.call(__MODULE__, {:set_url, url})

  @doc "Replaces the key set (`%{kid => public key}`)."
  def put_keys(keys) when is_map(keys) do
    if keys != keys(), do: :persistent_term.put(@key, keys)
    :ok
  end

  @doc "Parses an RFC 7517 key set, keeping the OKP/Ed25519 signing keys."
  @spec parse(binary()) :: {:ok, map()} | {:error, term()}
  def parse(body) do
    with {:ok, %{"keys" => list}} when is_list(list) <- JSON.decode(body) do
      keys =
        for %{"kty" => "OKP", "crv" => "Ed25519", "kid" => kid, "x" => x} = jwk <- list,
            Map.get(jwk, "use", "sig") == "sig",
            {:ok, public} <- [Base.url_decode64(x, padding: false)],
            byte_size(public) == 32,
            into: %{},
            do: {kid, public}

      {:ok, keys}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :not_a_jwks}
    end
  end

  @impl true
  def init(nil), do: {:ok, %{url: nil, fetched_at: nil}}

  def init(url) do
    send(self(), :refresh)
    {:ok, %{url: url, fetched_at: nil}}
  end

  @impl true
  def handle_info(:refresh, state) do
    state = fetch(state)
    Process.send_after(self(), :refresh, if(state.fetched_at, do: @refresh_ms, else: @retry_ms))
    {:noreply, state}
  end

  @impl true
  def handle_call({:set_url, url}, _from, state) do
    {:reply, :ok, fetch(%{state | url: url, fetched_at: nil})}
  end

  def handle_call(:refresh_if_stale, _from, %{url: nil} = state), do: {:reply, :stale, state}

  def handle_call(:refresh_if_stale, _from, state) do
    now = System.monotonic_time(:millisecond)

    if state.fetched_at && now - state.fetched_at < @min_refresh_ms do
      {:reply, :stale, state}
    else
      state = fetch(state)
      {:reply, if(state.fetched_at && state.fetched_at >= now, do: :ok, else: :stale), state}
    end
  end

  defp fetch(%{url: nil} = state), do: state

  defp fetch(%{url: url} = state) do
    request = {String.to_charlist(url), [{~c"accept", ~c"application/json"}]}

    with {:ok, {{_, 200, _}, _headers, body}} <-
           :httpc.request(:get, request, [timeout: 2_000, connect_timeout: 2_000],
             body_format: :binary
           ),
         {:ok, keys} when map_size(keys) > 0 <- parse(body) do
      if keys != keys(), do: Logger.info("JWKS loaded: #{Enum.join(Map.keys(keys), ", ")}")
      put_keys(keys)
      %{state | fetched_at: System.monotonic_time(:millisecond)}
    else
      other ->
        Logger.warning("JWKS fetch from #{url} failed: #{inspect(other)}")
        state
    end
  end
end
