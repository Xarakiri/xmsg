# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Bus.Local do
  @moduledoc "In-memory bus for tests: Registry-based subscriptions and a pluggable request responder."
  @behaviour XmsgGateway.Bus

  @registry __MODULE__.Subscriptions

  @impl true
  def child_specs, do: [{Registry, keys: :duplicate, name: @registry}]

  @impl true
  def subscribe(subject) do
    {:ok, _} = Registry.register(@registry, subject, nil)
    {:ok, Process.whereis(@registry)}
  end

  @impl true
  def publish(subject, body) do
    Registry.dispatch(@registry, subject, fn entries ->
      for {pid, _} <- entries, do: send(pid, {:bus_msg, subject, body})
    end)
  end

  @impl true
  def request(subject, body, _timeout_ms) do
    case :persistent_term.get({__MODULE__, :responder}, nil) do
      nil -> {:error, :no_responders}
      responder -> responder.(subject, body)
    end
  end

  @doc "Sets how requests are answered: a fun `(subject, body) -> {:ok, reply} | {:error, reason}`."
  def put_responder(fun), do: :persistent_term.put({__MODULE__, :responder}, fun)
end
