# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    http = {Bandit, plug: XmsgGateway.Router, port: XmsgGateway.Config.port(), scheme: :http}

    children =
      XmsgGateway.Config.bus().child_specs() ++
        XmsgGateway.Sessions.impl().child_specs() ++
        [
          {XmsgGateway.Auth.Jwks, XmsgGateway.Config.jwks_url()},
          {Registry,
           keys: :duplicate,
           name: XmsgGateway.Revocations.registry(),
           partitions: System.schedulers_online()},
          XmsgGateway.Revocations,
          {Registry,
           keys: :duplicate, name: XmsgGateway.Users, partitions: System.schedulers_online()},
          {Registry,
           keys: :duplicate,
           name: XmsgGateway.Presence.registry(),
           partitions: System.schedulers_online()},
          XmsgGateway.Presence
        ] ++
        Enum.map(0..(XmsgGateway.Delivery.worker_count() - 1), &{XmsgGateway.Delivery.Worker, &1}) ++
        [
          XmsgGateway.Delivery,
          XmsgGateway.Heartbeat,
          XmsgGateway.Sessions.Tracker,
          Supervisor.child_spec(http, id: :http)
        ]

    Supervisor.start_link(children, strategy: :one_for_one, name: XmsgGateway.Supervisor)
  end

  @doc "The port the WebSocket server listens on (useful when configured with port 0)."
  def port do
    {:http, pid, _, _} =
      XmsgGateway.Supervisor
      |> Supervisor.which_children()
      |> List.keyfind(:http, 0)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(pid)
    port
  end
end
