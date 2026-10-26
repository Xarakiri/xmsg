# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Router do
  @moduledoc false
  use Plug.Router
  require Logger

  plug(:match)
  plug(:dispatch)

  get "/ws" do
    forwarded = conn |> get_req_header("x-forwarded-for") |> List.first()
    ip = XmsgGateway.ClientIp.from(conn.remote_ip, forwarded, XmsgGateway.Config.proxy_hops())

    admission =
      if XmsgGateway.Drain.draining?(),
        do: {:refuse, :draining},
        else: XmsgGateway.Admission.check(ip)

    case admission do
      :ok ->
        conn
        |> WebSockAdapter.upgrade(XmsgGateway.Socket, %{ip: ip}, timeout: 70_000, compress: false)
        |> halt()

      {:refuse, reason} ->
        Logger.debug("admission refused (#{reason})")

        conn
        |> put_resp_header("retry-after", Integer.to_string(XmsgGateway.Admission.retry_after()))
        |> send_resp(503, "busy, retry later")
        |> halt()
    end
  end

  # 503 while draining, so the readiness probe takes the pod out of its Service.
  get "/healthz" do
    if XmsgGateway.Drain.draining?(),
      do: send_resp(conn, 503, "draining"),
      else: send_resp(conn, 200, "ok")
  end

  match _ do
    send_resp(conn, 404, "not found")
  end
end
