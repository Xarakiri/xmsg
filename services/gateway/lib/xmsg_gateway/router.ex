# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Router do
  @moduledoc false
  use Plug.Router

  plug(:match)
  plug(:dispatch)

  get "/ws" do
    conn
    |> WebSockAdapter.upgrade(XmsgGateway.Socket, %{}, timeout: 70_000, compress: false)
    |> halt()
  end

  get "/healthz" do
    send_resp(conn, 200, "ok")
  end

  match _ do
    send_resp(conn, 404, "not found")
  end
end
