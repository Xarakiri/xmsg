# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.DrainTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.{Drain, WsClient}
  alias Xmsg.V1.{ClientFrame, Connect, ServerFrame, Welcome}

  setup do
    previous = Application.get_env(:xmsg_gateway, :admission_rate)
    Application.put_env(:xmsg_gateway, :admission_rate, 0)

    on_exit(fn ->
      Drain.reset()
      Application.put_env(:xmsg_gateway, :admission_rate, previous || 2_000)
    end)

    %{port: XmsgGateway.Application.port()}
  end

  defp connect_as(port, user_id) do
    c =
      port
      |> WsClient.connect()
      |> WsClient.send_frame(%ClientFrame{
        frame: {:connect, %Connect{access_token: "dev:#{user_id}", protocol_version: 1}}
      })

    {c, %ServerFrame{frame: {:welcome, %Welcome{}}}, _} = WsClient.recv_server_frame(c)
    c
  end

  defp health(port) do
    {:ok, conn} = Mint.HTTP.connect(:http, "localhost", port, mode: :passive)
    {:ok, conn, _ref} = Mint.HTTP.request(conn, "GET", "/healthz", [], nil)
    {:ok, _conn, responses} = Mint.HTTP.recv(conn, 0, 2_000)

    Enum.find_value(responses, fn
      {:status, _, s} -> s
      _ -> nil
    end)
  end

  defp next_close(c) do
    case WsClient.recv(c, 3_000) do
      # Like a real client, answer by closing; the server's socket then ends.
      {c, {:close, code, reason}} ->
        Mint.HTTP.close(c.conn)
        {code, reason}

      {c, {:binary, _}} ->
        next_close(c)

      {_c, other} ->
        other
    end
  end

  test "a drain refuses newcomers and closes everyone, spread over the window", %{port: port} do
    clients = for id <- 601..604, do: connect_as(port, id)
    assert health(port) == 200

    started = System.monotonic_time(:millisecond)
    drain = Task.async(fn -> Drain.run(800) end)
    Process.sleep(50)

    # Out of the Service, and no new sockets while draining.
    assert health(port) == 503
    assert {:refused, 503, _} = WsClient.try_connect(port)

    closes = Enum.map(clients, &next_close/1)
    assert Enum.all?(closes, &(&1 == {1012, "service restart"})), inspect(closes)
    assert Task.await(drain, 10_000) == :ok
    assert System.monotonic_time(:millisecond) - started < 2_000

    for id <- 601..604, do: assert(Registry.lookup(XmsgGateway.Users, id) == [])
  end

  test "an empty gateway drains at once" do
    assert Drain.run(60_000) == :ok
  end
end
