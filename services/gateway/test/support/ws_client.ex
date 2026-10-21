# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.WsClient do
  @moduledoc "A minimal synchronous WebSocket client for tests (Mint, passive mode)."

  alias Xmsg.V1.{ClientFrame, ServerFrame}

  defstruct [:conn, :ref, :ws, frames: []]

  def connect(port) do
    {:ok, conn} = Mint.HTTP.connect(:http, "localhost", port, mode: :passive, protocols: [:http1])
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/ws", [])
    {conn, status, headers} = await_upgrade(conn, ref, nil, [])
    {:ok, conn, ws} = Mint.WebSocket.new(conn, ref, status, headers)
    %__MODULE__{conn: conn, ref: ref, ws: ws}
  end

  defp await_upgrade(conn, ref, status, headers) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, 5_000)

    Enum.reduce(responses, {conn, status, headers, false}, fn
      {:status, ^ref, s}, {c, _, h, d} -> {c, s, h, d}
      {:headers, ^ref, h}, {c, s, _, d} -> {c, s, h, d}
      {:done, ^ref}, {c, s, h, _} -> {c, s, h, true}
      _, acc -> acc
    end)
    |> case do
      {conn, status, headers, true} -> {conn, status, headers}
      {conn, status, headers, false} -> await_upgrade(conn, ref, status, headers)
    end
  end

  def send_frame(%__MODULE__{} = c, %ClientFrame{} = frame),
    do: send_raw(c, {:binary, ClientFrame.encode(frame)})

  def send_raw(%__MODULE__{} = c, frame) do
    {:ok, ws, data} = Mint.WebSocket.encode(c.ws, frame)
    {:ok, conn} = Mint.WebSocket.stream_request_body(c.conn, c.ref, data)
    %{c | ws: ws, conn: conn}
  end

  @doc "Next frame: `{client, {:binary, bin}}`, `{client, {:close, code, reason}}`, `{client, :closed}` or `{client, :timeout}`."
  def recv(%__MODULE__{frames: [frame | rest]} = c, _timeout), do: {%{c | frames: rest}, frame}

  def recv(%__MODULE__{} = c, timeout) do
    # After the upgrade, passive connections must be read with Mint.WebSocket.recv/3.
    case Mint.WebSocket.recv(c.conn, 0, timeout) do
      {:ok, conn, responses} ->
        # Returns the first decoded frame, or keeps reading if this chunk held none.
        recv(decode(%{c | conn: conn}, responses), timeout)

      {:error, conn, reason, _}
      when reason == :timeout or reason == %Mint.TransportError{reason: :timeout} ->
        {%{c | conn: conn}, :timeout}

      # The server may send its close frame and close the socket in one go:
      # Mint then reports an error together with the data it did read.
      {:error, conn, _error, responses} ->
        case decode(%{c | conn: conn}, responses) do
          %{frames: [frame | rest]} = c -> {%{c | frames: rest}, frame}
          c -> {c, :closed}
        end
    end
  end

  defp decode(c, responses) do
    Enum.reduce(responses, c, fn
      {:data, _ref, data}, c ->
        {:ok, ws, frames} = Mint.WebSocket.decode(c.ws, data)
        %{c | ws: ws, frames: c.frames ++ frames}

      _, c ->
        c
    end)
  end

  @doc "Next binary frame decoded as a ServerFrame."
  def recv_server_frame(c, timeout \\ 2_000) do
    case recv(c, timeout) do
      {c, {:binary, bin}} -> {c, ServerFrame.decode(bin), bin}
      {c, other} -> {c, other, nil}
    end
  end
end
