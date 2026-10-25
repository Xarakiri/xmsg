# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.ClientIp do
  @moduledoc """
  The client's address behind `XMSG_PROXY_HOPS` trusted proxies, the same rule as the
  Rust services (`xmsg_ratelimit::client_ip`): each proxy appends to `X-Forwarded-For`,
  so with H proxies the client is the H-th entry from the right. Anything further left
  was written by the client. With no proxies it's the TCP peer.
  """

  @doc "The address as a string, or nil when it can't be told."
  def from(peer, forwarded_for, hops)

  def from(peer, _forwarded_for, 0), do: peer && peer |> :inet.ntoa() |> to_string()

  def from(_peer, nil, _hops), do: nil

  def from(_peer, forwarded_for, hops) do
    entries = forwarded_for |> String.split(",") |> Enum.map(&String.trim/1)
    index = length(entries) - hops

    with true <- index >= 0,
         {:ok, ip} <- entries |> Enum.at(index) |> String.to_charlist() |> :inet.parse_address() do
      ip |> :inet.ntoa() |> to_string()
    else
      _ -> nil
    end
  end
end
