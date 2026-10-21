# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Auth do
  @moduledoc """
  Access-token validation. Phase 0 only accepts development tokens
  (`dev:<user_id>`) and only when `dev_auth` is on; Ed25519 JWT validation
  against the auth service's JWKS replaces this in Phase 1a (plan §5.2).
  """

  @spec verify(String.t()) :: {:ok, integer()} | {:error, :invalid_token}
  def verify("dev:" <> id) do
    with true <- XmsgGateway.Config.dev_auth?(),
         {user_id, ""} when user_id > 0 <- Integer.parse(id) do
      {:ok, user_id}
    else
      _ -> {:error, :invalid_token}
    end
  end

  def verify(_), do: {:error, :invalid_token}
end
