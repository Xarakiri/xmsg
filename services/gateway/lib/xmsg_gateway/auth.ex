# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.Auth do
  @moduledoc """
  Access-token validation (plan §5.2): Ed25519 (EdDSA) JWTs issued by the auth
  service, checked locally against its JWKS (`XmsgGateway.Auth.Jwks`). No call
  to the auth service per connect.

  Development tokens (`dev:<user_id>`) are accepted only when `dev_auth` is on.
  """

  alias XmsgGateway.Auth.Jwks

  @issuer "xmsg"
  @audience "xmsg"
  @leeway_s 30

  @spec verify(String.t()) :: {:ok, integer()} | {:error, :invalid_token}
  def verify(token) do
    with {:ok, %{user_id: user_id}} <- authenticate(token), do: {:ok, user_id}
  end

  @doc """
  The user and session (`sid`, the device) of a valid token. Development tokens have
  session 0, which is never revoked.
  """
  @spec authenticate(term()) ::
          {:ok, %{user_id: integer(), session_id: integer()}} | {:error, :invalid_token}
  def authenticate("dev:" <> id) do
    with true <- XmsgGateway.Config.dev_auth?(),
         {user_id, ""} when user_id > 0 <- Integer.parse(id) do
      {:ok, %{user_id: user_id, session_id: 0}}
    else
      _ -> {:error, :invalid_token}
    end
  end

  def authenticate(token) when is_binary(token) do
    with {:ok, %{"sub" => sub} = claims} <- verify_jwt(token, System.os_time(:second)),
         {:ok, user_id} <- parse_user_id(sub) do
      session_id = if is_integer(claims["sid"]), do: claims["sid"], else: 0
      {:ok, %{user_id: user_id, session_id: session_id}}
    else
      _ -> {:error, :invalid_token}
    end
  end

  def authenticate(_), do: {:error, :invalid_token}

  @doc "Checks signature and claims; returns the claims. `now` is Unix seconds."
  @spec verify_jwt(String.t(), integer()) :: {:ok, map()} | {:error, atom()}
  def verify_jwt(token, now) do
    with [h64, c64, s64] <- String.split(token, "."),
         {:ok, header} <- decode_json(h64),
         %{"alg" => "EdDSA", "kid" => kid} when is_binary(kid) <- header,
         {:ok, sig} <- Base.url_decode64(s64, padding: false),
         {:ok, public_key} <- Jwks.key(kid),
         true <- :crypto.verify(:eddsa, :none, h64 <> "." <> c64, sig, [public_key, :ed25519]),
         {:ok, claims} <- decode_json(c64),
         :ok <- check_claims(claims, now) do
      {:ok, claims}
    else
      {:error, reason} when is_atom(reason) -> {:error, reason}
      false -> {:error, :bad_signature}
      _ -> {:error, :malformed}
    end
  end

  defp check_claims(%{"iss" => @issuer, "aud" => aud, "exp" => exp} = claims, now)
       when is_integer(exp) do
    cond do
      aud != @audience and not (is_list(aud) and @audience in aud) -> {:error, :wrong_audience}
      exp + @leeway_s < now -> {:error, :expired}
      is_integer(claims["nbf"]) and claims["nbf"] - @leeway_s > now -> {:error, :not_yet_valid}
      true -> :ok
    end
  end

  defp check_claims(_claims, _now), do: {:error, :bad_claims}

  defp decode_json(part) do
    with {:ok, bin} <- Base.url_decode64(part, padding: false),
         {:ok, %{} = map} <- JSON.decode(bin) do
      {:ok, map}
    else
      _ -> {:error, :malformed}
    end
  end

  defp parse_user_id(sub) when is_binary(sub) do
    case Integer.parse(sub) do
      {user_id, ""} when user_id > 0 -> {:ok, user_id}
      _ -> {:error, :invalid_token}
    end
  end

  defp parse_user_id(_), do: {:error, :invalid_token}
end
