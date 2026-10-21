# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.TestJwt do
  @moduledoc "Signs EdDSA JWTs the way the auth service does, for tests."

  def keypair do
    {public, private} = :crypto.generate_key(:eddsa, :ed25519)

    %{
      kid: Base.encode16(:crypto.strong_rand_bytes(8), case: :lower),
      public: public,
      private: private
    }
  end

  def sign(%{kid: kid, private: private}, claims, header \\ %{}) do
    header = Map.merge(%{"alg" => "EdDSA", "typ" => "JWT", "kid" => kid}, header)
    input = b64(JSON.encode!(header)) <> "." <> b64(JSON.encode!(claims))
    input <> "." <> b64(:crypto.sign(:eddsa, :none, input, [private, :ed25519]))
  end

  def claims(user_id, overrides \\ %{}) do
    now = System.os_time(:second)

    Map.merge(
      %{
        "sub" => to_string(user_id),
        "sid" => 1,
        "iat" => now,
        "exp" => now + 900,
        "iss" => "xmsg",
        "aud" => "xmsg"
      },
      overrides
    )
  end

  def jwks(keys) do
    JSON.encode!(%{
      "keys" =>
        for k <- keys do
          %{
            "kty" => "OKP",
            "crv" => "Ed25519",
            "alg" => "EdDSA",
            "use" => "sig",
            "kid" => k.kid,
            "x" => b64(k.public)
          }
        end
    })
  end

  defp b64(bin), do: Base.url_encode64(bin, padding: false)
end
