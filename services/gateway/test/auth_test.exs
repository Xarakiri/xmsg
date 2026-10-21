# SPDX-License-Identifier: AGPL-3.0-only
defmodule XmsgGateway.AuthTest do
  use ExUnit.Case, async: false

  alias XmsgGateway.Auth
  alias XmsgGateway.Auth.Jwks
  alias XmsgGateway.TestJwt

  setup do
    key = TestJwt.keypair()
    previous = Jwks.keys()
    Jwks.put_keys(%{key.kid => key.public})
    on_exit(fn -> Jwks.put_keys(previous) end)
    %{key: key}
  end

  test "a valid token yields its user", %{key: key} do
    assert Auth.verify(TestJwt.sign(key, TestJwt.claims(42))) == {:ok, 42}
  end

  test "expired, foreign-audience and foreign-issuer tokens are refused", %{key: key} do
    now = System.os_time(:second)
    expired = TestJwt.sign(key, TestJwt.claims(1, %{"exp" => now - 60}))
    assert Auth.verify_jwt(expired, now) == {:error, :expired}
    # Within the 30 s leeway it's still fine.
    assert {:ok, _} =
             Auth.verify_jwt(TestJwt.sign(key, TestJwt.claims(1, %{"exp" => now - 10})), now)

    audience = TestJwt.sign(key, TestJwt.claims(1, %{"aud" => "other"}))
    assert Auth.verify_jwt(audience, now) == {:error, :wrong_audience}
    issuer = TestJwt.sign(key, TestJwt.claims(1, %{"iss" => "other"}))
    assert Auth.verify_jwt(issuer, now) == {:error, :bad_claims}
  end

  test "tampered, unsigned and unknown-key tokens are refused", %{key: key} do
    [h, _c, s] = String.split(TestJwt.sign(key, TestJwt.claims(1)), ".")
    forged_claims = Base.url_encode64(JSON.encode!(TestJwt.claims(2)), padding: false)
    assert Auth.verify(Enum.join([h, forged_claims, s], ".")) == {:error, :invalid_token}

    none = TestJwt.sign(key, TestJwt.claims(1), %{"alg" => "none"})
    assert Auth.verify_jwt(none, System.os_time(:second)) == {:error, :malformed}

    other = TestJwt.keypair()

    assert Auth.verify_jwt(TestJwt.sign(other, TestJwt.claims(1)), System.os_time(:second)) ==
             {:error, :unknown_key}

    assert Auth.verify("not.a.jwt") == {:error, :invalid_token}
    assert Auth.verify("") == {:error, :invalid_token}
  end

  test "a non-numeric subject is refused", %{key: key} do
    assert Auth.verify(TestJwt.sign(key, TestJwt.claims("alice"))) == {:error, :invalid_token}
  end

  test "JWKS parsing keeps only Ed25519 signing keys" do
    k = TestJwt.keypair()

    body =
      JSON.encode!(%{
        "keys" => [
          %{
            "kty" => "OKP",
            "crv" => "Ed25519",
            "kid" => k.kid,
            "x" => Base.url_encode64(k.public, padding: false)
          },
          %{"kty" => "RSA", "kid" => "rsa", "n" => "x", "e" => "AQAB"},
          %{
            "kty" => "OKP",
            "crv" => "Ed25519",
            "kid" => "enc",
            "use" => "enc",
            "x" => Base.url_encode64(k.public, padding: false)
          },
          %{"kty" => "OKP", "crv" => "Ed25519", "kid" => "short", "x" => "AAAA"}
        ]
      })

    assert Jwks.parse(body) == {:ok, %{k.kid => k.public}}
    assert {:error, _} = Jwks.parse("nope")
  end

  test "the key set is fetched from the JWKS URL and a new kid triggers a refresh" do
    k1 = TestJwt.keypair()
    k2 = TestJwt.keypair()
    served = :counters.new(1, [])
    jwks = Agent.start_link(fn -> TestJwt.jwks([k1]) end) |> elem(1)

    plug = fn conn, _ ->
      :counters.add(served, 1, 1)
      Plug.Conn.send_resp(conn, 200, Agent.get(jwks, & &1))
    end

    {:ok, server} =
      Bandit.start_link(plug: {__MODULE__.Fn, plug}, port: 0, ip: :loopback, startup_log: false)

    {:ok, {_, port}} = ThousandIsland.listener_info(server)

    :ok = Jwks.set_url("http://127.0.0.1:#{port}/.well-known/jwks.json")
    on_exit(fn -> Jwks.set_url(nil) end)

    assert Map.has_key?(Jwks.keys(), k1.kid)
    assert Auth.verify(TestJwt.sign(k1, TestJwt.claims(5))) == {:ok, 5}

    # Rotation: the auth service publishes k2; the first token signed with it refreshes the set.
    Agent.update(jwks, fn _ -> TestJwt.jwks([k1, k2]) end)
    # Within @min_refresh_ms of the last fetch an unknown kid does not refetch...
    assert Auth.verify(TestJwt.sign(k2, TestJwt.claims(6))) == {:error, :invalid_token}
    # ...after it, it does.
    :sys.replace_state(Jwks, fn s -> %{s | fetched_at: s.fetched_at - 60_000} end)
    assert Auth.verify(TestJwt.sign(k2, TestJwt.claims(6))) == {:ok, 6}
    assert :counters.get(served, 1) == 2
  end

  defmodule Fn do
    @moduledoc false
    def init(fun), do: fun
    def call(conn, fun), do: fun.(conn, nil)
  end
end
