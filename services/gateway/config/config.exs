# SPDX-License-Identifier: AGPL-3.0-only
import Config

config :xmsg_gateway,
  gateway_id: "local",
  port: 4000,
  partition_count: 16,
  auth_timeout_ms: 5_000,
  request_timeout_ms: 2_000,
  # Development-only tokens of the form "dev:<user_id>", accepted next to real JWTs.
  dev_auth: true,
  # JWKS of the auth service; nil = no fetching (keys only via Auth.Jwks.put_keys/1).
  jwks_url: nil,
  bus: XmsgGateway.Bus.Nats,
  sessions: XmsgGateway.Sessions.Kafka,
  kafka_bootstrap: "127.0.0.1:9092",
  nats: %{host: "127.0.0.1", port: 4222}

config :logger, level: :info

import_config "#{config_env()}.exs"
