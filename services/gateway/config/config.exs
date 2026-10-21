# SPDX-License-Identifier: AGPL-3.0-only
import Config

config :xmsg_gateway,
  gateway_id: "local",
  port: 4000,
  partition_count: 16,
  auth_timeout_ms: 5_000,
  request_timeout_ms: 2_000,
  # Development-only tokens of the form "dev:<user_id>". Real JWT validation comes with auth (Phase 1a).
  dev_auth: true,
  bus: XmsgGateway.Bus.Nats,
  nats: %{host: "127.0.0.1", port: 4222}

config :logger, level: :info

import_config "#{config_env()}.exs"
