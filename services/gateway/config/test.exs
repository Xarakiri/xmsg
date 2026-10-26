# SPDX-License-Identifier: AGPL-3.0-only
import Config

config :xmsg_gateway,
  gateway_id: "test-gw",
  port: 0,
  partition_count: 16,
  auth_timeout_ms: 300,
  request_timeout_ms: 500,
  bus: XmsgGateway.Bus.Local,
  sessions: XmsgGateway.Sessions.Local,
  # The test run ends without a long drain.
  drain_seconds: 0,
  bump_ms: 300

config :logger, level: :warning
