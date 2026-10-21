# SPDX-License-Identifier: AGPL-3.0-only
import Config

if config_env() != :test do
  env = fn name, default -> System.get_env(name, default) end

  config :xmsg_gateway,
    gateway_id: env.("XMSG_GATEWAY_ID", System.get_env("HOSTNAME", "local")),
    port: String.to_integer(env.("XMSG_GATEWAY_PORT", "4000")),
    partition_count: String.to_integer(env.("XMSG_PARTITIONS", "16")),
    dev_auth: env.("XMSG_DEV_AUTH", "false") == "true",
    nats: %{
      host: env.("XMSG_NATS_HOST", "127.0.0.1"),
      port: String.to_integer(env.("XMSG_NATS_PORT", "4222"))
    }
end
