# SPDX-License-Identifier: AGPL-3.0-only
import Config

if config_env() != :test do
  env = fn name, default -> System.get_env(name, default) end

  config :xmsg_gateway,
    gateway_id: env.("XMSG_GATEWAY_ID", System.get_env("HOSTNAME", "local")),
    port: String.to_integer(env.("XMSG_GATEWAY_PORT", "4000")),
    partition_count: String.to_integer(env.("XMSG_PARTITIONS", "16")),
    dev_auth: env.("XMSG_DEV_AUTH", "false") == "true",
    # The auth service's key set, e.g. http://auth:8081/.well-known/jwks.json
    jwks_url: System.get_env("XMSG_AUTH_JWKS_URL"),
    # The channels service, e.g. http://channels:8082: each user's pull-mode channels.
    channels_url: System.get_env("XMSG_CHANNELS_URL"),
    # Rebuild the revocation deny set from Kafka auth.events on start.
    revocations_replay: env.("XMSG_REVOCATIONS_REPLAY", "true") == "true",
    kafka_bootstrap: env.("XMSG_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
    # Admission control and per-connection limits (XmsgGateway.Admission).
    admission_rate: String.to_integer(env.("XMSG_ADMISSION_RATE", "2000")),
    max_connections: String.to_integer(env.("XMSG_MAX_CONNECTIONS", "0")),
    preauth_per_ip: String.to_integer(env.("XMSG_PREAUTH_PER_IP", "100")),
    proxy_hops: String.to_integer(env.("XMSG_PROXY_HOPS", "0")),
    frame_rate: String.to_integer(env.("XMSG_FRAME_RATE", "30")),
    frame_burst: String.to_integer(env.("XMSG_FRAME_BURST", "100")),
    drain_seconds: String.to_integer(env.("XMSG_DRAIN_SECONDS", "300")),
    nats: %{
      host: env.("XMSG_NATS_HOST", "127.0.0.1"),
      port: String.to_integer(env.("XMSG_NATS_PORT", "4222"))
    }
end
