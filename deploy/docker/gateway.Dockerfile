# SPDX-License-Identifier: AGPL-3.0-only
# The Elixir gateway as an OTP release. Build from the repository root:
#   docker build -f deploy/docker/gateway.Dockerfile -t xmsg/gateway .
FROM elixir:1.20 AS build
# crc32cer (brod's checksum NIF) builds with cmake.
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
ENV MIX_ENV=prod
WORKDIR /src/services/gateway
COPY services/gateway/mix.exs services/gateway/mix.lock ./
RUN mix local.hex --force && mix local.rebar --force && mix deps.get --only prod && mix deps.compile
COPY services/gateway ./
RUN mix release --overwrite

# The same base as the build, so the release's ERTS finds the libraries it was built against.
FROM elixir:1.20
RUN useradd --system --uid 10001 xmsg
COPY --from=build /src/services/gateway/_build/prod/rel/xmsg_gateway /opt/xmsg_gateway
USER 10001
ENTRYPOINT ["/opt/xmsg_gateway/bin/xmsg_gateway"]
CMD ["start"]
