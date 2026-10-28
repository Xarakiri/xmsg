# SPDX-License-Identifier: AGPL-3.0-only
# Every Rust service in one image; each deployment runs its own binary
# (xmsg-auth, xmsg-messages, ...). Build from the repository root:
#   docker build -f deploy/docker/services.Dockerfile -t xmsg/services .
FROM rust:1.99-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bins \
      -p xmsg-auth -p xmsg-users -p xmsg-channels -p xmsg-messages -p xmsg-persister -p xmsg-fanout \
      -p xmsg-outbox -p xmsg-readstate -p xmsg-reaction-batcher -p xmsg-presence -p xmsg-push \
      -p xmsg-search-indexer -p xmsg-media -p xmsg-calls \
    && mkdir /out && find target/release -maxdepth 1 -type f -name 'xmsg-*' -perm -u+x -exec cp {} /out/ \;

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 xmsg
COPY --from=build /out/ /usr/local/bin/
USER 10001
