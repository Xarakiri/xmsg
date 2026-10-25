# SPDX-License-Identifier: LicenseRef-xmsg-EE
# The Enterprise Edition services in one image (ADR-0022): xmsg-auth-ee (the auth service
# with SSO and LDAP, in place of xmsg-auth), xmsg-scim-ee and xmsg-admin-ee. Build from the repository
# root, since EE depends on the core crates:
#   docker build -f ee/deploy/Dockerfile -t xmsg/enterprise .
FROM rust:1.99-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler cmake && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release --locked --manifest-path ee/Cargo.toml --bins -p xmsg-auth-ee -p xmsg-scim-ee -p xmsg-admin-ee \
    && mkdir /out && cp ee/target/release/xmsg-auth-ee ee/target/release/xmsg-scim-ee ee/target/release/xmsg-admin-ee /out/

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 xmsg
COPY --from=build /out/ /usr/local/bin/
USER 10001
