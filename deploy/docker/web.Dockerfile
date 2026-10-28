# SPDX-License-Identifier: AGPL-3.0-only
# The web client, served by nginx, which also forwards the API paths and the WebSocket
# to the services (one origin, like the Vite dev server). Build from the repository root:
#   docker build -f deploy/docker/web.Dockerfile -t xmsg/web .
FROM node:26 AS build
WORKDIR /src/clients
COPY clients/package.json clients/package-lock.json ./
COPY clients/core/package.json core/
COPY clients/web/package.json web/
RUN npm ci
COPY clients ./
RUN npm run build --workspace @xmsg/web

FROM nginx:1.29-alpine
COPY deploy/docker/web/default.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /src/clients/web/dist /usr/share/nginx/html
# Upstreams (service:port), set by the Helm chart.
ENV XMSG_AUTH_UPSTREAM=auth:8081 XMSG_CHANNELS_UPSTREAM=channels:8082 XMSG_MESSAGES_UPSTREAM=messages:8083 \
    XMSG_USERS_UPSTREAM=users:8084 XMSG_READSTATE_UPSTREAM=readstate:8085 XMSG_PUSH_UPSTREAM=push:8086 XMSG_MEDIA_UPSTREAM=media:8087 \
    XMSG_GATEWAY_UPSTREAM=gateway:4000
