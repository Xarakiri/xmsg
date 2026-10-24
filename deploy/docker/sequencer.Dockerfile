# SPDX-License-Identifier: AGPL-3.0-only
# The Kotlin sequencer (Kafka Streams). Build from the repository root:
#   docker build -f deploy/docker/sequencer.Dockerfile -t xmsg/sequencer .
FROM eclipse-temurin:27-jdk AS build
WORKDIR /src
COPY proto proto
COPY services/sequencer services/sequencer
RUN cd services/sequencer && ./gradlew installDist --no-daemon --console=plain

FROM eclipse-temurin:27-jre
RUN useradd --system --uid 10001 xmsg && mkdir /state && chown 10001 /state
COPY --from=build /src/services/sequencer/build/install/xmsg-sequencer /opt/xmsg-sequencer
USER 10001
ENV XMSG_STATE_DIR=/state
ENTRYPOINT ["/opt/xmsg-sequencer/bin/xmsg-sequencer"]
