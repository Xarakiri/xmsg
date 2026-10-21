// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import java.net.InetAddress
import java.util.Properties
import java.util.concurrent.CountDownLatch

/** Builds the Kafka Streams configuration from environment variables. */
fun streamsConfig(env: Map<String, String> = System.getenv()): Properties = Properties().apply {
    put(StreamsConfig.APPLICATION_ID_CONFIG, Names.APPLICATION_ID)
    put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, env["XMSG_KAFKA_BOOTSTRAP"] ?: "localhost:9092")
    put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
    // ADR-0003: commit every 5–10 ms; the EOS default (100 ms) adds too much delivery latency.
    put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, env["XMSG_COMMIT_INTERVAL_MS"] ?: "10")
    put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, env["XMSG_STANDBY_REPLICAS"] ?: "0")
    put(StreamsConfig.REPLICATION_FACTOR_CONFIG, env["XMSG_REPLICATION_FACTOR"] ?: "1")
    put(StreamsConfig.STATE_DIR_CONFIG, env["XMSG_STATE_DIR"] ?: "/tmp/xmsg-sequencer")
    put(StreamsConfig.producerPrefix("linger.ms"), "1")
    // Static membership: restarts don't trigger rebalances (ADR-0003).
    put(
        StreamsConfig.consumerPrefix("group.instance.id"),
        env["XMSG_INSTANCE_ID"] ?: InetAddress.getLocalHost().hostName,
    )
}

fun main() {
    val streams = KafkaStreams(SequencerTopology.build(), streamsConfig())
    val stopped = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            streams.close()
            stopped.countDown()
        },
    )
    streams.setUncaughtExceptionHandler { _ ->
        org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD
    }
    streams.start()
    stopped.await()
}
