// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse
import org.apache.kafka.streams.state.HostInfo
import kotlin.system.exitProcess
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
    env["XMSG_SESSION_TIMEOUT_MS"]?.let { put(StreamsConfig.consumerPrefix("session.timeout.ms"), it) }
    // Where this instance's membership endpoint is reachable, so others can redirect to it.
    put(StreamsConfig.APPLICATION_SERVER_CONFIG, "${advertisedHost(env)}:${httpPort(env)}")
    // Static membership: restarts don't trigger rebalances (ADR-0003).
    put(
        StreamsConfig.consumerPrefix("group.instance.id"),
        env["XMSG_INSTANCE_ID"] ?: InetAddress.getLocalHost().hostName,
    )
}

fun httpPort(env: Map<String, String>): Int = env["XMSG_SEQUENCER_HTTP_PORT"]?.toIntOrNull() ?: 8090

fun advertisedHost(env: Map<String, String>): String = env["XMSG_ADVERTISED_HOST"] ?: InetAddress.getLocalHost().hostName

/** Push/pull thresholds (ADR-0008): `XMSG_FANOUT_PULL_AT` (1,000) and `XMSG_FANOUT_PUSH_BELOW` (800). */
fun fanoutThresholds(env: Map<String, String>): FanoutThresholds {
    val defaults = FanoutThresholds()
    return FanoutThresholds(
        pullAt = env["XMSG_FANOUT_PULL_AT"]?.toLongOrNull() ?: defaults.pullAt,
        pushBelow = env["XMSG_FANOUT_PUSH_BELOW"]?.toLongOrNull() ?: defaults.pushBelow,
    )
}

fun main() {
    val env = System.getenv()
    val partitions = env["XMSG_PARTITIONS"]?.toIntOrNull()
        ?: fail("XMSG_PARTITIONS must be set to this installation's partition count (ADR-0021)")
    val config = streamsConfig(env)

    Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to config[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG])).use { admin ->
        val problems = StartupCheck(admin).problems(partitions)
        if (problems.isNotEmpty()) fail(problems.joinToString("\n") { "  - $it" }.let { "startup check failed:\n$it" })
    }

    val thresholds = try {
        fanoutThresholds(env)
    } catch (e: IllegalArgumentException) {
        fail("fan-out thresholds: ${e.message}")
    }
    val streams = KafkaStreams(SequencerTopology.build(thresholds), config)
    val membership = MembershipServer(streams, HostInfo(advertisedHost(env), httpPort(env)), httpPort(env))
    val stopped = CountDownLatch(1)
    Runtime.getRuntime().addShutdownHook(
        Thread {
            membership.stop()
            streams.close()
            stopped.countDown()
        },
    )
    streams.setUncaughtExceptionHandler { _ -> StreamThreadExceptionResponse.REPLACE_THREAD }
    streams.start()
    membership.start()
    stopped.await()
}

private fun fail(message: String): Nothing {
    System.err.println("xmsg-sequencer: $message")
    exitProcess(2)
}
