// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import dev.xmsg.proto.v1.ChannelCreated
import dev.xmsg.proto.v1.ChannelType
import dev.xmsg.proto.v1.InboundEvent
import dev.xmsg.proto.v1.MemberKickRequested
import dev.xmsg.proto.v1.SendMessage
import dev.xmsg.proto.v1.SequencedEvent
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.LongSerializer
import org.testcontainers.kafka.KafkaContainer
import java.io.File
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit

/** A single-broker Kafka in Docker, configured like the Small tier (transactions on one node). */
class KafkaHarness : AutoCloseable {
    val kafka: KafkaContainer = KafkaContainer("apache/kafka:3.9.1")
        .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .withEnv("KAFKA_MIN_INSYNC_REPLICAS", "1")
        .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .withEnv("KAFKA_GROUP_MIN_SESSION_TIMEOUT_MS", "1000")
        .apply { start() }

    val bootstrap: String get() = kafka.bootstrapServers

    val admin: Admin = Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap))

    private val producer = KafkaProducer(
        mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG to true,
            ProducerConfig.ACKS_CONFIG to "all",
        ),
        LongSerializer(),
        ByteArraySerializer(),
    )

    fun createTopics(partitions: Int, channelTopicPartitions: Int = partitions) {
        admin.createTopics(
            listOf(
                NewTopic(Names.TOPIC_IN, channelTopicPartitions, 1.toShort()),
                NewTopic(Names.TOPIC_PERSISTED, channelTopicPartitions, 1.toShort()),
                NewTopic(Names.TOPIC_COMMAND_RESULTS, 1, 1.toShort()),
            ),
        ).all().get(30, TimeUnit.SECONDS)
    }

    fun send(event: InboundEvent) {
        producer.send(ProducerRecord(Names.TOPIC_IN, event.channelId, event.toByteArray())).get(30, TimeUnit.SECONDS)
    }

    fun sendAsync(event: InboundEvent) {
        producer.send(ProducerRecord(Names.TOPIC_IN, event.channelId, event.toByteArray()))
    }

    fun flush() = producer.flush()

    /**
     * Reads committed output (read_committed: aborted transactions are invisible)
     * until [done] holds for everything read so far, or the timeout expires.
     */
    fun readUntil(timeout: Duration, done: (List<SequencedEvent>) -> Boolean): List<SequencedEvent> {
        val events = mutableListOf<SequencedEvent>()
        KafkaConsumer(
            mapOf(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
                ConsumerConfig.GROUP_ID_CONFIG to "it-reader-${UUID.randomUUID()}",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG to "read_committed",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ),
            LongDeserializer(),
            ByteArrayDeserializer(),
        ).use { consumer ->
            consumer.subscribe(listOf(Names.TOPIC_PERSISTED))
            val deadline = System.nanoTime() + timeout.toNanos()
            while (System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach { events += SequencedEvent.parseFrom(it.value()) }
                if (done(events)) return events
            }
        }
        return events
    }

    override fun close() {
        producer.close()
        admin.close()
        kafka.stop()
    }

    companion object {
        const val WORKSPACE = 10L

        fun base(channel: Long, actor: Long, dedupe: String = "", request: String = ""): InboundEvent.Builder =
            InboundEvent.newBuilder().setChannelId(channel).setWorkspaceId(WORKSPACE).setActorId(actor)
                .setDedupeId(dedupe).setRequestId(request)

        fun createChannel(channel: Long, actor: Long, type: ChannelType, members: Collection<Long>) =
            base(channel, actor, dedupe = "create-$channel")
                .setChannelCreated(ChannelCreated.newBuilder().setType(type).addAllInitialMemberIds(members))
                .build()

        fun message(channel: Long, actor: Long, dedupe: String, body: String = dedupe) =
            base(channel, actor, dedupe = dedupe).setSendMessage(SendMessage.newBuilder().setBody(body)).build()

        fun kick(channel: Long, actor: Long, user: Long, request: String) =
            base(channel, actor, request = request)
                .setMemberKickRequested(MemberKickRequested.newBuilder().setUserId(user)).build()
    }
}

/**
 * The sequencer as a separate JVM process, so a test can kill it with SIGKILL
 * in the middle of a transaction, like a real crash.
 */
class SequencerProcess(
    private val bootstrap: String,
    private val partitions: Int,
    val instanceId: String,
    val stateDir: File = Files.createTempDirectory("xmsg-seq-$instanceId").toFile(),
) {
    private var process: Process? = null
    private val logDir = File(System.getProperty("xmsg.it.logs") ?: "build/it-logs").apply { mkdirs() }
    val logFile = File(logDir, "sequencer-$instanceId.log")

    fun start(): SequencerProcess {
        check(process?.isAlive != true) { "already running" }
        val java = ProcessHandle.current().info().command().orElse("java")
        process = ProcessBuilder(java, "-Xmx256m", "-cp", System.getProperty("java.class.path"), "dev.xmsg.sequencer.AppKt")
            .apply {
                environment().putAll(
                    mapOf(
                        "XMSG_KAFKA_BOOTSTRAP" to bootstrap,
                        "XMSG_PARTITIONS" to partitions.toString(),
                        "XMSG_STATE_DIR" to stateDir.absolutePath,
                        "XMSG_INSTANCE_ID" to instanceId,
                        "XMSG_COMMIT_INTERVAL_MS" to "10",
                        "XMSG_SESSION_TIMEOUT_MS" to "6000",
                    ),
                )
                redirectErrorStream(true)
                redirectOutput(ProcessBuilder.Redirect.appendTo(logFile))
            }
            .start()
        return this
    }

    /** SIGKILL: no shutdown hook, no clean close, possibly mid-transaction. */
    fun kill() {
        process?.destroyForcibly()?.waitFor(30, TimeUnit.SECONDS)
    }

    /** SIGTERM: the shutdown hook closes Kafka Streams cleanly. */
    fun stop() {
        process?.let {
            it.destroy()
            if (!it.waitFor(60, TimeUnit.SECONDS)) it.destroyForcibly()
        }
    }

    fun exitCodeWithin(timeout: Duration): Int? =
        process?.let { if (it.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) it.exitValue() else null }

    val isAlive: Boolean get() = process?.isAlive == true
}
