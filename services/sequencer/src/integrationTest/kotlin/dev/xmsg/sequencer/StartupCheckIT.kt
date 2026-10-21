// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import dev.xmsg.proto.v1.ChannelType
import dev.xmsg.proto.v1.SequencedEvent
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The startup check refuses unsafe starts (ADR-0003, ADR-0021). A fresh broker per test. */
class StartupCheckIT {
    private lateinit var harness: KafkaHarness

    @BeforeEach
    fun startKafka() {
        harness = KafkaHarness()
    }

    @AfterEach
    fun stopKafka() = harness.close()

    private fun problems(expected: Int) = StartupCheck(harness.admin).problems(expected)

    @Test
    fun `missing topics are reported`() {
        val problems = problems(2)
        assertEquals(3, problems.size, problems.toString())
        assertTrue(problems.all { "does not exist" in it })
    }

    @Test
    fun `a partition count different from the installation's is refused`() {
        harness.createTopics(partitions = 2, channelTopicPartitions = 3)
        val problems = problems(2)
        assertEquals(2, problems.size, problems.toString()) // messages.in and messages.persisted
        assertTrue(problems.all { "has 3 partitions" in it && "migrate-partitions" in it })
    }

    @Test
    fun `a fresh installation passes, and so does one with intact state`() {
        harness.createTopics(partitions = 2)
        assertEquals(emptyList(), problems(2))

        runOnce()
        assertEquals(emptyList(), problems(2))
    }

    @Test
    fun `processed data without changelogs is refused, and the process exits with code 2`() {
        harness.createTopics(partitions = 2)
        runOnce()

        val changelogs = Names.ALL_STORES.map(Names::changelogTopic)
        harness.admin.deleteTopics(changelogs).all().get(30, TimeUnit.SECONDS)
        // Topic deletion is asynchronous on the broker.
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (System.nanoTime() < deadline &&
            harness.admin.listTopics().names().get().any { it in changelogs }
        ) {
            Thread.sleep(200)
        }

        val problems = problems(2)
        assertEquals(Names.ALL_STORES.size, problems.size, problems.toString())
        assertTrue(problems.all { "would reset its state" in it })

        val process = SequencerProcess(harness.bootstrap, 2, "refused").start()
        assertEquals(2, process.exitCodeWithin(Duration.ofSeconds(60)), "see ${process.logFile}")
        assertTrue("startup check failed" in process.logFile.readText())
    }

    /** Runs the sequencer until it has processed one channel and one message, then stops it. */
    private fun runOnce() {
        val process = SequencerProcess(harness.bootstrap, 2, "run-once").start()
        harness.send(KafkaHarness.createChannel(77, 1, ChannelType.CHANNEL_TYPE_PUBLIC, listOf(1)))
        harness.send(KafkaHarness.message(77, 1, "hello"))
        val events = harness.readUntil(Duration.ofMinutes(2)) { it.any(SequencedEvent::hasMessageCreated) }
        assertTrue(events.any(SequencedEvent::hasMessageCreated), "sequencer processed the message; see ${process.logFile}")
        process.stop()
    }
}
