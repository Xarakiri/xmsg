// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import dev.xmsg.proto.v1.ChannelType
import dev.xmsg.proto.v1.SequencedEvent
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import kotlin.concurrent.thread
import kotlin.random.Random
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Phase 0 spike: exactly-once ordering survives crashes (ADR-0002, ADR-0003).
 *
 * Two sequencer processes share 4 partitions while messages stream in. The test
 * SIGKILLs one of them repeatedly, sometimes restarting it at once (its new
 * transactional producer fences the dead one's open transaction) and sometimes
 * after the session timeout (its partitions move to the other process, which
 * restores state from the changelogs, then move back).
 *
 * Pass criteria, checked on read_committed output:
 * - every produced message appears exactly once (none lost, none duplicated),
 * - per channel, msg_seq is exactly 1..n and event_seq has no gaps or repeats,
 * - message IDs are unique, and nothing was rejected.
 *
 * Intensity: -Dxmsg.crash.kills=N (default 12 in CI; raise for soak runs).
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CrashFencingIT {
    private lateinit var harness: KafkaHarness
    private val partitions = 4
    private val channels = (1L..8L).map { 1_000L + it }
    private val members = (1L..5L).toList()
    private val messageCount = 3_000

    @BeforeAll
    fun startKafka() {
        harness = KafkaHarness()
        harness.createTopics(partitions)
    }

    @AfterAll
    fun stopKafka() = harness.close()

    @Test
    fun `no message is lost or duplicated while sequencers are killed mid-commit`() {
        val kills = System.getProperty("xmsg.crash.kills").toInt()
        val random = Random(42)

        channels.forEach { harness.send(KafkaHarness.createChannel(it, members.first(), ChannelType.CHANNEL_TYPE_PRIVATE, members)) }

        val sequencers = listOf("a", "b").map { SequencerProcess(harness.bootstrap, partitions, "crash-$it").start() }

        // Traffic: ~100 messages/s for ~30 s, so kills land while transactions are open.
        // Every 10th message is sent twice with the same dedupe ID, like a client retry.
        val expected = (1..messageCount).map { "m-$it" }
        val producer = thread(name = "traffic") {
            val trafficRandom = Random(7) // kotlin.random.Random isn't thread-safe; don't share it
            expected.forEachIndexed { i, id ->
                val event = KafkaHarness.message(channels[trafficRandom.nextInt(channels.size)], members.random(trafficRandom), id)
                harness.sendAsync(event)
                if (i % 10 == 0) harness.sendAsync(event)
                if (i % 50 == 0) Thread.sleep(500)
            }
            harness.flush()
        }

        var performed = 0
        repeat(kills) { k ->
            Thread.sleep(1_500L + random.nextLong(2_000))
            val victim = sequencers[random.nextInt(sequencers.size)]
            victim.kill()
            performed++
            // Every third kill waits past the session timeout, forcing a rebalance and a restore.
            if (k % 3 == 2) Thread.sleep(8_000)
            victim.start()
        }
        producer.join()

        val events = harness.readUntil(Duration.ofMinutes(4)) { read ->
            read.count { it.hasMessageCreated() } >= expected.size
        }
        sequencers.forEach { it.stop() }

        assertEquals(kills, performed)
        verify(events, expected)
    }

    private fun verify(events: List<SequencedEvent>, expected: List<String>) {
        val rejected = events.filter { it.hasRejected() }
        assertTrue(rejected.isEmpty(), "unexpected rejections: ${rejected.take(3)}")

        val messages = events.filter { it.hasMessageCreated() }
        val ids = messages.map { it.dedupeId }
        val duplicates = ids.groupingBy { it }.eachCount().filterValues { it > 1 }
        if (duplicates.isNotEmpty()) fail("messages delivered more than once: ${duplicates.entries.take(5)}")
        val missing = expected.toSet() - ids.toSet()
        if (missing.isNotEmpty()) fail("${missing.size} messages lost, e.g. ${missing.take(5)}")
        assertEquals(expected.size, messages.size)

        for ((channel, channelEvents) in events.groupBy { it.channelId }) {
            val msgSeqs = channelEvents.filter { it.hasMessageCreated() }.map { it.msgSeq }
            assertEquals((1L..msgSeqs.size).toList(), msgSeqs, "msg_seq for channel $channel")
            val eventSeqs = channelEvents.map { it.eventSeq }
            assertEquals((1L..eventSeqs.size).toList(), eventSeqs, "event_seq for channel $channel")
        }

        val messageIds = messages.map { it.messageCreated.messageId }
        assertEquals(messageIds.size, messageIds.toSet().size, "message IDs must be unique")
    }
}
