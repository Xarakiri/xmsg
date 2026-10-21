// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import dev.xmsg.proto.v1.ChannelType
import dev.xmsg.proto.v1.RejectReason
import dev.xmsg.proto.v1.SequencedEvent
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Duration
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * State survives replacing the sequencer with a fresh instance (ADR-0003).
 *
 * Version N runs, processes data and stops. A new instance starts on an empty
 * state directory with a different instance ID, as a newly deployed version on
 * a new host would, so it must restore every store from its changelog. Counters,
 * membership and the dedupe window must all carry over.
 *
 * Today both "versions" are built from the current code. Once releases exist,
 * CI runs version N from the previous release's artifact, which also catches
 * accidental renames of stores or processors.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RestoreUpgradeIT {
    private lateinit var harness: KafkaHarness
    private val partitions = 2
    private val channel = 500L
    private val alice = 1L
    private val bob = 2L
    private val mallory = 3L

    @BeforeAll
    fun startKafka() {
        harness = KafkaHarness()
        harness.createTopics(partitions)
    }

    @AfterAll
    fun stopKafka() = harness.close()

    @Test
    fun `a new instance on an empty state directory continues counters, membership and dedupe`() {
        val versionN = SequencerProcess(harness.bootstrap, partitions, "version-n").start()
        harness.send(KafkaHarness.createChannel(channel, alice, ChannelType.CHANNEL_TYPE_PRIVATE, listOf(alice, bob)))
        (1..5).forEach { harness.send(KafkaHarness.message(channel, alice, "before-$it")) }
        val before = harness.readUntil(Duration.ofMinutes(2)) { it.count(SequencedEvent::hasMessageCreated) >= 5 }
        assertEquals(5L, before.last { it.hasMessageCreated() }.msgSeq)
        versionN.stop()

        val versionNext = SequencerProcess(harness.bootstrap, partitions, "version-n-plus-1").start()
        harness.send(KafkaHarness.message(channel, bob, "after-bob")) // member: accepted, msg_seq 6
        harness.send(KafkaHarness.message(channel, mallory, "after-mallory")) // never a member
        harness.send(KafkaHarness.message(channel, alice, "before-5")) // retry inside the dedupe window
        harness.send(KafkaHarness.kick(channel, alice, bob, request = "kick-bob"))
        harness.send(KafkaHarness.message(channel, bob, "after-kick")) // no longer a member

        val all = harness.readUntil(Duration.ofMinutes(3)) { events -> events.any { it.dedupeId == "after-kick" } }
        versionNext.stop()
        val after = all.drop(before.size)

        val bobsMessage = after.single { it.dedupeId == "after-bob" }
        assertEquals(6L, bobsMessage.msgSeq, "msg_seq continues from the restored counter")
        assertEquals(7L, bobsMessage.eventSeq, "event_seq continues (1 create + 5 messages before)")
        assertEquals(
            RejectReason.REJECT_REASON_NOT_A_MEMBER,
            after.single { it.dedupeId == "after-mallory" }.rejected.reason,
            "membership restored: non-members are still rejected",
        )
        assertTrue(after.none { it.dedupeId == "before-5" }, "dedupe window restored: the retry is dropped")
        val left = after.single { it.hasMemberLeft() }
        assertEquals(left.eventSeq, left.membershipVersion)
        assertEquals(RejectReason.REJECT_REASON_NOT_A_MEMBER, after.single { it.dedupeId == "after-kick" }.rejected.reason)

        val changelogs = harness.admin.listTopics().names().get(30, TimeUnit.SECONDS)
        for (store in Names.ALL_STORES) {
            assertTrue(Names.changelogTopic(store) in changelogs, "changelog for $store keeps its fixed name")
        }
    }
}
