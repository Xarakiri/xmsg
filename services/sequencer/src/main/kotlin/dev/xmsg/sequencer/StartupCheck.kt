// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.common.TopicPartition
import java.util.concurrent.TimeUnit

/**
 * Checks run before the sequencer starts (ADR-0003, ADR-0021). Any problem
 * stops the process, because starting would either break ordering (wrong
 * partition count) or silently reset counters (missing changelogs).
 */
class StartupCheck(private val admin: Admin, private val timeoutSeconds: Long = 30) {

    /** Returns a list of problems; empty means it's safe to start. */
    fun problems(expectedPartitions: Int): List<String> {
        val problems = mutableListOf<String>()
        val existing = admin.listTopics().names().get(timeoutSeconds, TimeUnit.SECONDS)

        // 1. Channel-keyed topics exist with this installation's fixed partition count.
        val channelTopics = listOf(Names.TOPIC_IN, Names.TOPIC_PERSISTED)
        val missing = channelTopics.filterNot { it in existing }
        missing.forEach { problems += "topic $it does not exist; run the topic setup first" }
        val present = channelTopics - missing.toSet()
        if (present.isNotEmpty()) {
            val described = admin.describeTopics(present).allTopicNames().get(timeoutSeconds, TimeUnit.SECONDS)
            for ((name, description) in described) {
                val actual = description.partitions().size
                if (actual != expectedPartitions) {
                    problems += "topic $name has $actual partitions but this installation is configured for " +
                        "$expectedPartitions; never change the count in place, use xmsgctl migrate-partitions (ADR-0021)"
                }
            }
        }
        if (Names.TOPIC_COMMAND_RESULTS !in existing) {
            problems += "topic ${Names.TOPIC_COMMAND_RESULTS} does not exist; run the topic setup first"
        }

        // 2. An installation that has processed data must still have its state changelogs.
        if (hasProcessedData()) {
            for (store in Names.ALL_STORES) {
                val topic = Names.changelogTopic(store)
                if (topic !in existing) {
                    problems += "changelog $topic is missing although the sequencer has processed data; " +
                        "starting now would reset its state (check the application.id and store names, ADR-0003)"
                }
            }
            val heads = Names.changelogTopic(Names.STORE_CHANNEL_HEADS)
            if (heads in existing && totalEndOffset(heads) == 0L) {
                problems += "changelog $heads is empty although the sequencer has processed data"
            }
        }
        return problems
    }

    private fun hasProcessedData(): Boolean {
        val committed = admin.listConsumerGroupOffsets(Names.APPLICATION_ID)
            .partitionsToOffsetAndMetadata()
            .get(timeoutSeconds, TimeUnit.SECONDS)
        return committed.values.any { it != null && it.offset() > 0 }
    }

    private fun totalEndOffset(topic: String): Long {
        val partitions = admin.describeTopics(listOf(topic)).allTopicNames().get(timeoutSeconds, TimeUnit.SECONDS)
            .getValue(topic).partitions().map { TopicPartition(topic, it.partition()) }
        return admin.listOffsets(partitions.associateWith { OffsetSpec.latest() })
            .all().get(timeoutSeconds, TimeUnit.SECONDS)
            .values.sumOf { it.offset() }
    }
}
