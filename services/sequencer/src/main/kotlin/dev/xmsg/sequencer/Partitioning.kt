// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.utils.Utils

/** Kafka's default partitioning for channel IDs (spec/partitioning, ADR-0004, ADR-0021). */
object Partitioning {
    private val serializer = LongSerializer()

    fun channelKey(channelId: Long): ByteArray = serializer.serialize("", channelId)

    fun partition(channelId: Long, partitionCount: Int): Int {
        require(partitionCount > 0) { "partitionCount must be positive" }
        return Utils.toPositive(Utils.murmur2(channelKey(channelId))) % partitionCount
    }
}
