// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import org.apache.kafka.common.utils.Utils
import java.io.File
import java.util.HexFormat
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Cross-checks spec/partitioning/vectors.csv against Kafka's own Utils.murmur2,
 * so the generator script is never the only source of truth.
 */
class PartitioningTest {
    private val rows = File(System.getProperty("xmsg.vectors")).readLines().filter { it.isNotBlank() }
    private val counts = rows.first().split(",").drop(3).map { it.removePrefix("p").toInt() }

    @Test
    fun `vectors match Kafka's partitioner`() {
        val hex = HexFormat.of()
        for (row in rows.drop(1)) {
            val cols = row.split(",")
            val channelId = cols[0].toLong()
            val key = Partitioning.channelKey(channelId)
            assertEquals(cols[1], hex.formatHex(key), "key bytes for $channelId")
            assertEquals(cols[2].toInt(), Utils.murmur2(key), "Kafka murmur2 for $channelId")
            counts.forEachIndexed { i, n ->
                val expected = cols[3 + i].toInt()
                assertEquals(expected, Utils.toPositive(Utils.murmur2(key)) % n, "Kafka partition($channelId, $n)")
                assertEquals(expected, Partitioning.partition(channelId, n), "partition($channelId, $n)")
            }
        }
        assertTrue(rows.size > 200)
    }
}
