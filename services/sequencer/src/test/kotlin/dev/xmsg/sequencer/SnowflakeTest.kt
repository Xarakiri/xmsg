// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SnowflakeTest {
    private val t0 = Snowflake.EPOCH_MS + 1_000_000

    @Test
    fun `ids are unique and increasing within a millisecond and across the counter limit`() {
        var last = 0L
        val seen = HashSet<Long>()
        repeat(20_000) {
            val id = Snowflake.next(last, t0, partition = 7)
            assertTrue(id > last, "ids must increase")
            assertTrue(seen.add(id), "ids must be unique")
            assertEquals(7, Snowflake.partition(id))
            last = id
        }
        // 20k ids in the same event-time millisecond spill into the next milliseconds.
        assertTrue(Snowflake.millis(last) > t0)
    }

    @Test
    fun `event time going backwards never produces a smaller id`() {
        val first = Snowflake.next(0, t0 + 500, partition = 1)
        val second = Snowflake.next(first, t0, partition = 1)
        assertTrue(second > first)
    }
}
