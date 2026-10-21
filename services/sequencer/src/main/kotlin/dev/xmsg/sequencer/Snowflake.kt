// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

/**
 * Message IDs: 41 bits of milliseconds since [EPOCH_MS], 9 bits of partition,
 * 13 bits of counter. The clock is the record's event time, so replays produce
 * the same IDs, and a new partition owner continues from the last issued ID.
 */
object Snowflake {
    const val EPOCH_MS = 1_767_225_600_000L // 2026-01-01T00:00:00Z
    private const val PARTITION_BITS = 9
    private const val COUNTER_BITS = 13
    private const val MAX_COUNTER = (1L shl COUNTER_BITS) - 1

    fun compose(ms: Long, partition: Int, counter: Long): Long {
        require(partition in 0 until (1 shl PARTITION_BITS)) { "partition out of range: $partition" }
        return ((ms - EPOCH_MS) shl (PARTITION_BITS + COUNTER_BITS)) or
            (partition.toLong() shl COUNTER_BITS) or counter
    }

    fun millis(id: Long): Long = (id ushr (PARTITION_BITS + COUNTER_BITS)) + EPOCH_MS

    fun partition(id: Long): Int = ((id ushr COUNTER_BITS) and ((1L shl PARTITION_BITS) - 1)).toInt()

    private fun counter(id: Long): Long = id and MAX_COUNTER

    /** The next ID after [lastId] (0 if none) for an event at [eventTimeMs]. */
    fun next(lastId: Long, eventTimeMs: Long, partition: Int): Long {
        if (lastId == 0L) return compose(maxOf(eventTimeMs, EPOCH_MS), partition, 0)
        val lastMs = millis(lastId)
        return when {
            eventTimeMs > lastMs -> compose(eventTimeMs, partition, 0)
            counter(lastId) < MAX_COUNTER -> compose(lastMs, partition, counter(lastId) + 1)
            else -> compose(lastMs + 1, partition, 0)
        }
    }
}
