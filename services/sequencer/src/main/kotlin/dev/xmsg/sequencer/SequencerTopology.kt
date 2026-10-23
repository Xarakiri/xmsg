// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.state.Stores
import java.time.Duration

/** Thresholds for push/pull fan-out with hysteresis (ADR-0008). */
data class FanoutThresholds(val pullAt: Long = 1_000, val pushBelow: Long = 800) {
    init {
        require(pushBelow < pullAt) { "pushBelow must be below pullAt" }
    }
}

object SequencerTopology {
    val DEDUPE_WINDOW: Duration = Duration.ofMinutes(15)

    fun build(thresholds: FanoutThresholds = FanoutThresholds()): Topology {
        val topology = Topology()
        topology.addSource(
            Names.SOURCE,
            Serdes.Long().deserializer(),
            Serdes.ByteArray().deserializer(),
            Names.TOPIC_IN,
        )
        topology.addProcessor(Names.PROCESSOR, { SequencerProcessor(thresholds) }, Names.SOURCE)

        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Names.STORE_CHANNEL_HEADS),
                Serdes.Long(),
                Serdes.ByteArray(),
            ),
            Names.PROCESSOR,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Names.STORE_MEMBERS),
                Serdes.String(),
                Serdes.ByteArray(),
            ),
            Names.PROCESSOR,
        )
        topology.addStateStore(
            Stores.windowStoreBuilder(
                Stores.persistentWindowStore(
                    Names.STORE_DEDUPE,
                    DEDUPE_WINDOW.plusMinutes(1), // retention
                    Duration.ofMillis(1), // window size: one entry per event time
                    false,
                ),
                Serdes.String(),
                Serdes.Long(),
            ),
            Names.PROCESSOR,
        )
        topology.addStateStore(
            Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(Names.STORE_PARTITION_STATE),
                Serdes.Integer(),
                Serdes.ByteArray(),
            ),
            Names.PROCESSOR,
        )

        for (store in listOf(Names.STORE_REACTIONS, Names.STORE_REACTION_COUNTS, Names.STORE_THREADS)) {
            topology.addStateStore(
                Stores.keyValueStoreBuilder(
                    Stores.persistentKeyValueStore(store),
                    Serdes.String(),
                    Serdes.ByteArray(),
                ),
                Names.PROCESSOR,
            )
        }

        topology.addSink(
            Names.SINK_PERSISTED,
            Names.TOPIC_PERSISTED,
            Serdes.Long().serializer(),
            Serdes.ByteArray().serializer(),
            Names.PROCESSOR,
        )
        topology.addSink(
            Names.SINK_COMMAND_RESULTS,
            Names.TOPIC_COMMAND_RESULTS,
            Serdes.String().serializer(),
            Serdes.ByteArray().serializer(),
            Names.PROCESSOR,
        )
        return topology
    }
}
