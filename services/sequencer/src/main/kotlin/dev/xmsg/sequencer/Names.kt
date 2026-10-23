// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

/**
 * Every processor, store and topic name, fixed explicitly (ADR-0003).
 *
 * These names are a versioned contract: changelog topics are named
 * "<application.id>-<store>-changelog", so renaming a store would make an
 * upgraded sequencer start with empty state. Changing any value here needs
 * a migration step and an ADR.
 */
object Names {
    const val APPLICATION_ID = "xmsg-sequencer"

    const val TOPIC_IN = "messages.in"
    const val TOPIC_PERSISTED = "messages.persisted"
    const val TOPIC_COMMAND_RESULTS = "commands.results"

    const val SOURCE = "xmsg-source-messages-in"
    const val PROCESSOR = "xmsg-sequencer-processor"
    const val SINK_PERSISTED = "xmsg-sink-messages-persisted"
    const val SINK_COMMAND_RESULTS = "xmsg-sink-commands-results"

    const val STORE_CHANNEL_HEADS = "channel-heads"
    const val STORE_MEMBERS = "members"
    const val STORE_DEDUPE = "dedupe"
    const val STORE_PARTITION_STATE = "partition-state"

    /** Phase 2 (ADR-0015): reactions per (channel, message, emoji, user) and their counts. */
    const val STORE_REACTIONS = "reactions"
    const val STORE_REACTION_COUNTS = "reaction-counts"

    /** Phase 2 (ADR-0005): reply counts per thread root. */
    const val STORE_THREADS = "threads"

    /** Stores every installation that has processed data must have changelogs for. */
    val ALL_STORES = setOf(STORE_CHANNEL_HEADS, STORE_MEMBERS, STORE_DEDUPE, STORE_PARTITION_STATE)

    /**
     * Stores added after the first release. An installation upgrading to the version that
     * adds one has no changelog for it yet and starts it empty, which is correct (nothing
     * was ever in it), so the startup check doesn't require them. Same names contract.
     */
    val ADDED_STORES = setOf(STORE_REACTIONS, STORE_REACTION_COUNTS, STORE_THREADS)

    fun changelogTopic(store: String) = "$APPLICATION_ID-$store-changelog"
}
