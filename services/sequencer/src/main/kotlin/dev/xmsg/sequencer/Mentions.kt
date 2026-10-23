// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

/**
 * Mentions in a message body, written by clients as Slack-style tokens: `<@user_id>` for
 * a person, `<!channel>` or `<!here>` for everyone in the channel (ADR-0012). The
 * sequencer decides who is actually mentioned, since only it knows the members.
 */
object Mentions {
    const val MAX_USERS = 50
    private val user = Regex("<@(\\d{1,19})>")
    private val everyone = Regex("<!(channel|here)>")

    /** Mentioned user IDs in order of first appearance, without repeats. */
    fun users(body: String): List<Long> =
        user.findAll(body).mapNotNull { it.groupValues[1].toLongOrNull() }.distinct().toList()

    fun channel(body: String): Boolean = everyone.containsMatchIn(body)
}
