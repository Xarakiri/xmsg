// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class AppTest {
    @Test
    fun `fan-out thresholds default to ADR-0008's and can be lowered for tests`() {
        assertEquals(FanoutThresholds(1_000, 800), fanoutThresholds(emptyMap()))
        assertEquals(
            FanoutThresholds(2, 1),
            fanoutThresholds(mapOf("XMSG_FANOUT_PULL_AT" to "2", "XMSG_FANOUT_PUSH_BELOW" to "1")),
        )
        assertFailsWith<IllegalArgumentException> {
            fanoutThresholds(mapOf("XMSG_FANOUT_PULL_AT" to "5", "XMSG_FANOUT_PUSH_BELOW" to "5"))
        }
    }
}
