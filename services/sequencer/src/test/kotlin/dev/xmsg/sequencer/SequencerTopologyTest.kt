// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import dev.xmsg.proto.v1.ChannelCreated
import dev.xmsg.proto.v1.ChannelType
import dev.xmsg.proto.v1.DeleteMessage
import dev.xmsg.proto.v1.EditMessage
import dev.xmsg.proto.v1.ReactionBatch
import dev.xmsg.proto.v1.ReactionOp
import dev.xmsg.proto.v1.CommandResult
import dev.xmsg.proto.v1.FanoutMode
import dev.xmsg.proto.v1.InboundEvent
import dev.xmsg.proto.v1.MemberJoinRequested
import dev.xmsg.proto.v1.MemberKickRequested
import dev.xmsg.proto.v1.MemberRole
import dev.xmsg.proto.v1.RejectReason
import dev.xmsg.proto.v1.SendMessage
import dev.xmsg.proto.v1.SequencedEvent
import dev.xmsg.proto.v1.ThreadRoot
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TestInputTopic
import org.apache.kafka.streams.TestOutputTopic
import org.apache.kafka.streams.TopologyTestDriver
import java.nio.file.Files
import java.time.Duration
import java.time.Instant
import java.util.Properties
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SequencerTopologyTest {
    private lateinit var driver: TopologyTestDriver
    private lateinit var input: TestInputTopic<Long, ByteArray>
    private lateinit var persisted: TestOutputTopic<Long, ByteArray>
    private lateinit var results: TestOutputTopic<String, ByteArray>
    private var clock = Instant.parse("2026-06-01T12:00:00Z")

    private val workspace = 10L
    private val alice = 1L
    private val bob = 2L
    private val carol = 3L

    @BeforeTest
    fun setUp() = start(FanoutThresholds(pullAt = 4, pushBelow = 3))

    private fun start(thresholds: FanoutThresholds) {
        val props = Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, Names.APPLICATION_ID)
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092")
            put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2)
            put(StreamsConfig.STATE_DIR_CONFIG, Files.createTempDirectory("xmsg-seq").toString())
        }
        driver = TopologyTestDriver(SequencerTopology.build(thresholds), props)
        input = driver.createInputTopic(Names.TOPIC_IN, Serdes.Long().serializer(), Serdes.ByteArray().serializer())
        persisted = driver.createOutputTopic(Names.TOPIC_PERSISTED, Serdes.Long().deserializer(), Serdes.ByteArray().deserializer())
        results = driver.createOutputTopic(Names.TOPIC_COMMAND_RESULTS, Serdes.String().deserializer(), Serdes.ByteArray().deserializer())
    }

    @AfterTest
    fun tearDown() = driver.close()

    // --- helpers -------------------------------------------------------------

    private fun send(event: InboundEvent, advance: Duration = Duration.ofMillis(1)) {
        clock = clock.plus(advance)
        input.pipeInput(event.channelId, event.toByteArray(), clock)
    }

    private fun base(channel: Long, actor: Long, dedupe: String = "", request: String = "") =
        InboundEvent.newBuilder().setChannelId(channel).setWorkspaceId(workspace).setActorId(actor)
            .setDedupeId(dedupe).setRequestId(request)

    private fun create(channel: Long, actor: Long, type: ChannelType, vararg members: Long, dedupe: String = "outbox-$channel") =
        send(base(channel, actor, dedupe = dedupe).setChannelCreated(ChannelCreated.newBuilder().setType(type).addAllInitialMemberIds(members.toList())).build())

    private fun post(channel: Long, actor: Long, body: String, dedupe: String = "msg-$channel-$actor-$body") =
        send(base(channel, actor, dedupe = dedupe).setSendMessage(SendMessage.newBuilder().setBody(body)).build())

    private fun join(channel: Long, actor: Long, user: Long, request: String) =
        send(base(channel, actor, request = request).setMemberJoinRequested(MemberJoinRequested.newBuilder().setUserId(user)).build())

    private fun kick(channel: Long, actor: Long, user: Long, request: String) =
        send(base(channel, actor, request = request).setMemberKickRequested(MemberKickRequested.newBuilder().setUserId(user)).build())

    private fun edit(channel: Long, actor: Long, msgSeq: Long, body: String) =
        send(base(channel, actor, dedupe = "edit-$channel-$msgSeq-$body").setEditMessage(EditMessage.newBuilder().setMsgSeq(msgSeq).setMessageId(msgSeq * 1000).setBody(body)).build())

    private fun delete(channel: Long, actor: Long, msgSeq: Long, asAdmin: Boolean) =
        send(base(channel, actor, dedupe = "delete-$channel-$msgSeq-$actor").setDeleteMessage(DeleteMessage.newBuilder().setMsgSeq(msgSeq).setMessageId(msgSeq * 1000).setAsAdmin(asAdmin)).build())

    private fun react(channel: Long, msgSeq: Long, emoji: String, batchId: String, vararg ops: Triple<Long, Boolean, Long>) =
        send(
            base(channel, 0, dedupe = "rx-$batchId").setReactionBatch(
                ReactionBatch.newBuilder().setMessageId(msgSeq * 1000).setMsgSeq(msgSeq).setEmoji(emoji)
                    .addAllOps(ops.map { (user, add, counter) -> ReactionOp.newBuilder().setUserId(user).setAdd(add).setCounter(counter).build() }),
            ).build(),
        )

    private fun reply(channel: Long, actor: Long, rootSeq: Long, rootId: Long, body: String) =
        send(
            base(channel, actor, dedupe = "reply-$channel-$actor-$body")
                .setSendMessage(SendMessage.newBuilder().setBody(body).setThreadRoot(ThreadRoot.newBuilder().setMsgSeq(rootSeq).setMessageId(rootId)))
                .build(),
        )

    private fun drain(): List<SequencedEvent> = persisted.readValuesToList().map { SequencedEvent.parseFrom(it) }

    private fun drainResults(): Map<String, CommandResult> =
        results.readKeyValuesToMap().mapValues { CommandResult.parseFrom(it.value) }

    // --- tests ---------------------------------------------------------------

    @Test
    fun `messages get consecutive counters per channel, independently per channel`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PUBLIC, bob)
        create(200, alice, ChannelType.CHANNEL_TYPE_PUBLIC)
        post(100, alice, "a1"); post(100, bob, "b1"); post(200, alice, "x1"); post(100, alice, "a2")

        val events = drain().filter { it.hasMessageCreated() }
        val ch100 = events.filter { it.channelId == 100L }
        assertEquals(listOf(1L, 2L, 3L), ch100.map { it.msgSeq })
        assertEquals(listOf(2L, 3L, 4L), ch100.map { it.eventSeq }) // event_seq 1 was channel.created
        assertEquals(listOf(1L), events.filter { it.channelId == 200L }.map { it.msgSeq })
        val ids = events.map { it.messageCreated.messageId }
        assertEquals(ids.sorted(), ids, "message ids increase in processing order")
        assertEquals(ids.size, ids.toSet().size, "message ids are unique")
    }

    @Test
    fun `a retried client_msg_id within 15 minutes of event time is dropped`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PUBLIC)
        post(100, alice, "hello", dedupe = "c-1")
        post(100, alice, "hello", dedupe = "c-1") // immediate retry
        send(base(100, alice, dedupe = "c-1").setSendMessage(SendMessage.newBuilder().setBody("hello")).build(), Duration.ofMinutes(14))
        assertEquals(1, drain().count { it.hasMessageCreated() })

        // Outside the window the same ID is treated as new (the client gave up after 10 minutes, ADR-0011).
        send(base(100, alice, dedupe = "c-1").setSendMessage(SendMessage.newBuilder().setBody("hello")).build(), Duration.ofMinutes(16))
        assertEquals(1, drain().count { it.hasMessageCreated() })
    }

    @Test
    fun `channel_created lists initial members with roles, and every event carries the channel type`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        post(100, bob, "hi")
        val events = drain()
        val created = events.single { it.hasChannelCreated() }.channelCreated
        assertEquals(
            listOf(alice to MemberRole.MEMBER_ROLE_ADMIN, bob to MemberRole.MEMBER_ROLE_MEMBER),
            created.membersList.map { it.userId to it.role },
        )
        assertTrue(events.all { it.channelType == ChannelType.CHANNEL_TYPE_PRIVATE })
    }

    @Test
    fun `duplicate channel_created from fast path and outbox is a no-op`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PUBLIC, bob, dedupe = "outbox-1")
        create(100, alice, ChannelType.CHANNEL_TYPE_PUBLIC, bob, dedupe = "outbox-1") // same outbox event ID
        create(100, alice, ChannelType.CHANNEL_TYPE_PUBLIC, bob, dedupe = "outbox-1-late") // a later replay that the dedupe window no longer covers
        assertEquals(1, drain().count { it.hasChannelCreated() })
    }

    @Test
    fun `posts are rejected for unknown channels and non-members, without taking an event_seq`() {
        post(999, alice, "nobody home")
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE)
        post(100, carol, "let me in")
        post(100, alice, "ok")

        val events = drain()
        val rejected = events.filter { it.hasRejected() }
        assertEquals(
            listOf(RejectReason.REJECT_REASON_UNKNOWN_CHANNEL, RejectReason.REJECT_REASON_NOT_A_MEMBER),
            rejected.map { it.rejected.reason },
        )
        assertTrue(rejected.all { it.eventSeq == 0L })
        assertEquals(2L, events.single { it.hasMessageCreated() }.eventSeq)
    }

    @Test
    fun `mentions keep members only, never the author, and @channel only from admins`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        post(100, alice, "hi <@$bob> <@$carol> <@$alice> <@$bob> <!channel>")
        post(100, bob, "<!here> anyone? <@$alice>")
        val (first, second) = drain().filter { it.hasMessageCreated() }.map { it.messageCreated }
        assertEquals(listOf(bob), first.mentionedUserIdsList, "carol isn't a member; alice is the author; bob once")
        assertTrue(first.mentionsChannel)
        assertEquals(listOf(alice), second.mentionedUserIdsList)
        assertTrue(!second.mentionsChannel, "bob isn't an admin")
        assertEquals(listOf(5L, 6L), Mentions.users("<@5> <@x> <@6> <@5>"))
    }

    @Test
    fun `thread replies take an event_seq and their own reply_seq, never a msg_seq`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        post(100, alice, "plan?")
        val rootId = drain().single { it.hasMessageCreated() }.messageCreated.messageId

        reply(100, bob, 1, rootId, "yes <@$alice>")
        reply(100, alice, 1, rootId, "great")
        reply(100, bob, 1, rootId + 1, "wrong root id")
        reply(100, bob, 9, rootId, "no such root")
        reply(100, carol, 1, rootId, "not a member")
        post(100, bob, "next topic")

        val events = drain()
        val replies = events.filter { it.hasThreadReply() }
        assertEquals(listOf(1L to 1L, 2L to 2L), replies.map { it.threadReply.replySeq to it.threadReply.replyCount })
        assertTrue(replies.all { it.msgSeq == 0L && it.threadReply.rootMsgSeq == 1L && it.threadReply.rootMessageId == rootId })
        assertEquals(listOf(alice), replies.first().threadReply.mentionedUserIdsList)
        assertEquals(listOf(3L, 4L), replies.map { it.eventSeq })
        assertEquals(
            listOf(RejectReason.REJECT_REASON_INVALID, RejectReason.REJECT_REASON_INVALID, RejectReason.REJECT_REASON_NOT_A_MEMBER),
            events.filter { it.hasRejected() }.map { it.rejected.reason },
        )
        val next = events.single { it.hasMessageCreated() }
        assertEquals(5L to 2L, next.eventSeq to next.msgSeq, "replies don't move msg_seq")
    }

    @Test
    fun `reaction batches keep absolute counts and ignore stale, repeated and non-member operations`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        post(100, bob, "lunch?")
        drain()

        react(100, 1, "👍", "1", Triple(bob, true, 1), Triple(alice, true, 1), Triple(carol, true, 1))
        react(100, 1, "👍", "1b", Triple(bob, true, 1)) // a replay: same counter
        react(100, 1, "👍", "2", Triple(bob, false, 0)) // older than what's applied
        react(100, 1, "👍", "3", Triple(bob, false, 2))
        react(100, 7, "👍", "4", Triple(bob, true, 3)) // no such message

        val changes = drain().filter { it.hasReactionsChanged() }
        assertEquals(2, changes.size, "the replay, the stale op and the unknown message emit nothing")
        val (first, second) = changes.map { it.reactionsChanged }
        assertEquals(2L, first.count, "carol isn't a member")
        assertEquals(listOf(bob, alice), first.addedList)
        assertEquals(1L, second.count)
        assertEquals(listOf(bob), second.removedList)
        assertEquals(listOf(3L, 4L), changes.map { it.eventSeq })
        assertTrue(changes.all { it.msgSeq == 0L }, "reactions never count as messages")
    }

    @Test
    fun `edits and deletes take an event_seq but no msg_seq, and are checked`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        post(100, bob, "helo")
        drain()

        edit(100, bob, 1, "hello")
        edit(100, carol, 1, "not mine") // not a member
        edit(100, bob, 7, "no such message")
        delete(100, bob, 1, asAdmin = true) // bob isn't an admin
        delete(100, alice, 1, asAdmin = true)
        post(100, bob, "next")

        val events = drain()
        val edited = events.single { it.hasMessageEdited() }
        assertEquals(3L, edited.eventSeq)
        assertEquals(0L, edited.msgSeq)
        assertEquals(1L, edited.messageEdited.msgSeq)
        assertEquals("hello", edited.messageEdited.body)
        assertTrue(edited.messageEdited.editedAtMs > 0)
        assertEquals(
            listOf(RejectReason.REJECT_REASON_NOT_A_MEMBER, RejectReason.REJECT_REASON_INVALID, RejectReason.REJECT_REASON_FORBIDDEN),
            events.filter { it.hasRejected() }.map { it.rejected.reason },
        )
        val deleted = events.single { it.hasMessageDeleted() }
        assertEquals(4L, deleted.eventSeq)
        assertEquals(alice, deleted.actorId)
        // The next message continues msg_seq where it was: edits and deletes don't count.
        val next = events.single { it.hasMessageCreated() }
        assertEquals(5L to 2L, next.eventSeq to next.msgSeq)
    }

    @Test
    fun `nothing sequenced after a removal is accepted from the removed member`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        post(100, bob, "before")
        kick(100, alice, bob, request = "r-kick")
        post(100, bob, "after")

        val events = drain()
        val left = events.single { it.hasMemberLeft() }
        assertEquals(left.eventSeq, left.membershipVersion, "membership_version moves to the change's event_seq")
        assertEquals(RejectReason.REJECT_REASON_NOT_A_MEMBER, events.last().rejected.reason)
        assertTrue(drainResults().getValue("r-kick").accepted)
    }

    @Test
    fun `membership commands are validated by the sequencer`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        create(200, alice, ChannelType.CHANNEL_TYPE_PUBLIC)
        join(100, carol, carol, request = "self-join-private")  // not allowed
        join(200, carol, carol, request = "self-join-public")   // allowed
        kick(100, bob, alice, request = "member-kicks-admin")   // bob isn't an admin
        join(100, bob, carol, request = "member-invites")       // members may invite to private channels
        join(100, bob, carol, request = "invite-again")         // already a member: accepted, no event

        val r = drainResults()
        assertFalse(r.getValue("self-join-private").accepted)
        assertEquals(RejectReason.REJECT_REASON_FORBIDDEN, r.getValue("self-join-private").reason)
        assertTrue(r.getValue("self-join-public").accepted)
        assertEquals(RejectReason.REJECT_REASON_FORBIDDEN, r.getValue("member-kicks-admin").reason)
        assertTrue(r.getValue("member-invites").accepted)
        assertTrue(r.getValue("invite-again").accepted)
        assertEquals(2, drain().count { it.hasMemberJoined() })
    }

    @Test
    fun `fan-out mode switches with hysteresis and is announced in order`() {
        // Thresholds in this test: pull at 4 members, back to push below 3.
        create(100, alice, ChannelType.CHANNEL_TYPE_PUBLIC, bob) // 2 members
        join(100, carol, carol, request = "j3")                  // 3
        join(100, 4, 4, request = "j4")                          // 4 -> pull
        kick(100, 4, 4, request = "l4")                          // 3, stays pull
        kick(100, carol, carol, request = "l3")                  // 2 -> push

        val events = drain()
        val modes = events.filter { it.hasChannelModeChanged() }.map { it.channelModeChanged.mode }
        assertEquals(listOf(FanoutMode.FANOUT_MODE_PULL, FanoutMode.FANOUT_MODE_PUSH), modes)
        val seqs = events.map { it.eventSeq }
        assertEquals((1L..seqs.size).toList(), seqs, "every event, including mode changes, takes the next event_seq")
    }

    @Test
    fun `broadcast channels accept posts only from posters and admins`() {
        create(100, alice, ChannelType.CHANNEL_TYPE_BROADCAST, bob)
        post(100, bob, "can I post?")
        post(100, alice, "announcement")
        val events = drain()
        assertEquals(RejectReason.REJECT_REASON_FORBIDDEN, events.single { it.hasRejected() }.rejected.reason)
        assertEquals("announcement", events.single { it.hasMessageCreated() }.messageCreated.body)
    }

    @Test
    fun `store and changelog names are the fixed contract`() {
        val description = SequencerTopology.build().describe().toString()
        for (name in listOf(Names.SOURCE, Names.PROCESSOR, Names.SINK_PERSISTED, Names.SINK_COMMAND_RESULTS)) {
            assertTrue(name in description, "processor $name must keep its name")
        }
        for (store in Names.ALL_STORES + Names.ADDED_STORES) {
            assertTrue(store in description, "store $store must keep its name")
        }
        assertEquals("xmsg-sequencer-members-changelog", Names.changelogTopic(Names.STORE_MEMBERS))
    }

    @Test
    fun `the membership endpoint reads members and version straight from the stores`() {
        create(500, alice, ChannelType.CHANNEL_TYPE_PRIVATE, bob)
        create(5000, alice, ChannelType.CHANNEL_TYPE_PRIVATE) // shares the "500" prefix, not "500:"
        join(500, alice, carol, request = "r-join")
        post(500, bob, "hi")
        val heads = driver.getKeyValueStore<Long, ByteArray>(Names.STORE_CHANNEL_HEADS)
        val members = driver.getKeyValueStore<String, ByteArray>(Names.STORE_MEMBERS)

        val found = membersOf(heads, members, 500)!!
        assertEquals(listOf(alice, bob, carol), found.userIdsList)
        assertEquals(2L, found.membershipVersion, "the join (event 2), not the later message (event 3)")
        assertEquals(listOf(alice), membersOf(heads, members, 5000)!!.userIdsList)
        assertEquals(null, membersOf(heads, members, 404))
    }
}
