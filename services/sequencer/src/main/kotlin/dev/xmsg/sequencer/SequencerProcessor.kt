// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import dev.xmsg.proto.v1.ChannelHeadState
import dev.xmsg.proto.v1.ChannelModeChanged
import dev.xmsg.proto.v1.ChannelType
import dev.xmsg.proto.v1.CommandResult
import dev.xmsg.proto.v1.FanoutMode
import dev.xmsg.proto.v1.InboundEvent
import dev.xmsg.proto.v1.MemberJoined
import dev.xmsg.proto.v1.MemberLeft
import dev.xmsg.proto.v1.MemberRole
import dev.xmsg.proto.v1.MemberRoleChanged
import dev.xmsg.proto.v1.MemberState
import dev.xmsg.proto.v1.MessageCreated
import dev.xmsg.proto.v1.MessageDeleted
import dev.xmsg.proto.v1.MessageEdited
import dev.xmsg.proto.v1.PartitionState
import dev.xmsg.proto.v1.ReactionCountState
import dev.xmsg.proto.v1.ReactionState
import dev.xmsg.proto.v1.ReactionsChanged
import dev.xmsg.proto.v1.RejectReason
import dev.xmsg.proto.v1.Rejected
import dev.xmsg.proto.v1.SequencedEvent
import dev.xmsg.proto.v1.ThreadReply
import dev.xmsg.proto.v1.ThreadState
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.WindowStore
import java.time.Instant

private const val STATE_FORMAT = 1

/**
 * Assigns per-channel order (ADR-0002, ADR-0005), enforces and validates
 * membership (ADR-0006), decides the fan-out mode (ADR-0008) and issues
 * message IDs. One instance per stream task, i.e. per partition.
 */
class SequencerProcessor(
    private val thresholds: FanoutThresholds,
) : Processor<Long, ByteArray, Any, ByteArray> {
    private lateinit var context: ProcessorContext<Any, ByteArray>
    private lateinit var heads: KeyValueStore<Long, ByteArray>
    private lateinit var members: KeyValueStore<String, ByteArray>
    private lateinit var dedupe: WindowStore<String, Long>
    private lateinit var partitionState: KeyValueStore<Int, ByteArray>
    private lateinit var reactions: KeyValueStore<String, ByteArray>
    private lateinit var reactionCounts: KeyValueStore<String, ByteArray>
    private lateinit var threads: KeyValueStore<String, ByteArray>

    override fun init(context: ProcessorContext<Any, ByteArray>) {
        this.context = context
        heads = context.getStateStore(Names.STORE_CHANNEL_HEADS)
        members = context.getStateStore(Names.STORE_MEMBERS)
        dedupe = context.getStateStore(Names.STORE_DEDUPE)
        partitionState = context.getStateStore(Names.STORE_PARTITION_STATE)
        reactions = context.getStateStore(Names.STORE_REACTIONS)
        reactionCounts = context.getStateStore(Names.STORE_REACTION_COUNTS)
        threads = context.getStateStore(Names.STORE_THREADS)
    }

    override fun process(record: Record<Long, ByteArray>) {
        val event = try {
            InboundEvent.parseFrom(record.value())
        } catch (e: com.google.protobuf.InvalidProtocolBufferException) {
            return // Malformed input can't be attributed to anyone; a DLQ comes later.
        }
        val channelId = record.key()
        val now = record.timestamp()

        if (channelId == null || channelId != event.channelId) {
            reject(event, now, RejectReason.REJECT_REASON_INVALID, "record key does not match channel_id")
            return
        }
        if (isDuplicate(event, now)) return

        when (event.payloadCase) {
            InboundEvent.PayloadCase.CHANNEL_CREATED -> createChannel(event, now)
            InboundEvent.PayloadCase.SEND_MESSAGE -> sendMessage(event, now)
            InboundEvent.PayloadCase.MEMBER_JOIN_REQUESTED -> join(event, now)
            InboundEvent.PayloadCase.MEMBER_KICK_REQUESTED -> kick(event, now)
            InboundEvent.PayloadCase.MEMBER_ROLE_CHANGE_REQUESTED -> changeRole(event, now)
            InboundEvent.PayloadCase.REACTION_BATCH -> reactionBatch(event, now)
            InboundEvent.PayloadCase.EDIT_MESSAGE -> editMessage(event, now)
            InboundEvent.PayloadCase.DELETE_MESSAGE -> deleteMessage(event, now)
            InboundEvent.PayloadCase.CALL_EVENT -> callEvent(event, now)
            else -> reject(event, now, RejectReason.REJECT_REASON_INVALID, "empty payload")
        }
    }

    // Dedupe by event time (the Kafka record timestamp), not processing time, so
    // retries stay deduplicated even after a long sequencer outage (review #11).
    private fun isDuplicate(event: InboundEvent, now: Long): Boolean {
        if (event.dedupeId.isEmpty()) return false
        val key = "${event.channelId}:${event.dedupeId}"
        val from = Instant.ofEpochMilli(now).minus(SequencerTopology.DEDUPE_WINDOW)
        dedupe.backwardFetch(key, from, Instant.ofEpochMilli(now)).use { seen ->
            if (seen.hasNext()) return true
        }
        dedupe.put(key, now, now)
        return false
    }

    private fun createChannel(event: InboundEvent, now: Long) {
        if (heads.get(event.channelId) != null) {
            // Fast path and outbox copies of the same creation: a no-op (ADR-0010).
            result(event, accepted = true, eventSeq = head(event.channelId)!!.eventSeq)
            return
        }
        val created = event.channelCreated
        val initial = created.initialMemberIdsList.toSet() + event.actorId
        for (userId in initial) {
            val role = if (userId == event.actorId) MemberRole.MEMBER_ROLE_ADMIN else MemberRole.MEMBER_ROLE_MEMBER
            members.put(memberKey(event.channelId, userId), memberState(role))
        }
        val head = ChannelHeadState.newBuilder()
            .setFormatVersion(STATE_FORMAT)
            .setWorkspaceId(event.workspaceId)
            .setType(created.type)
            .setEventSeq(1)
            .setMembershipVersion(1)
            .setMemberCount(initial.size.toLong())
            .setFanoutMode(
                if (created.type == ChannelType.CHANNEL_TYPE_BROADCAST || initial.size >= thresholds.pullAt) {
                    FanoutMode.FANOUT_MODE_PULL
                } else {
                    FanoutMode.FANOUT_MODE_PUSH
                },
            )
            .build()
        heads.put(event.channelId, head.toByteArray())
        val members = initial.sorted().map { userId ->
            MemberJoined.newBuilder()
                .setUserId(userId)
                .setRole(if (userId == event.actorId) MemberRole.MEMBER_ROLE_ADMIN else MemberRole.MEMBER_ROLE_MEMBER)
                .build()
        }
        emit(event, head, now) {
            it.setChannelCreated(
                created.toBuilder().clearInitialMemberIds().addAllInitialMemberIds(initial.sorted()).addAllMembers(members),
            )
        }
        result(event, accepted = true, eventSeq = head.eventSeq)
    }

    private fun sendMessage(event: InboundEvent, now: Long) {
        val head = head(event.channelId)
            ?: return reject(event, now, RejectReason.REJECT_REASON_UNKNOWN_CHANNEL, "unknown channel")
        val role = role(event.channelId, event.actorId)
            ?: return reject(event, now, RejectReason.REJECT_REASON_NOT_A_MEMBER, "not a member")
        if (head.type == ChannelType.CHANNEL_TYPE_BROADCAST &&
            role != MemberRole.MEMBER_ROLE_POSTER && role != MemberRole.MEMBER_ROLE_ADMIN
        ) {
            return reject(event, now, RejectReason.REJECT_REASON_FORBIDDEN, "only posters can post in broadcast channels")
        }
        val body = event.sendMessage.body
        val mentioned = Mentions.users(body)
            .filter { it != event.actorId && role(event.channelId, it) != null }
            .take(Mentions.MAX_USERS)
        if (event.sendMessage.hasThreadRoot()) return threadReply(event, head, now, mentioned)
        val messageId = nextMessageId(now)
        val updated = head.toBuilder().setEventSeq(head.eventSeq + 1).setMsgSeq(head.msgSeq + 1).build()
        heads.put(event.channelId, updated.toByteArray())
        // @channel / @here: admin-only, and never in broadcast channels (plan §6.7).
        val channelWide = Mentions.channel(body) &&
            head.type != ChannelType.CHANNEL_TYPE_BROADCAST &&
            role == MemberRole.MEMBER_ROLE_ADMIN
        emit(event, updated, now) {
            it.setMessageCreated(
                MessageCreated.newBuilder()
                    .setMessageId(messageId)
                    .setBody(body)
                    .addAllMentionedUserIds(mentioned)
                    .setMentionsChannel(channelWide)
                    // Checked and filled in by the messages service (ADR-0024).
                    .addAllAttachments(event.sendMessage.attachmentsList),
            )
        }
    }

    /**
     * A thread reply (ADR-0005): an event_seq and the thread's next reply_seq, no msg_seq.
     * The root must be a sequenced message; its message_id is pinned by the first reply, so
     * a later reply naming another one is refused (the persister also checks the root row
     * exists before touching it).
     */
    private fun threadReply(event: InboundEvent, head: ChannelHeadState, now: Long, mentioned: List<Long>) {
        val root = event.sendMessage.threadRoot
        if (root.msgSeq < 1 || root.msgSeq > head.msgSeq) {
            return reject(event, now, RejectReason.REJECT_REASON_INVALID, "no message ${root.msgSeq} to reply to")
        }
        val key = "${event.channelId}:${root.msgSeq}"
        val thread = threads.get(key)?.let { ThreadState.parseFrom(it) }
        if (thread != null && thread.rootMessageId != root.messageId) {
            return reject(event, now, RejectReason.REJECT_REASON_INVALID, "message ${root.msgSeq} has another id")
        }
        val replySeq = (thread?.replyCount ?: 0L) + 1
        threads.put(
            key,
            ThreadState.newBuilder().setFormatVersion(STATE_FORMAT).setRootMessageId(root.messageId).setReplyCount(replySeq)
                .build().toByteArray(),
        )
        val messageId = nextMessageId(now)
        val updated = head.toBuilder().setEventSeq(head.eventSeq + 1).build()
        heads.put(event.channelId, updated.toByteArray())
        emit(event, updated, now) {
            it.setThreadReply(
                ThreadReply.newBuilder()
                    .setMessageId(messageId)
                    .setBody(event.sendMessage.body)
                    .setRootMsgSeq(root.msgSeq)
                    .setRootMessageId(root.messageId)
                    .setReplySeq(replySeq)
                    .setReplyCount(replySeq)
                    .addAllMentionedUserIds(mentioned)
                    // Checked and filled in by the messages service (ADR-0024).
                    .addAllAttachments(event.sendMessage.attachmentsList),
            )
        }
    }

    /**
     * Edits and deletes take an event_seq but no msg_seq. The messages service has checked
     * authorship against the stored row; here the actor must still be a member and the
     * target must exist, and a delete of someone else's message needs the admin role.
     */
    private fun editMessage(event: InboundEvent, now: Long) {
        val edit = event.editMessage
        val head = targetChannel(event, now, edit.msgSeq) ?: return
        val updated = head.toBuilder().setEventSeq(head.eventSeq + 1).build()
        heads.put(event.channelId, updated.toByteArray())
        emit(event, updated, now) {
            it.setMessageEdited(
                MessageEdited.newBuilder()
                    .setMsgSeq(edit.msgSeq)
                    .setMessageId(edit.messageId)
                    .setBody(edit.body)
                    .setEditedAtMs(now),
            )
        }
    }

    private fun deleteMessage(event: InboundEvent, now: Long) {
        val delete = event.deleteMessage
        val head = targetChannel(event, now, delete.msgSeq) ?: return
        if (delete.asAdmin && role(event.channelId, event.actorId) != MemberRole.MEMBER_ROLE_ADMIN) {
            return reject(event, now, RejectReason.REJECT_REASON_FORBIDDEN, "only admins delete others' messages")
        }
        val updated = head.toBuilder().setEventSeq(head.eventSeq + 1).build()
        heads.put(event.channelId, updated.toByteArray())
        emit(event, updated, now) {
            it.setMessageDeleted(MessageDeleted.newBuilder().setMsgSeq(delete.msgSeq).setMessageId(delete.messageId))
        }
    }

    /**
     * A call's change (ADR-0025), from the calls service, which checked membership when it
     * let people join. Ordered like any channel event: an event_seq, no msg_seq.
     */
    private fun callEvent(event: InboundEvent, now: Long) {
        val head = head(event.channelId)
            ?: return reject(event, now, RejectReason.REJECT_REASON_UNKNOWN_CHANNEL, "unknown channel")
        val updated = head.toBuilder().setEventSeq(head.eventSeq + 1).build()
        heads.put(event.channelId, updated.toByteArray())
        emit(event, updated, now) { it.setCallEvent(event.callEvent) }
    }

    /** The channel's head if the actor is a member and `msgSeq` names a sequenced message; else rejects. */
    private fun targetChannel(event: InboundEvent, now: Long, msgSeq: Long): ChannelHeadState? {
        val head = head(event.channelId)
            ?: return null.also { reject(event, now, RejectReason.REJECT_REASON_UNKNOWN_CHANNEL, "unknown channel") }
        role(event.channelId, event.actorId)
            ?: return null.also { reject(event, now, RejectReason.REJECT_REASON_NOT_A_MEMBER, "not a member") }
        if (msgSeq < 1 || msgSeq > head.msgSeq) {
            return null.also { reject(event, now, RejectReason.REJECT_REASON_INVALID, "no message $msgSeq") }
        }
        return head
    }

    /**
     * A reaction batch (ADR-0015). Each user's operation applies only if its counter is
     * above the last applied one and the user is still a member, so a replayed or late
     * batch changes nothing. The count is kept absolute; nothing is emitted if no
     * reaction actually changed.
     */
    private fun reactionBatch(event: InboundEvent, now: Long) {
        val batch = event.reactionBatch
        val head = head(event.channelId) ?: return // unknown channel: nobody to tell
        if (batch.msgSeq < 1 || batch.msgSeq > head.msgSeq || batch.emoji.isEmpty()) return
        val countKey = "${event.channelId}:${batch.messageId}:${batch.emoji}"
        var count = reactionCounts.get(countKey)?.let { ReactionCountState.parseFrom(it).count } ?: 0L
        val added = mutableListOf<Long>()
        val removed = mutableListOf<Long>()
        for (op in batch.opsList) {
            if (role(event.channelId, op.userId) == null) continue
            val key = "$countKey:${op.userId}"
            val current = reactions.get(key)?.let { ReactionState.parseFrom(it) }
            if (current != null && op.counter <= current.counter) continue
            val was = current?.present ?: false
            reactions.put(
                key,
                ReactionState.newBuilder().setFormatVersion(STATE_FORMAT).setCounter(op.counter).setPresent(op.add)
                    .build().toByteArray(),
            )
            if (op.add && !was) {
                count++
                added += op.userId
            } else if (!op.add && was) {
                count--
                removed += op.userId
            }
        }
        if (added.isEmpty() && removed.isEmpty()) return
        reactionCounts.put(countKey, ReactionCountState.newBuilder().setFormatVersion(STATE_FORMAT).setCount(count).build().toByteArray())
        val updated = head.toBuilder().setEventSeq(head.eventSeq + 1).build()
        heads.put(event.channelId, updated.toByteArray())
        emit(event, updated, now) {
            it.setReactionsChanged(
                ReactionsChanged.newBuilder()
                    .setMessageId(batch.messageId)
                    .setMsgSeq(batch.msgSeq)
                    .setEmoji(batch.emoji)
                    .setCount(count)
                    .addAllAdded(added)
                    .addAllRemoved(removed),
            )
        }
    }

    private fun join(event: InboundEvent, now: Long) {
        val head = head(event.channelId)
            ?: return command(event, now, RejectReason.REJECT_REASON_UNKNOWN_CHANNEL)
        val userId = event.memberJoinRequested.userId
        val selfJoin = userId == event.actorId
        val allowed = when {
            selfJoin -> head.type == ChannelType.CHANNEL_TYPE_PUBLIC
            else -> role(event.channelId, event.actorId) != null && head.type != ChannelType.CHANNEL_TYPE_DM
        }
        if (!allowed) return command(event, now, RejectReason.REJECT_REASON_FORBIDDEN)
        if (role(event.channelId, userId) != null) {
            result(event, accepted = true, eventSeq = head.eventSeq) // already a member
            return
        }
        members.put(memberKey(event.channelId, userId), memberState(MemberRole.MEMBER_ROLE_MEMBER))
        applyMembershipChange(event, head, now, delta = +1) {
            it.setMemberJoined(MemberJoined.newBuilder().setUserId(userId).setRole(MemberRole.MEMBER_ROLE_MEMBER))
        }
    }

    private fun kick(event: InboundEvent, now: Long) {
        val head = head(event.channelId)
            ?: return command(event, now, RejectReason.REJECT_REASON_UNKNOWN_CHANNEL)
        val userId = event.memberKickRequested.userId
        val leaving = userId == event.actorId
        if (role(event.channelId, userId) == null) {
            return command(event, now, RejectReason.REJECT_REASON_NOT_A_MEMBER)
        }
        if (!leaving && role(event.channelId, event.actorId) != MemberRole.MEMBER_ROLE_ADMIN) {
            return command(event, now, RejectReason.REJECT_REASON_FORBIDDEN)
        }
        members.delete(memberKey(event.channelId, userId))
        applyMembershipChange(event, head, now, delta = -1) {
            it.setMemberLeft(MemberLeft.newBuilder().setUserId(userId))
        }
    }

    /**
     * A channel admin sets another member's role (plan §6.7: `poster` posts in broadcast
     * channels). Membership changes like a join, so it moves `membership_version`: the
     * snapshots fan-out and others load carry roles too.
     */
    private fun changeRole(event: InboundEvent, now: Long) {
        val head = head(event.channelId)
            ?: return command(event, now, RejectReason.REJECT_REASON_UNKNOWN_CHANNEL)
        val change = event.memberRoleChangeRequested
        val role = change.role
        if (role != MemberRole.MEMBER_ROLE_MEMBER && role != MemberRole.MEMBER_ROLE_POSTER &&
            role != MemberRole.MEMBER_ROLE_ADMIN
        ) {
            return reject(event, now, RejectReason.REJECT_REASON_INVALID, "role must be member, poster or admin")
        }
        if (role == MemberRole.MEMBER_ROLE_POSTER && head.type != ChannelType.CHANNEL_TYPE_BROADCAST) {
            return reject(event, now, RejectReason.REJECT_REASON_INVALID, "posters are for broadcast channels")
        }
        if (role(event.channelId, event.actorId) != MemberRole.MEMBER_ROLE_ADMIN || change.userId == event.actorId) {
            return command(event, now, RejectReason.REJECT_REASON_FORBIDDEN)
        }
        val current = role(event.channelId, change.userId)
            ?: return command(event, now, RejectReason.REJECT_REASON_NOT_A_MEMBER)
        if (current == role) {
            result(event, accepted = true, eventSeq = head.eventSeq) // nothing to change
            return
        }
        members.put(memberKey(event.channelId, change.userId), memberState(role))
        applyMembershipChange(event, head, now, delta = 0) {
            it.setMemberRoleChanged(MemberRoleChanged.newBuilder().setUserId(change.userId).setRole(role))
        }
    }

    private fun applyMembershipChange(
        event: InboundEvent,
        head: ChannelHeadState,
        now: Long,
        delta: Long,
        payload: (SequencedEvent.Builder) -> Unit,
    ) {
        val eventSeq = head.eventSeq + 1
        val count = head.memberCount + delta
        var updated = head.toBuilder()
            .setEventSeq(eventSeq)
            .setMembershipVersion(eventSeq)
            .setMemberCount(count)
            .build()
        emit(event, updated, now, payload)
        result(event, accepted = true, eventSeq = eventSeq)

        val newMode = when {
            // Broadcast channels are always pulled (ADR-0008): one publish, however few members.
            head.type == ChannelType.CHANNEL_TYPE_BROADCAST -> FanoutMode.FANOUT_MODE_PULL
            head.fanoutMode != FanoutMode.FANOUT_MODE_PULL && count >= thresholds.pullAt -> FanoutMode.FANOUT_MODE_PULL
            head.fanoutMode == FanoutMode.FANOUT_MODE_PULL && count < thresholds.pushBelow -> FanoutMode.FANOUT_MODE_PUSH
            else -> head.fanoutMode
        }
        if (newMode != head.fanoutMode) {
            updated = updated.toBuilder().setEventSeq(updated.eventSeq + 1).setFanoutMode(newMode).build()
            emit(event.toBuilder().setDedupeId("").setRequestId("").build(), updated, now) {
                it.setChannelModeChanged(ChannelModeChanged.newBuilder().setMode(newMode))
            }
        }
        heads.put(event.channelId, updated.toByteArray())
    }

    private fun emit(
        event: InboundEvent,
        head: ChannelHeadState,
        now: Long,
        payload: (SequencedEvent.Builder) -> Unit,
    ) {
        val out = SequencedEvent.newBuilder()
            .setChannelId(event.channelId)
            .setWorkspaceId(head.workspaceId)
            .setActorId(event.actorId)
            .setEventSeq(head.eventSeq)
            .setMsgSeq(if (event.payloadCase == InboundEvent.PayloadCase.SEND_MESSAGE && !event.sendMessage.hasThreadRoot()) head.msgSeq else 0)
            .setMembershipVersion(head.membershipVersion)
            .setFanoutMode(head.fanoutMode)
            .setChannelType(head.type)
            .setDedupeId(event.dedupeId)
            .setRequestId(event.requestId)
            .also(payload)
            .build()
        context.forward(Record<Any, ByteArray>(event.channelId, out.toByteArray(), now), Names.SINK_PERSISTED)
    }

    /** Rejections aren't channel events: event_seq 0, delivered only to the actor. */
    private fun reject(event: InboundEvent, now: Long, reason: RejectReason, detail: String) {
        val out = SequencedEvent.newBuilder()
            .setChannelId(event.channelId)
            .setWorkspaceId(event.workspaceId)
            .setActorId(event.actorId)
            .setDedupeId(event.dedupeId)
            .setRequestId(event.requestId)
            .setRejected(Rejected.newBuilder().setReason(reason).setDetail(detail))
            .build()
        context.forward(Record<Any, ByteArray>(event.channelId, out.toByteArray(), now), Names.SINK_PERSISTED)
        result(event, accepted = false, reason = reason)
    }

    private fun command(event: InboundEvent, now: Long, reason: RejectReason) =
        reject(event, now, reason, reason.name.removePrefix("REJECT_REASON_").lowercase().replace('_', ' '))

    private fun result(event: InboundEvent, accepted: Boolean, eventSeq: Long = 0, reason: RejectReason? = null) {
        if (event.requestId.isEmpty()) return
        val out = CommandResult.newBuilder()
            .setRequestId(event.requestId)
            .setChannelId(event.channelId)
            .setAccepted(accepted)
            .setEventSeq(eventSeq)
            .also { if (reason != null) it.setReason(reason) }
            .build()
        context.forward(Record<Any, ByteArray>(event.requestId, out.toByteArray(), context.currentSystemTimeMs()), Names.SINK_COMMAND_RESULTS)
    }

    private fun nextMessageId(now: Long): Long {
        val partition = context.recordMetadata().map { it.partition() }.orElse(0)
        val last = partitionState.get(partition)?.let { PartitionState.parseFrom(it).lastMessageId } ?: 0L
        val id = Snowflake.next(last, now, partition)
        partitionState.put(
            partition,
            PartitionState.newBuilder().setFormatVersion(STATE_FORMAT).setLastMessageId(id).build().toByteArray(),
        )
        return id
    }

    private fun head(channelId: Long): ChannelHeadState? = heads.get(channelId)?.let { ChannelHeadState.parseFrom(it) }

    private fun role(channelId: Long, userId: Long): MemberRole? =
        members.get(memberKey(channelId, userId))?.let { MemberState.parseFrom(it).role }

    private fun memberKey(channelId: Long, userId: Long) = "$channelId:$userId"

    private fun memberState(role: MemberRole) =
        MemberState.newBuilder().setFormatVersion(STATE_FORMAT).setRole(role).build().toByteArray()
}
