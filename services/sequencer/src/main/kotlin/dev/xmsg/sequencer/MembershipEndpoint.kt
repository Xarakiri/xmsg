// SPDX-License-Identifier: AGPL-3.0-only
package dev.xmsg.sequencer

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.xmsg.proto.v1.ChannelHeadState
import dev.xmsg.proto.v1.ChannelMembers
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyQueryMetadata
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.errors.InvalidStateStoreException
import org.apache.kafka.streams.state.HostInfo
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import java.net.InetSocketAddress
import java.util.concurrent.Executors

/**
 * A channel's members straight from the sequencer's stores (plan §5.6): never behind any
 * event it has sequenced, so fan-out can ask here when the wide-column store hasn't
 * caught up (ADR-0006). Null for a channel this store doesn't know.
 */
fun membersOf(
    heads: ReadOnlyKeyValueStore<Long, ByteArray>,
    members: ReadOnlyKeyValueStore<String, ByteArray>,
    channelId: Long,
): ChannelMembers? {
    val head = heads.get(channelId)?.let { ChannelHeadState.parseFrom(it) } ?: return null
    // Keys are "<channel>:<user>"; the trailing ':' keeps channel 12 from matching 123.
    val users = members.prefixScan("$channelId:", StringSerializer()).use { rows ->
        rows.asSequence().map { it.key.substringAfter(':').toLong() }.toList()
    }
    return ChannelMembers.newBuilder()
        .setChannelId(channelId)
        .setMembershipVersion(head.membershipVersion)
        .addAllUserIds(users.sorted())
        .build()
}

/**
 * `GET /v1/channels/{id}/members` → protobuf `ChannelMembers`, over Kafka Streams
 * Interactive Queries. A channel's partition may live on another instance: then the
 * answer is a redirect to it (instances advertise themselves via `application.server`).
 * 503 while rebalancing or restoring; 404 for an unknown channel.
 *
 * Plain HTTP + protobuf rather than the plan's gRPC: the same contract, no gRPC stack on
 * either side; swapping the transport later doesn't change the message.
 */
class MembershipServer(private val streams: KafkaStreams, private val self: HostInfo, port: Int) {
    private val server = HttpServer.create(InetSocketAddress(port), 0).apply {
        executor = Executors.newFixedThreadPool(4)
        createContext("/v1/channels/") { exchange -> exchange.use { handle(it) } }
        createContext("/healthz") { exchange -> exchange.use { reply(it, 200, "ok".toByteArray(), "text/plain") } }
    }

    fun start() = server.start()

    fun stop() = server.stop(1)

    private fun handle(exchange: HttpExchange) {
        val path = exchange.requestURI.path
        val channelId = Regex("^/v1/channels/(\\d+)/members$").find(path)?.groupValues?.get(1)?.toLongOrNull()
            ?: return reply(exchange, 404, "not found".toByteArray(), "text/plain")
        if (streams.state() != KafkaStreams.State.RUNNING) return unavailable(exchange)

        val metadata = streams.queryMetadataForKey(Names.STORE_CHANNEL_HEADS, channelId, LongSerializer())
        if (metadata == null || metadata == KeyQueryMetadata.NOT_AVAILABLE) return unavailable(exchange)
        if (metadata.activeHost() != self) {
            val owner = metadata.activeHost()
            exchange.responseHeaders.add("Location", "http://${owner.host()}:${owner.port()}$path")
            return reply(exchange, 307, ByteArray(0), "text/plain")
        }
        val found = try {
            membersOf(store(Names.STORE_CHANNEL_HEADS), store(Names.STORE_MEMBERS), channelId)
        } catch (e: InvalidStateStoreException) {
            return unavailable(exchange) // migrating between threads or instances right now
        }
        if (found == null) return reply(exchange, 404, "unknown channel".toByteArray(), "text/plain")
        reply(exchange, 200, found.toByteArray(), "application/x-protobuf")
    }

    private fun <K> store(name: String): ReadOnlyKeyValueStore<K, ByteArray> =
        streams.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore<K, ByteArray>()))

    private fun unavailable(exchange: HttpExchange) = reply(exchange, 503, "try again".toByteArray(), "text/plain")

    private fun reply(exchange: HttpExchange, status: Int, body: ByteArray, type: String) {
        exchange.responseHeaders.add("Content-Type", type)
        exchange.sendResponseHeaders(status, if (body.isEmpty()) -1 else body.size.toLong())
        if (body.isNotEmpty()) exchange.responseBody.use { it.write(body) }
    }
}
