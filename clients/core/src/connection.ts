// SPDX-License-Identifier: Apache-2.0
// WebSocket connection to the gateway: authenticate with Connect, then exchange
// protobuf frames. Reconnects with backoff; the caller resends its outbox when the
// connection is back ("online"), which is safe because the sequencer drops repeated
// client_msg_ids within 15 minutes.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { PresenceStatus } from "./gen/xmsg/v1/enums_pb.js";
import { ClientFrameSchema, ServerFrameSchema, type ServerFrame } from "./gen/xmsg/v1/client_pb.js";
import type { SequencedEvent } from "./gen/xmsg/v1/events_pb.js";
import type { RejectReason } from "./gen/xmsg/v1/enums_pb.js";

export const PROTOCOL_VERSION = 1;

const PRESENCE: Record<number, Presence> = {
  [PresenceStatus.ONLINE]: "online",
  [PresenceStatus.AWAY]: "away",
  [PresenceStatus.OFFLINE]: "offline",
};

/** Typing notices go out at most this often per channel (plan §5.9). */
export const TYPING_EVERY_MS = 3_000;
/** A typing indicator shows this long after the last notice (a bit over the send interval). */
export const TYPING_SHOWN_MS = 5_000;

/**
 * Who is typing where: notices keep someone listed for TYPING_SHOWN_MS, and their next
 * message ends it at once. In memory only; never saved.
 */
export class TypingTracker {
  private readonly until = new Map<string, Map<string, number>>();

  note(channelId: string, userId: string, now = Date.now()): void {
    const channel = this.until.get(channelId) ?? new Map<string, number>();
    channel.set(userId, now + TYPING_SHOWN_MS);
    this.until.set(channelId, channel);
  }

  /** They sent a message: they're done typing. */
  stop(channelId: string, userId: string): void {
    this.until.get(channelId)?.delete(userId);
  }

  /** Who is typing in the channel now, in the order they started. */
  who(channelId: string, now = Date.now()): string[] {
    const channel = this.until.get(channelId);
    if (!channel) return [];
    for (const [user, until] of channel) if (until <= now) channel.delete(user);
    return [...channel.keys()];
  }
}

export type ConnectionStatus = "connecting" | "online" | "offline" | "unauthorized";

export interface ConnectionHandlers {
  onEvent(event: SequencedEvent): void;
  onAck(clientMsgId: string): void;
  onSendRejected(clientMsgId: string, reason: RejectReason, detail: string): void;
  onStatus(status: ConnectionStatus): void;
  /**
   * The gateway lost deliveries for us (ADR-0009): catch up through sync. `partition`
   * names the only affected partition; undefined means everything.
   */
  onResync?(partition: number | undefined): void;
  /** Someone else is typing in a channel (best-effort; show it for a few seconds). */
  onTyping?(channelId: string, userId: string): void;
  /** Statuses of watched users: the current ones after watchPresence, then changes. */
  onPresence?(updates: { userId: string; status: Presence }[]): void;
}

export type Presence = "online" | "away" | "offline";

export interface ConnectionOptions extends ConnectionHandlers {
  url: string;
  /** A current access token (refreshed by the caller as needed). */
  token(): Promise<string>;
  /** Injectable for tests; defaults to the global WebSocket. */
  WebSocket?: typeof WebSocket;
  pingEveryMs?: number;
}

/** Frames as sent over the socket (WebSocket.send wants an ArrayBuffer-backed view). */
export type Frame = Uint8Array<ArrayBuffer>;

export function encodeSend(
  channelId: string,
  clientMsgId: string,
  body: string,
  threadRoot?: { msgSeq: number; messageId: string },
): Frame {
  const root = threadRoot && { msgSeq: BigInt(threadRoot.msgSeq), messageId: BigInt(threadRoot.messageId) };
  return toBinary(
    ClientFrameSchema,
    create(ClientFrameSchema, {
      frame: { case: "sendMessage", value: { channelId: BigInt(channelId), clientMsgId, body, ...(root && { threadRoot: root }) } },
    }),
  ) as Frame;
}

export function encodeConnect(accessToken: string): Frame {
  return toBinary(
    ClientFrameSchema,
    create(ClientFrameSchema, {
      frame: { case: "connect", value: { accessToken, protocolVersion: PROTOCOL_VERSION } },
    }),
  ) as Frame;
}

export function decodeServer(bytes: Uint8Array): ServerFrame {
  return fromBinary(ServerFrameSchema, bytes);
}

export class Connection {
  private ws: WebSocket | undefined;
  private stopped = false;
  private online = false;
  private attempt = 0;
  private pinger: ReturnType<typeof setInterval> | undefined;
  private retry: ReturnType<typeof setTimeout> | undefined;
  private partitions = 0;

  private readonly opts: ConnectionOptions;
  constructor(opts: ConnectionOptions) {
    this.opts = opts;
  }

  start(): void {
    this.stopped = false;
    void this.open();
  }

  stop(): void {
    this.stopped = true;
    clearTimeout(this.retry);
    clearInterval(this.pinger);
    this.ws?.close(1000, "bye");
    this.ws = undefined;
  }

  get isOnline(): boolean {
    return this.online;
  }

  /** The installation's partition count from the last Welcome (0 before the first). */
  get partitionCount(): number {
    return this.partitions;
  }

  /**
   * Tells the channel I'm typing: call it on every keystroke. Sent at most once per
   * TYPING_EVERY_MS per channel (the gateway enforces the same), never queued offline.
   */
  typing(channelId: string, now = Date.now()): void {
    if (!this.online || !this.ws) return;
    if (now - (this.typedAt.get(channelId) ?? -Infinity) < TYPING_EVERY_MS) return;
    this.typedAt.set(channelId, now);
    this.ws.send(
      toBinary(ClientFrameSchema, create(ClientFrameSchema, { frame: { case: "typing", value: { channelId: BigInt(channelId) } } })) as Frame,
    );
  }

  private readonly typedAt = new Map<string, number>();
  private watched: string[] = [];
  private active = true;

  /** Watch these users' presence (replacing the previous set; at most 200). Kept across reconnects. */
  watchPresence(userIds: string[]): void {
    const ids = [...new Set(userIds)].slice(0, 200);
    if (ids.length === this.watched.length && ids.every((id, i) => id === this.watched[i])) return;
    this.watched = ids;
    this.sendWatch();
  }

  /** Whether the user is using the app (front, recent input); others see "away" when not. */
  setActive(active: boolean): void {
    if (active === this.active) return;
    this.active = active;
    this.sendActivity();
  }

  private sendWatch(): void {
    if (!this.online || !this.ws) return;
    const frame = create(ClientFrameSchema, { frame: { case: "watchPresence", value: { userIds: this.watched.map(BigInt) } } });
    this.ws.send(toBinary(ClientFrameSchema, frame) as Frame);
  }

  private sendActivity(): void {
    if (!this.online || !this.ws) return;
    const frame = create(ClientFrameSchema, { frame: { case: "activity", value: { active: this.active } } });
    this.ws.send(toBinary(ClientFrameSchema, frame) as Frame);
  }

  /** Sends if online; returns false otherwise (the message stays in the caller's outbox). */
  send(channelId: string, clientMsgId: string, body: string, threadRoot?: { msgSeq: number; messageId: string }): boolean {
    if (!this.online || !this.ws) return false;
    this.ws.send(encodeSend(channelId, clientMsgId, body, threadRoot));
    return true;
  }

  private async open(): Promise<void> {
    this.opts.onStatus("connecting");
    let token: string;
    try {
      token = await this.opts.token();
    } catch {
      this.opts.onStatus("unauthorized");
      return;
    }
    if (this.stopped) return;
    const Ws = this.opts.WebSocket ?? WebSocket;
    const ws = new Ws(this.opts.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    ws.onopen = () => ws.send(encodeConnect(token));
    ws.onmessage = (msg) => this.receive(new Uint8Array(msg.data as ArrayBuffer));
    ws.onclose = (close) => {
      if (this.ws !== ws) return;
      this.online = false;
      clearInterval(this.pinger);
      if (this.stopped) return;
      // An invalid token: get a fresh one (the caller refreshes) and try again, once quickly.
      const delay = close.reason === "invalid token" && this.attempt === 0 ? 0 : this.backoff();
      this.attempt += 1;
      this.opts.onStatus("offline");
      this.retry = setTimeout(() => void this.open(), delay);
    };
  }

  private receive(bytes: Uint8Array): void {
    let frame: ServerFrame;
    try {
      frame = decodeServer(bytes);
    } catch {
      return; // a frame from a newer protocol we can't read
    }
    switch (frame.frame.case) {
      case "welcome":
        this.partitions = frame.frame.value.partitionCount;
        this.online = true;
        this.attempt = 0;
        this.startPings();
        // A new connection starts active and watching nobody: restore both.
        if (!this.active) this.sendActivity();
        if (this.watched.length) this.sendWatch();
        this.opts.onStatus("online");
        break;
      case "event":
        this.opts.onEvent(frame.frame.value);
        break;
      case "ack":
        this.opts.onAck(frame.frame.value.clientMsgId);
        break;
      case "sendRejected":
        this.opts.onSendRejected(frame.frame.value.clientMsgId, frame.frame.value.reason, frame.frame.value.detail);
        break;
      case "resync":
        this.opts.onResync?.(frame.frame.value.partition);
        break;
      case "presence":
        this.opts.onPresence?.(
          frame.frame.value.updates.map((u) => ({ userId: u.userId.toString(), status: PRESENCE[u.status] ?? "offline" })),
        );
        break;
      case "typing":
        this.opts.onTyping?.(frame.frame.value.channelId.toString(), frame.frame.value.userId.toString());
        break;
      default:
        break; // pong
    }
  }

  private startPings(): void {
    clearInterval(this.pinger);
    this.pinger = setInterval(() => {
      const ping = create(ClientFrameSchema, { frame: { case: "ping", value: { sentAtMs: BigInt(Date.now()) } } });
      this.ws?.send(toBinary(ClientFrameSchema, ping) as Frame);
    }, this.opts.pingEveryMs ?? 25_000);
  }

  /** 0.5 s, 1 s, 2 s ... capped at 15 s, with jitter. */
  private backoff(): number {
    const base = Math.min(15_000, 500 * 2 ** this.attempt);
    return base / 2 + Math.random() * (base / 2);
  }
}
