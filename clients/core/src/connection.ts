// SPDX-License-Identifier: Apache-2.0
// WebSocket connection to the gateway: authenticate with Connect, then exchange
// protobuf frames. Reconnects with backoff; the caller resends its outbox when the
// connection is back ("online"), which is safe because the sequencer drops repeated
// client_msg_ids within 15 minutes.

import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { ClientFrameSchema, ServerFrameSchema, type ServerFrame } from "./gen/xmsg/v1/client_pb.js";
import type { SequencedEvent } from "./gen/xmsg/v1/events_pb.js";
import type { RejectReason } from "./gen/xmsg/v1/enums_pb.js";

export const PROTOCOL_VERSION = 1;

export type ConnectionStatus = "connecting" | "online" | "offline" | "unauthorized";

export interface ConnectionHandlers {
  onEvent(event: SequencedEvent): void;
  onAck(clientMsgId: string): void;
  onSendRejected(clientMsgId: string, reason: RejectReason, detail: string): void;
  onStatus(status: ConnectionStatus): void;
}

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

export function encodeSend(channelId: string, clientMsgId: string, body: string): Frame {
  return toBinary(
    ClientFrameSchema,
    create(ClientFrameSchema, {
      frame: { case: "sendMessage", value: { channelId: BigInt(channelId), clientMsgId, body } },
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

  /** Sends if online; returns false otherwise (the message stays in the caller's outbox). */
  send(channelId: string, clientMsgId: string, body: string): boolean {
    if (!this.online || !this.ws) return false;
    this.ws.send(encodeSend(channelId, clientMsgId, body));
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
        this.online = true;
        this.attempt = 0;
        this.startPings();
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
      default:
        break; // pong; resync comes with the sync API
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
