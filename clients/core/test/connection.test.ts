// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { ClientFrameSchema, ServerFrameSchema } from "../src/gen/xmsg/v1/client_pb.js";
import { Connection, type ConnectionStatus } from "../src/connection.ts";

/** Minimal stand-in for the browser WebSocket: records frames, lets the test push frames back. */
class FakeSocket {
  static last: FakeSocket;
  binaryType = "blob";
  sent: Uint8Array[] = [];
  onopen?: () => void;
  onmessage?: (m: { data: ArrayBuffer }) => void;
  onclose?: (c: { code: number; reason: string }) => void;
  url: string;
  constructor(url: string) {
    this.url = url;
    FakeSocket.last = this;
    queueMicrotask(() => this.onopen?.());
  }
  send(bytes: Uint8Array) {
    this.sent.push(bytes);
  }
  close() {}
  push(frame: Parameters<typeof create<typeof ServerFrameSchema>>[1]) {
    const bytes = toBinary(ServerFrameSchema, create(ServerFrameSchema, frame));
    this.onmessage?.({ data: bytes.slice().buffer });
  }
}

const tick = () => new Promise((r) => setTimeout(r, 5));

test("connects with the token, goes online on Welcome, and routes frames", async () => {
  const statuses: ConnectionStatus[] = [];
  const acks: string[] = [];
  const events: bigint[] = [];
  const conn = new Connection({
    url: "ws://gw/ws",
    token: async () => "tok-1",
    WebSocket: FakeSocket as unknown as typeof WebSocket,
    onStatus: (s) => statuses.push(s),
    onAck: (id) => acks.push(id),
    onEvent: (e) => events.push(e.msgSeq),
    onSendRejected: () => {},
  });
  assert.equal(conn.send("7", "c-0", "too early"), false, "not online yet");
  conn.start();
  await tick();
  const sock = FakeSocket.last;
  const connect = fromBinary(ClientFrameSchema, sock.sent[0]!);
  assert.equal(connect.frame.case, "connect");
  assert.equal(connect.frame.case === "connect" && connect.frame.value.accessToken, "tok-1");

  sock.push({ frame: { case: "welcome", value: { partitionCount: 16 } } });
  assert.deepEqual(statuses, ["connecting", "online"]);
  assert.equal(conn.send("7", "c-1", "hi"), true);
  const send = fromBinary(ClientFrameSchema, sock.sent[1]!);
  assert.equal(send.frame.case === "sendMessage" && send.frame.value.channelId, 7n);

  sock.push({ frame: { case: "ack", value: { clientMsgId: "c-1" } } });
  sock.push({ frame: { case: "event", value: { channelId: 7n, msgSeq: 3n } } });
  assert.deepEqual(acks, ["c-1"]);
  assert.deepEqual(events, [3n]);
  conn.stop();
});

test("a closed socket reconnects with a fresh token after an invalid-token close", async () => {
  let n = 0;
  const statuses: ConnectionStatus[] = [];
  const conn = new Connection({
    url: "ws://gw/ws",
    token: async () => `tok-${++n}`,
    WebSocket: FakeSocket as unknown as typeof WebSocket,
    onStatus: (s) => statuses.push(s),
    onAck: () => {},
    onEvent: () => {},
    onSendRejected: () => {},
  });
  conn.start();
  await tick();
  FakeSocket.last.onclose?.({ code: 1008, reason: "invalid token" });
  await tick();
  const second = fromBinary(ClientFrameSchema, FakeSocket.last.sent[0]!);
  assert.equal(second.frame.case === "connect" && second.frame.value.accessToken, "tok-2");
  assert.deepEqual(statuses, ["connecting", "offline", "connecting"]);
  conn.stop();
});
