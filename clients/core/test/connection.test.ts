// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { ClientFrameSchema, ServerFrameSchema } from "../src/gen/xmsg/v1/client_pb.js";
import { Connection, encodeSend, type ConnectionStatus } from "../src/connection.ts";

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

test("Resync frames reach onResync, scoped to their partition", async () => {
  const resyncs: (number | undefined)[] = [];
  const conn = new Connection({
    url: "ws://gw/ws",
    token: async () => "t",
    WebSocket: FakeSocket as unknown as typeof WebSocket,
    onStatus: () => {},
    onAck: () => {},
    onEvent: () => {},
    onSendRejected: () => {},
    onResync: (p) => resyncs.push(p),
  });
  conn.start();
  await tick();
  FakeSocket.last.push({ frame: { case: "welcome", value: { partitionCount: 16 } } });
  assert.equal(conn.partitionCount, 16);
  FakeSocket.last.push({ frame: { case: "resync", value: { partition: 3 } } });
  FakeSocket.last.push({ frame: { case: "resync", value: {} } });
  assert.deepEqual(resyncs, [3, undefined]);
  conn.stop();
});

test("typing shows for a few seconds after the last notice, and a message ends it", async () => {
  const { TypingTracker, TYPING_SHOWN_MS } = await import("../src/connection.ts");
  const t = new TypingTracker();
  t.note("7", "102", 1_000);
  t.note("7", "103", 2_000);
  assert.deepEqual(t.who("7", 2_500), ["102", "103"]);
  assert.deepEqual(t.who("7", 1_000 + TYPING_SHOWN_MS), ["103"], "102's notice ran out");
  t.stop("7", "103");
  assert.deepEqual(t.who("7", 2_500), []);
  assert.deepEqual(t.who("8", 0), []);
});

test("typing goes out at most every 3 s per channel, and others' typing reaches onTyping", async () => {
  const typing: [string, string][] = [];
  const conn = new Connection({
    url: "ws://gw/ws",
    token: async () => "t",
    WebSocket: FakeSocket as unknown as typeof WebSocket,
    onStatus: () => {},
    onAck: () => {},
    onEvent: () => {},
    onSendRejected: () => {},
    onTyping: (c, u) => typing.push([c, u]),
  });
  conn.typing("7", 0); // offline: dropped, never queued
  conn.start();
  await tick();
  FakeSocket.last.push({ frame: { case: "welcome", value: { partitionCount: 16 } } });
  const before = FakeSocket.last.sent.length;
  conn.typing("7", 10_000);
  conn.typing("7", 11_000);
  conn.typing("8", 11_000);
  conn.typing("7", 13_000);
  const frames = FakeSocket.last.sent.slice(before).map((b) => fromBinary(ClientFrameSchema, b).frame);
  assert.deepEqual(
    frames.map((f) => (f.case === "typing" ? f.value.channelId : null)),
    [7n, 8n, 7n],
  );
  FakeSocket.last.push({ frame: { case: "typing", value: { channelId: 7n, userId: 102n } } });
  assert.deepEqual(typing, [["7", "102"]]);
  conn.stop();
});

test("presence: watches and activity survive reconnects, updates reach onPresence", async () => {
  const seen: { userId: string; status: string }[][] = [];
  const conn = new Connection({
    url: "ws://gw/ws",
    token: async () => "t",
    WebSocket: FakeSocket as unknown as typeof WebSocket,
    onStatus: () => {},
    onAck: () => {},
    onEvent: () => {},
    onSendRejected: () => {},
    onPresence: (u) => seen.push(u),
  });
  conn.watchPresence(["102", "103", "102"]); // before connecting: sent on Welcome
  conn.setActive(false);
  conn.start();
  await tick();
  FakeSocket.last.push({ frame: { case: "welcome", value: { partitionCount: 16 } } });
  const frames = FakeSocket.last.sent.slice(1).map((b) => fromBinary(ClientFrameSchema, b).frame);
  assert.deepEqual(frames.map((f) => f.case), ["activity", "watchPresence"]);
  const watch = frames[1]!;
  assert.deepEqual(watch.case === "watchPresence" ? watch.value.userIds : [], [102n, 103n]);
  const before = FakeSocket.last.sent.length;
  conn.watchPresence(["102", "103"]); // unchanged: nothing sent
  assert.equal(FakeSocket.last.sent.length, before);
  FakeSocket.last.push({ frame: { case: "presence", value: { updates: [{ userId: 102n, status: 3 }, { userId: 103n, status: 2 }] } } });
  assert.deepEqual(seen, [[{ userId: "102", status: "online" }, { userId: "103", status: "away" }]]);
  conn.stop();
});

test("the open channel is sent on Welcome and when it changes; bumps reach onBump", async () => {
  const bumps: [string, number, number][] = [];
  const conn = new Connection({
    url: "ws://gw/ws",
    token: async () => "t",
    WebSocket: FakeSocket as unknown as typeof WebSocket,
    onStatus: () => {},
    onAck: () => {},
    onEvent: () => {},
    onSendRejected: () => {},
    onBump: (channelId, eventSeq, msgSeq) => bumps.push([channelId, eventSeq, msgSeq]),
  });
  conn.view("901"); // before connecting: sent on Welcome
  conn.start();
  await tick();
  FakeSocket.last.push({ frame: { case: "welcome", value: { partitionCount: 16 } } });
  const sent = () => FakeSocket.last.sent.slice(1).map((b) => fromBinary(ClientFrameSchema, b).frame);
  const views = () => sent().filter((f) => f.case === "viewChannel").map((f) => (f.case === "viewChannel" ? f.value.channelId : -1n));
  assert.deepEqual(views(), [901n]);
  conn.view("901"); // unchanged: nothing sent
  conn.view(null); // closed: 0
  assert.deepEqual(views(), [901n, 0n]);
  FakeSocket.last.push({ frame: { case: "bump", value: { channelId: 902n, eventSeq: 9n, msgSeq: 5n } } });
  assert.deepEqual(bumps, [["902", 9, 5]]);
  conn.stop();
});

test("sends carry the attached files' IDs", () => {
  const frame = fromBinary(ClientFrameSchema, encodeSend("7", "c-1", "", undefined, ["42", "43"]));
  assert.equal(frame.frame.case, "sendMessage");
  if (frame.frame.case === "sendMessage") assert.deepEqual(frame.frame.value.fileIds, [42n, 43n]);
});
