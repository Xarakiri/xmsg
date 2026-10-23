// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { SequencedEventSchema, type SequencedEvent } from "../src/gen/xmsg/v1/events_pb.js";
import { ChannelType, MemberRole, RejectReason } from "../src/gen/xmsg/v1/enums_pb.js";
import { initialState, reduce, unsent, type ChatState } from "../src/state.ts";

const ME = "101";
const BOB = "102";

function ev(init: MessageInitShape<typeof SequencedEventSchema>): SequencedEvent {
  return create(SequencedEventSchema, { channelId: 7n, channelType: ChannelType.PRIVATE, ...init });
}

const created = ev({
  eventSeq: 1n,
  actorId: 101n,
  payload: {
    case: "channelCreated",
    value: {
      type: ChannelType.PRIVATE,
      name: "design",
      members: [
        { userId: 101n, role: MemberRole.ADMIN },
        { userId: 102n, role: MemberRole.MEMBER },
      ],
    },
  },
});

const message = (seq: number, actor: bigint, dedupeId: string, body: string) =>
  ev({
    eventSeq: BigInt(seq + 1),
    msgSeq: BigInt(seq),
    actorId: actor,
    dedupeId,
    payload: { case: "messageCreated", value: { messageId: BigInt(1000 + seq), body } },
  });

const apply = (state: ChatState, ...events: SequencedEvent[]) =>
  events.reduce((s, event) => reduce(s, { type: "event", event, at: 1 }), state);

test("channel.created brings a channel with its name and members", () => {
  const s = apply(initialState(ME), created);
  assert.deepEqual(s.channels["7"], {
    id: "7",
    kind: "private",
    name: "design",
    members: ["101", "102"],
    eventSeq: 1,
    syncedSeq: 1,
    membershipSeq: 1,
    msgSeq: 0,
    readSeq: 0,
    muted: false,
    notifyLevel: "all",
    left: false,
  });
});

test("an own message goes pending → sent → confirmed in place, ordered by msg_seq", () => {
  let s = apply(initialState(ME), created, message(1, 102n, "b-1", "hi"));
  s = reduce(s, { type: "send", channelId: "7", clientMsgId: "c-1", body: "hello", at: 5 });
  assert.equal(s.messages["7"]!.at(-1)!.status, "pending");
  s = reduce(s, { type: "ack", clientMsgId: "c-1" });
  assert.equal(s.messages["7"]!.at(-1)!.status, "sent");
  assert.deepEqual(unsent(s).map((m) => m.key), ["c-1"], "acked but not yet sequenced is still resent");
  s = apply(s, message(2, 101n, "c-1", "hello"));
  assert.deepEqual(
    s.messages["7"]!.map((m) => [m.key, m.msgSeq, m.status]),
    [
      ["seq:1", 1, "confirmed"],
      ["c-1", 2, "confirmed"],
    ],
  );
  assert.equal(s.messages["7"]![1]!.at, 5, "keeps the local send time");
  assert.deepEqual(unsent(s), []);
});

test("repeated events change nothing", () => {
  const once = apply(initialState(ME), created, message(1, 102n, "b-1", "hi"));
  assert.deepEqual(apply(once, message(1, 102n, "b-1", "hi"), created), once);
});

test("history arriving after newer live events still fills in the older messages", () => {
  // Live: created (1), then message 3 (event 4) while events 2-3 were missed.
  let s = apply(initialState(ME), created, message(3, 102n, "b-3", "third"));
  assert.equal(s.channels["7"]!.eventSeq, 4);
  assert.equal(s.channels["7"]!.syncedSeq, 1, "a gap: only event 1 is contiguous");
  s = reduce(s, { type: "history", channelId: "7", events: [message(1, 102n, "b-1", "first"), message(2, 101n, "c-2", "second")], through: 3, at: 2 });
  assert.deepEqual(s.messages["7"]!.map((m) => m.body), ["first", "second", "third"]);
  assert.equal(s.channels["7"]!.syncedSeq, 3);
  assert.equal(s.messages["7"]![1]!.key, "c-2", "own message from history keeps its client_msg_id key");
});

test("an older membership event never undoes a newer one", () => {
  const leave = ev({ eventSeq: 5n, payload: { case: "memberLeft", value: { userId: 103n } } });
  const join = ev({ eventSeq: 3n, payload: { case: "memberJoined", value: { userId: 103n, role: MemberRole.MEMBER } } });
  const s = apply(initialState(ME), created, leave, join);
  assert.deepEqual(s.channels["7"]!.members, ["101", "102"]);
});

test("the channel list adds channels we missed, with names, and sync knows what's behind", async () => {
  const { behind } = await import("../src/state.ts");
  let s = apply(initialState(ME), created);
  const listed = [
    { id: "7", kind: "private" as const, name: "design", eventSeq: 6, msgSeq: 3 },
    { id: "9", kind: "public" as const, name: "general", eventSeq: 2, msgSeq: 1 },
  ];
  s = reduce(s, { type: "channelList", channels: listed });
  assert.equal(s.channels["9"]!.name, "general");
  assert.deepEqual(behind(s, listed), [
    { id: "7", after: 1 },
    { id: "9", after: 0 },
  ]);
  s = reduce(s, { type: "lostAccess", channelId: "9" });
  assert.equal(s.channels["9"]!.left, true);
});

test("a rejected send becomes a visible error, never silently dropped", () => {
  let s = reduce(apply(initialState(ME), created), { type: "send", channelId: "7", clientMsgId: "c-9", body: "x", at: 1 });
  s = apply(
    s,
    ev({ actorId: 101n, dedupeId: "c-9", payload: { case: "rejected", value: { reason: RejectReason.NOT_A_MEMBER, detail: "" } } }),
  );
  const m = s.messages["7"]![0]!;
  assert.equal(m.status, "rejected");
  assert.match(m.error!, /not a member/);
  s = reduce(s, { type: "send", channelId: "7", clientMsgId: "c-10", body: "y", at: 2 });
  s = reduce(s, { type: "sendRejected", clientMsgId: "c-10", reason: RejectReason.INVALID, detail: "too long" });
  assert.equal(s.messages["7"]![1]!.status, "rejected");
});

test("membership events update members, and being removed marks the channel left", () => {
  let s = apply(
    initialState(ME),
    created,
    ev({ eventSeq: 2n, payload: { case: "memberJoined", value: { userId: 103n, role: MemberRole.MEMBER } } }),
  );
  assert.deepEqual(s.channels["7"]!.members, ["101", "102", "103"]);
  s = apply(s, ev({ eventSeq: 3n, payload: { case: "memberLeft", value: { userId: 101n } } }));
  assert.equal(s.channels["7"]!.left, true);
  assert.deepEqual(s.channels["7"]!.members, ["102", "103"]);
});

test("an invite to a channel we never saw creates it from the join", () => {
  const s = apply(
    initialState(BOB),
    ev({ channelId: 9n, channelType: ChannelType.PUBLIC, eventSeq: 4n, payload: { case: "memberJoined", value: { userId: 102n, role: MemberRole.MEMBER } } }),
  );
  assert.equal(s.channels["9"]!.kind, "public");
  assert.deepEqual(s.channels["9"]!.members, ["102"]);
});

test("state survives a JSON round trip (saved between page loads)", () => {
  const s = apply(initialState(ME), created, message(1, 102n, "b-1", "hi"));
  assert.deepEqual(JSON.parse(JSON.stringify(s)), s);
});

test("unread counts: others' messages count, own sends and joins don't, positions only rise", async () => {
  const { unread } = await import("../src/state.ts");
  let s = apply(initialState(ME), created, message(1, 102n, "b-1", "hi"), message(2, 102n, "b-2", "there"));
  assert.equal(unread(s.channels["7"]!), 2);
  s = apply(s, message(3, 101n, "c-1", "mine"));
  assert.equal(unread(s.channels["7"]!), 0, "sending reads the channel up to my message");
  s = apply(s, message(4, 102n, "b-3", "more"));
  assert.equal(unread(s.channels["7"]!), 1);
  s = reduce(s, { type: "readPositions", positions: [{ channelId: "7", msgSeq: 2 }] });
  assert.equal(unread(s.channels["7"]!), 1, "an older server position doesn't undo local progress");
  s = reduce(s, { type: "markRead", channelId: "7", msgSeq: 4 });
  assert.equal(unread(s.channels["7"]!), 0);

  // Joining a channel with history: nothing before the join is unread.
  let bob = reduce(initialState(BOB), {
    type: "channelList",
    channels: [{ id: "9", kind: "public", name: "general", eventSeq: 10, msgSeq: 8 }],
  });
  bob = apply(bob, ev({ channelId: 9n, eventSeq: 11n, payload: { case: "memberJoined", value: { userId: 102n, role: MemberRole.MEMBER } } }));
  assert.equal(unread(bob.channels["9"]!), 0);
});

test("settings apply to known channels", () => {
  let s = apply(initialState(ME), created);
  s = reduce(s, { type: "settings", settings: [{ channelId: "7", muted: true, notifyLevel: "mentions" }, { channelId: "404", muted: true, notifyLevel: "all" }] });
  assert.equal(s.channels["7"]!.muted, true);
  assert.equal(s.channels["7"]!.notifyLevel, "mentions");
  assert.equal(s.channels["404"], undefined, "settings never create channels");
});

test("a state saved by an older client gets the newer fields' defaults", async () => {
  const { restore } = await import("../src/state.ts");
  const old = { me: ME, channels: { "7": { id: "7", kind: "private", name: "design", members: ["101"], eventSeq: 4, left: false } }, messages: {} };
  const s = restore(JSON.parse(JSON.stringify(old)), ME);
  assert.equal(s.channels["7"]!.notifyLevel, "all");
  assert.equal(s.channels["7"]!.muted, false);
  assert.equal(s.channels["7"]!.readSeq, 0);
  assert.equal(s.channels["7"]!.eventSeq, 4, "saved values are kept");
  assert.deepEqual(restore({ me: "someone else", channels: {} }, ME), initialState(ME));
  assert.deepEqual(restore(null, ME), initialState(ME));
});

test("the outbox gives up after 10 minutes; a retry goes out under a new client_msg_id", async () => {
  const { OUTBOX_RETRY_MS } = await import("../src/state.ts");
  let s = reduce(apply(initialState(ME), created), { type: "send", channelId: "7", clientMsgId: "c-old", body: "hi", at: 1_000 });
  s = reduce(s, { type: "send", channelId: "7", clientMsgId: "c-new", body: "later", at: 1_000 + OUTBOX_RETRY_MS });
  s = reduce(s, { type: "expire", now: 1_000 + OUTBOX_RETRY_MS + 1 });
  assert.deepEqual(s.messages["7"]!.map((m) => [m.key, m.status]), [["c-old", "failed"], ["c-new", "pending"]]);
  assert.deepEqual(unsent(s).map((m) => m.key), ["c-new"], "a failed message isn't resent automatically");

  s = reduce(s, { type: "retry", oldKey: "c-old", newKey: "c-retry", at: 2_000_000 });
  const retried = s.messages["7"]!.find((m) => m.body === "hi")!;
  assert.deepEqual([retried.key, retried.status, retried.error], ["c-retry", "pending", undefined]);
  assert.ok(unsent(s).some((m) => m.key === "c-retry"));
});

test("a failed message that did get through is confirmed by its event", () => {
  let s = reduce(apply(initialState(ME), created), { type: "send", channelId: "7", clientMsgId: "c-1", body: "hi", at: 0 });
  s = reduce(s, { type: "expire", now: 11 * 60_000 });
  assert.equal(s.messages["7"]![0]!.status, "failed");
  s = apply(s, message(1, 101n, "c-1", "hi"));
  assert.deepEqual(s.messages["7"]!.map((m) => [m.key, m.status]), [["c-1", "confirmed"]]);
});

const edited = (eventSeq: number, msgSeq: number, body: string) =>
  ev({
    eventSeq: BigInt(eventSeq),
    actorId: 102n,
    payload: { case: "messageEdited", value: { msgSeq: BigInt(msgSeq), messageId: BigInt(1000 + msgSeq), body, editedAtMs: 5n } },
  });

const deleted = (eventSeq: number, msgSeq: number) =>
  ev({
    eventSeq: BigInt(eventSeq),
    actorId: 102n,
    payload: { case: "messageDeleted", value: { msgSeq: BigInt(msgSeq), messageId: BigInt(1000 + msgSeq) } },
  });

test("edits replace the body in place; an older edit redelivered never wins", () => {
  const s = apply(initialState(ME), created, message(1, 102n, "b-1", "helo"), edited(3, 1, "hello"), edited(4, 1, "hello!"), edited(3, 1, "hello"));
  const [m] = s.messages["7"]!;
  assert.equal(m!.body, "hello!");
  assert.equal(m!.editedAt, 5);
  assert.equal(s.channels["7"]!.msgSeq, 1, "edits don't count as messages");
});

test("a deleted message keeps its place with no body, and nothing brings the body back", () => {
  const s = apply(
    initialState(ME),
    created,
    message(1, 102n, "b-1", "oops"),
    message(2, 102n, "b-2", "fine"),
    deleted(4, 1),
    edited(5, 1, "sneaky"), // sequenced after the delete
    message(1, 102n, "b-1", "oops"), // history replaying the creation
  );
  const list = s.messages["7"]!;
  assert.deepEqual(
    list.map((m) => [m.msgSeq, m.body, m.deleted ?? false]),
    [
      [1, "", true],
      [2, "fine", false],
    ],
  );
});
