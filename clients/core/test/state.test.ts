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

test("repeated and out-of-date events are ignored", () => {
  const once = apply(initialState(ME), created, message(1, 102n, "b-1", "hi"));
  assert.equal(apply(once, message(1, 102n, "b-1", "hi"), created), once);
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
