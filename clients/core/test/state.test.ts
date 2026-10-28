// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { create, type MessageInitShape } from "@bufbuild/protobuf";
import { SequencedEventSchema, type SequencedEvent } from "../src/gen/xmsg/v1/events_pb.js";
import { CallChange, ChannelType, MemberRole, RejectReason } from "../src/gen/xmsg/v1/enums_pb.js";
import { canPost, initialState, ringing, needsCatchUp, reduce, unread, unsent, type ChatState } from "../src/state.ts";

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
    mentions: [],
    roles: { "101": "admin", "102": "member" },
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
  s = reduce(s, { type: "send", channelId: "7", clientMsgId: "c-11", body: "z", at: 3 });
  s = reduce(s, { type: "sendRejected", clientMsgId: "c-11", reason: RejectReason.RATE_LIMITED, detail: "" });
  assert.match(s.messages["7"]![2]!.error!, /too fast/);
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

const mentioning = (seq: number, actor: bigint, ids: bigint[], everyone = false) =>
  ev({
    eventSeq: BigInt(seq + 1),
    msgSeq: BigInt(seq),
    actorId: actor,
    dedupeId: `m-${seq}`,
    payload: { case: "messageCreated", value: { messageId: BigInt(1000 + seq), body: "hey", mentionedUserIds: ids, mentionsChannel: everyone } },
  });

test("mentions of me or @channel count until read; my own @channel doesn't", async () => {
  const { mentionCount } = await import("../src/state.ts");
  let s = apply(
    initialState(ME),
    created,
    mentioning(1, 102n, [101n]),
    mentioning(2, 102n, [103n]), // someone else
    mentioning(3, 102n, [], true), // @channel
    mentioning(4, 101n, [], true), // mine
    mentioning(1, 102n, [101n]), // redelivered
  );
  assert.equal(mentionCount(s.channels["7"]!), 0, "my own send at msg_seq 4 marks everything before it read");
  s = apply(initialState(ME), created, mentioning(1, 102n, [101n]), mentioning(2, 102n, [103n]), mentioning(3, 102n, [], true));
  assert.deepEqual(s.channels["7"]!.mentions, [1, 3]);
  s = reduce(s, { type: "mentions", mentions: [{ channelId: "7", msgSeqs: [3, 5] }] });
  assert.deepEqual(s.channels["7"]!.mentions, [1, 3, 5], "merged as a set");
  s = reduce(s, { type: "markRead", channelId: "7", msgSeq: 3 });
  assert.equal(mentionCount(s.channels["7"]!), 1);
  assert.deepEqual(s.channels["7"]!.mentions, [5], "read ones are dropped");
});

const reacted = (eventSeq: number, msgSeq: number, emoji: string, count: number, added: bigint[], removed: bigint[] = []) =>
  ev({
    eventSeq: BigInt(eventSeq),
    actorId: 0n,
    payload: { case: "reactionsChanged", value: { messageId: BigInt(1000 + msgSeq), msgSeq: BigInt(msgSeq), emoji, count: BigInt(count), added, removed } },
  });

test("reactions: mine shows at once, the server's absolute count wins, older updates never undo newer", () => {
  let s = apply(initialState(ME), created, message(1, 102n, "b-1", "lunch?"));
  s = reduce(s, { type: "react", channelId: "7", msgSeq: 1, emoji: "👍", add: true });
  assert.deepEqual(s.messages["7"]![0]!.reactions, { "👍": { count: 1, mine: true, seq: 0 } });
  s = apply(s, reacted(3, 1, "👍", 2, [101n, 102n]), reacted(4, 1, "👍", 1, [], [102n]));
  assert.deepEqual(s.messages["7"]![0]!.reactions, { "👍": { count: 1, mine: true, seq: 4 } });
  s = apply(s, reacted(3, 1, "👍", 2, [101n, 102n])); // history replaying an older update
  assert.equal(s.messages["7"]![0]!.reactions!["👍"]!.count, 1);
  s = apply(s, reacted(5, 1, "👍", 0, [], [101n]));
  assert.deepEqual(s.messages["7"]![0]!.reactions, {}, "nobody has it any more");
  assert.equal(s.channels["7"]!.msgSeq, 1, "reactions don't count as messages");
});

test("thread replies live under the thread, count on the root, and never move msg_seq", async () => {
  const { threadKey } = await import("../src/state.ts");
  const root = { msgSeq: 1, messageId: "1001" };
  let s = apply(initialState(ME), created, message(1, 102n, "b-1", "plan?"));
  s = reduce(s, { type: "send", channelId: "7", clientMsgId: "r-1", body: "yes", at: 5, threadRoot: root });
  const key = threadKey("7", 1);
  assert.equal(s.messages[key]![0]!.status, "pending");
  const reply = (eventSeq: number, replySeq: number, actor: bigint, dedupeId: string, body: string) =>
    ev({
      eventSeq: BigInt(eventSeq),
      actorId: actor,
      dedupeId,
      payload: {
        case: "threadReply",
        value: { messageId: BigInt(5000 + replySeq), body, rootMsgSeq: 1n, rootMessageId: 1001n, replySeq: BigInt(replySeq), replyCount: BigInt(replySeq) },
      },
    });
  s = apply(s, reply(3, 1, 102n, "b-r", "me first"), reply(4, 2, 101n, "r-1", "yes"), reply(3, 1, 102n, "b-r", "me first"));
  assert.deepEqual(
    s.messages[key]!.map((m) => [m.msgSeq, m.body, m.status]),
    [
      [1, "me first", "confirmed"],
      [2, "yes", "confirmed"],
    ],
  );
  assert.equal(s.messages["7"]![0]!.replyCount, 2);
  assert.equal(s.channels["7"]!.msgSeq, 1, "replies aren't channel messages");
  s = reduce(s, { type: "threadLoaded", channelId: "7", root, replies: [{ replySeq: 3, messageId: "5003", authorId: "102", body: "late" }] });
  assert.deepEqual(s.messages[key]!.map((m) => m.msgSeq), [1, 2, 3]);
});

test("read receipts only move forward and never come from me", () => {
  let s = apply(initialState(ME), created);
  for (const [userId, msgSeq] of [["102", 3], ["102", 2], ["101", 9], ["102", 5]] as const) {
    s = reduce(s, { type: "receipt", channelId: "7", userId, msgSeq });
  }
  assert.deepEqual(s.channels["7"]!.seenBy, { "102": 5 });
});

test("bumps move a large channel's badge and mark it for catching up when opened", () => {
  let s = reduce(initialState(ME), { type: "event", event: created, at: 1 });
  const before = s.channels["7"]!;
  assert.equal(needsCatchUp(before), false);
  s = reduce(s, { type: "bump", channelId: "7", eventSeq: 9, msgSeq: 5 });
  const after = s.channels["7"]!;
  assert.equal(unread(after), 5);
  assert.equal(needsCatchUp(after), true);
  // Fetched (history through the head): caught up.
  s = reduce(s, { type: "history", channelId: "7", events: [], through: 9, at: 2 });
  assert.equal(needsCatchUp(s.channels["7"]!), false);
  // An unknown channel's bump is ignored (the next sync lists it).
  assert.deepEqual(reduce(s, { type: "bump", channelId: "99", eventSeq: 1, msgSeq: 1 }), s);
});

test("roles follow membership events, and only posters and admins post in broadcast channels", () => {
  const broadcast = (init: MessageInitShape<typeof SequencedEventSchema>) =>
    ev({ channelType: ChannelType.BROADCAST, ...init });
  let s = reduce(initialState(ME), {
    type: "event",
    at: 1,
    event: broadcast({
      eventSeq: 1n,
      actorId: 102n,
      payload: {
        case: "channelCreated",
        value: {
          type: ChannelType.BROADCAST,
          members: [
            { userId: 102n, role: MemberRole.ADMIN },
            { userId: 101n, role: MemberRole.MEMBER },
          ],
        },
      },
    }),
  });
  let channel = s.channels["7"]!;
  assert.deepEqual(channel.roles, { "102": "admin", "101": "member" });
  assert.equal(canPost(channel, ME), false);
  assert.equal(canPost(channel, BOB), true);

  s = reduce(s, {
    type: "event",
    at: 2,
    event: broadcast({ eventSeq: 2n, actorId: 102n, payload: { case: "memberRoleChanged", value: { userId: 101n, role: MemberRole.POSTER } } }),
  });
  channel = s.channels["7"]!;
  assert.equal(channel.roles?.[ME], "poster");
  assert.equal(canPost(channel, ME), true);

  s = reduce(s, { type: "event", at: 3, event: broadcast({ eventSeq: 3n, actorId: 102n, payload: { case: "memberLeft", value: { userId: 101n } } }) });
  assert.equal(s.channels["7"]!.roles?.[ME], undefined);
  // Elsewhere anyone posts; an unseen role doesn't hide the composer (the server decides).
  assert.equal(canPost({ ...channel, kind: "public" }, ME), true);
  assert.equal(canPost({ ...channel, roles: {} }, ME), true);
});

test("attachments: shown at once when sent, kept from the server's event, gone when deleted", () => {
  const file = { fileId: "42", name: "cat.png", contentType: "image/png", size: 10, width: 4, height: 3, hasThumbnail: true };
  let s = apply(initialState(ME), created);
  s = reduce(s, { type: "send", channelId: "7", clientMsgId: "c-1", body: "", at: 1, attachments: [file] });
  assert.deepEqual(s.messages["7"]![0]!.attachments, [file]);
  assert.deepEqual(unsent(s)[0]!.attachments, [file], "the outbox resends them");

  s = apply(
    s,
    ev({
      eventSeq: 2n,
      msgSeq: 1n,
      actorId: 101n,
      dedupeId: "c-1",
      payload: {
        case: "messageCreated",
        value: {
          messageId: 1001n,
          body: "",
          attachments: [{ fileId: 42n, name: "cat.png", contentType: "image/png", size: 10n, width: 4, height: 3, hasThumbnail: true }],
        },
      },
    }),
  );
  const confirmed = s.messages["7"]![0]!;
  assert.equal(confirmed.status, "confirmed");
  assert.deepEqual(confirmed.attachments, [file]);

  s = apply(s, ev({ eventSeq: 3n, actorId: 101n, payload: { case: "messageDeleted", value: { msgSeq: 1n, messageId: 1001n } } }));
  assert.equal(s.messages["7"]![0]!.attachments, undefined);
});

test("a message without files has no attachments field", () => {
  const s = apply(initialState(ME), created, message(1, 102n, "", "hi"));
  assert.equal("attachments" in s.messages["7"]![0]!, false);
});

test("thread replies carry files, live and when the thread is loaded", () => {
  const file = { fileId: "42", name: "cat.png", contentType: "image/png", size: 10, width: 4, height: 3, hasThumbnail: true };
  let s = apply(initialState(ME), created, message(1, 102n, "", "root"));
  s = apply(
    s,
    ev({
      eventSeq: 3n,
      actorId: 102n,
      payload: {
        case: "threadReply",
        value: {
          messageId: 2001n,
          body: "",
          rootMsgSeq: 1n,
          rootMessageId: 1001n,
          replySeq: 1n,
          replyCount: 1n,
          attachments: [{ fileId: 42n, name: "cat.png", contentType: "image/png", size: 10n, width: 4, height: 3, hasThumbnail: true }],
        },
      },
    }),
  );
  assert.deepEqual(s.messages["7#1"]![0]!.attachments, [file]);

  s = reduce(s, {
    type: "threadLoaded",
    channelId: "7",
    root: { msgSeq: 1, messageId: "1001" },
    replies: [{ replySeq: 2, messageId: "2002", authorId: "102", body: "", attachments: [file] }],
  });
  assert.deepEqual(s.messages["7#1"]![1]!.attachments, [file]);
});

test("calls: a DM call rings until I'm in it; huddles never ring; the end clears it", () => {
  const callEvent = (seq: number, change: CallChange, participants: bigint[], callId = "c7-ab", huddle = false) =>
    ev({ eventSeq: BigInt(seq), actorId: 102n, payload: { case: "callEvent", value: { callId, change, huddle, participantIds: participants, startedAtMs: 5n } } });
  let s = apply(initialState(ME), created, callEvent(2, CallChange.STARTED, [102n]));
  assert.deepEqual(s.channels["7"]!.call, { callId: "c7-ab", huddle: false, participants: ["102"], startedAt: 5 });
  assert.equal(ringing(s.channels["7"]!, ME), true, "bob is calling me");
  s = apply(s, callEvent(3, CallChange.PARTICIPANTS, [102n, 101n]));
  assert.equal(ringing(s.channels["7"]!, ME), false, "I joined");
  // A late end of another call changes nothing; this call's end clears it.
  s = apply(s, callEvent(4, CallChange.ENDED, [], "c7-old"));
  assert.equal(s.channels["7"]!.call?.callId, "c7-ab");
  s = apply(s, callEvent(5, CallChange.ENDED, []));
  assert.equal(s.channels["7"]!.call, undefined);
  // A huddle with someone in it doesn't ring.
  s = apply(s, callEvent(6, CallChange.STARTED, [102n], "h7", true));
  assert.equal(ringing(s.channels["7"]!, ME), false);
});
