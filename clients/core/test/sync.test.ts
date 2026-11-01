// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { create } from "@bufbuild/protobuf";
import { EventPageSchema, type EventPage } from "../src/gen/xmsg/v1/sync_pb.js";
import { ChannelType } from "../src/gen/xmsg/v1/enums_pb.js";
import { ApiError, type Api } from "../src/api.ts";
import { initialState, reduce, type Action, type ChatState, type ListedChannel } from "../src/state.ts";
import { inPartition, sync } from "../src/sync.ts";
import { partition } from "../src/partition.ts";

const msg = (seq: number) => ({
  channelId: 7n,
  channelType: ChannelType.PUBLIC,
  eventSeq: BigInt(seq),
  msgSeq: BigInt(seq),
  actorId: 102n,
  payload: { case: "messageCreated" as const, value: { messageId: BigInt(seq), body: `m${seq}` } },
});

/** Channel 7 has events 1..450; channel 8 refuses us (we were removed). */
function fakeApi(calls: string[]): Api {
  const listed: ListedChannel[] = [
    { id: "7", kind: "public", name: "big", eventSeq: 450, msgSeq: 450 },
    { id: "8", kind: "private", name: "gone", eventSeq: 3, msgSeq: 1 },
  ];
  return {
    myChannels: async () => listed,
    readState: async () => [{ channelId: "7", msgSeq: 400 }],
    settings: async () => [{ channelId: "7", muted: true, notifyLevel: "mentions" }],
    // 399 is before the read position (already read); 420 is unread.
    mentions: async () => [{ channelId: "7", msgSeqs: [399, 420] }],
    events: async (_t: string, id: string, after: number, limit: number): Promise<EventPage> => {
      calls.push(`${id}@${after}`);
      if (id === "8") throw new ApiError(404, "unknown channel");
      const seqs = Array.from({ length: Math.min(limit, 450 - after) }, (_, i) => after + 1 + i);
      return create(EventPageSchema, { events: seqs.map(msg), hasMore: after + seqs.length < 450, headEventSeq: 450n });
    },
    eventsBefore: async (_t: string, id: string, before: number, limit: number): Promise<EventPage> => {
      calls.push(`${id}|${before}`);
      if (id === "8") throw new ApiError(404, "unknown channel");
      const seqs = Array.from({ length: Math.min(limit, before - 1) }, (_, i) => before - limit + i).filter((s) => s >= 1);
      return create(EventPageSchema, {
        events: seqs.map(msg),
        hasMore: seqs.length > 0 && seqs[0]! > 1,
        headEventSeq: 450n,
        truncated: seqs.length < Math.min(limit, before - 1),
      });
    },
  } as unknown as Api;
}

test("sync opens a long channel at the first unread page, not from sequence 0", async () => {
  let state: ChatState = initialState("101");
  const calls: string[] = [];
  const fetched = await sync({
    api: fakeApi(calls),
    token: async () => "t",
    state: () => state,
    dispatch: (a: Action) => (state = reduce(state, a)),
  });
  assert.deepEqual(calls, ["7|451", "8|4"], "one backward page holding the first unread; the refused channel walks back too");
  assert.equal(fetched, 200, "only the page around the first unread, not all 450 events");
  const channel = state.channels["7"]!;
  assert.equal(channel.syncedSeq, 450, "the backward page reaches the head, so sync is caught up");
  assert.equal(channel.earliestSeq, 251);
  assert.equal(channel.hasEarlier, true, "older pages come in on scroll-up");
  assert.equal(state.messages["7"]!.length, 200);
  assert.equal(state.messages["7"]![0]!.msgSeq, 251);
  assert.equal(state.messages["7"]!.at(-1)!.msgSeq, 450);
  assert.equal(state.channels["8"]!.left, true);
  assert.equal(channel.readSeq, 400, "read positions come with the sync");
  assert.equal(channel.muted, true, "and personal settings");
  assert.deepEqual(channel.mentions, [420], "and unread mentions");
  assert.equal(channel.msgSeq - channel.readSeq, 50, "unread count stays right with a partial load");

  // Nothing moved: a second sync fetches no events.
  calls.length = 0;
  await sync({ api: fakeApi(calls), token: async () => "t", state: () => state, dispatch: (a) => (state = reduce(state, a)) });
  assert.deepEqual(calls, ["8|4"], "only the refused channel is retried");
});

test("a caught-up channel opens at the newest page only", async () => {
  const api = fakeApi([]);
  api.readState = async () => [{ channelId: "7", msgSeq: 450 }];
  const inner: string[] = [];
  const realEvents: Api["events"] = api.events!.bind(api);
  api.events = ((...args: [string, string, number, number?]) => (inner.push("events"), realEvents(...args))) as Api["events"];
  let state: ChatState = initialState("101");
  await sync({
    api,
    token: async () => "t",
    state: () => state,
    dispatch: (a: Action) => (state = reduce(state, a)),
  });
  assert.equal(state.messages["7"]!.length, 200);
  assert.equal(state.channels["7"]!.hasEarlier, true);
  assert.deepEqual(inner, [], "no forward catch-up after the newest page");
  assert.equal(state.channels["7"]!.syncedSeq, 450);
});

test("a partition-scoped resync only fetches that partition's channels", async () => {
  let state: ChatState = initialState("101");
  const calls: string[] = [];
  const p7 = partition(7n, 16);
  await sync({
    api: fakeApi(calls),
    token: async () => "t",
    state: () => state,
    dispatch: (a: Action) => (state = reduce(state, a)),
    only: inPartition(p7, 16),
  });
  const fetched = new Set(calls.map((c) => c.split(/[@|]/)[0]));
  assert.ok(fetched.has("7"));
  assert.equal(fetched.has("8"), partition(8n, 16) === p7, "channel 8 only if it shares the partition");
});

test("the scheduler runs one sync at a time and merges what was asked meanwhile", async () => {
  const { SyncScheduler } = await import("../src/sync.ts");
  const runs: string[] = [];
  let release!: () => void;
  const first = new Promise<void>((r) => (release = r));
  let n = 0;
  const scheduler = new SyncScheduler(async (only) => {
    n += 1;
    runs.push(only ? ["1", "2", "3"].filter(only).join(",") : "all");
    if (n === 1) await first;
  });
  scheduler.request((id) => id === "1"); // starts at once
  scheduler.request((id) => id === "2"); // both pending scoped requests merge...
  scheduler.request((id) => id === "3");
  release();
  await new Promise((r) => setTimeout(r, 10));
  assert.deepEqual(runs, ["1", "2,3"], "...into one follow-up run");

  runs.length = 0;
  n = 0;
  let release2!: () => void;
  const blocked = new Promise<void>((r) => (release2 = r));
  const s2 = new SyncScheduler(async (only) => {
    n += 1;
    runs.push(only ? "scoped" : "all");
    if (n === 1) await blocked;
  });
  s2.request((id) => id === "1");
  s2.request((id) => id === "2");
  s2.request(); // a full request swallows the scoped one
  release2();
  await new Promise((r) => setTimeout(r, 10));
  assert.deepEqual(runs, ["scoped", "all"]);
});
