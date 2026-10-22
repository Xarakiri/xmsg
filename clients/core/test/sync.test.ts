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
    events: async (_t: string, id: string, after: number, limit: number): Promise<EventPage> => {
      calls.push(`${id}@${after}`);
      if (id === "8") throw new ApiError(404, "unknown channel");
      const seqs = Array.from({ length: Math.min(limit, 450 - after) }, (_, i) => after + 1 + i);
      return create(EventPageSchema, { events: seqs.map(msg), hasMore: after + seqs.length < 450, headEventSeq: 450n });
    },
  } as unknown as Api;
}

test("sync pages through moved channels and marks refused ones as left", async () => {
  let state: ChatState = initialState("101");
  const calls: string[] = [];
  const fetched = await sync({
    api: fakeApi(calls),
    token: async () => "t",
    state: () => state,
    dispatch: (a: Action) => (state = reduce(state, a)),
  });
  assert.equal(fetched, 450);
  assert.deepEqual(calls.sort(), ["7@0", "7@200", "7@400", "8@0"]);
  assert.equal(state.channels["7"]!.syncedSeq, 450);
  assert.equal(state.messages["7"]!.length, 450);
  assert.equal(state.channels["8"]!.left, true);
  assert.equal(state.channels["7"]!.readSeq, 400, "read positions come with the sync");
  assert.equal(state.channels["7"]!.muted, true, "and personal settings");

  // Nothing moved: a second sync fetches no events.
  calls.length = 0;
  await sync({ api: fakeApi(calls), token: async () => "t", state: () => state, dispatch: (a) => (state = reduce(state, a)) });
  assert.deepEqual(calls, ["8@0"], "only the refused channel is retried");
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
  const fetched = new Set(calls.map((c) => c.split("@")[0]));
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
