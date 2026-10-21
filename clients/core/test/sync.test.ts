// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { create } from "@bufbuild/protobuf";
import { EventPageSchema, type EventPage } from "../src/gen/xmsg/v1/sync_pb.js";
import { ChannelType } from "../src/gen/xmsg/v1/enums_pb.js";
import { ApiError, type Api } from "../src/api.ts";
import { initialState, reduce, type Action, type ChatState, type ListedChannel } from "../src/state.ts";
import { sync } from "../src/sync.ts";

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
    { id: "7", kind: "public", name: "big", eventSeq: 450 },
    { id: "8", kind: "private", name: "gone", eventSeq: 3 },
  ];
  return {
    myChannels: async () => listed,
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

  // Nothing moved: a second sync fetches no events.
  calls.length = 0;
  await sync({ api: fakeApi(calls), token: async () => "t", state: () => state, dispatch: (a) => (state = reduce(state, a)) });
  assert.deepEqual(calls, ["8@0"], "only the refused channel is retried");
});
