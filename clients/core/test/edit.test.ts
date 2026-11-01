// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { initialState, reduce, type Channel, type Message } from "../src/state.ts";

const channel: Channel = { id: "9", kind: "public", name: "general", members: ["1"], eventSeq: 1, syncedSeq: 1, membershipSeq: 1, msgSeq: 1, readSeq: 1, muted: false, notifyLevel: "all", left: false, mentions: [] };
const message: Message = { key: "seq:1", channelId: "9", authorId: "1", body: "hello", msgSeq: 1, status: "confirmed", at: 1 };

test("editLocal shows the new body at once; editFailed puts the old one back", () => {
  const base = { ...initialState("1"), channels: { "9": channel }, messages: { "9": [message] } };
  const edited = reduce(base, { type: "editLocal", channelId: "9", msgSeq: 1, body: "edited", at: 5 });
  assert.equal(edited.messages["9"]![0]!.body, "edited");
  assert.equal(edited.messages["9"]![0]!.editedAt, 5);
  const rolled = reduce(edited, { type: "editFailed", channelId: "9", msgSeq: 1, body: "hello" });
  assert.equal(rolled.messages["9"]![0]!.body, "hello");
  assert.equal(rolled.messages["9"]![0]!.editedAt, undefined);
  assert.equal(rolled.messages["9"]![0]!.error, "not sent");
});
