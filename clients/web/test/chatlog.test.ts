// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import type { Channel, Message } from "@xmsg/client-core";
import { autocomplete, dateLabel, draftKey, groupMessages, unreadTotal } from "../src/chatlog.ts";

const NOW = new Date(2026, 9, 31, 12, 0, 0).getTime();
const msg = (over: Partial<Message>): Message => ({ key: "k", channelId: "c", authorId: "1", body: "", msgSeq: 1, status: "confirmed", at: NOW, ...over });

test("date labels: today, yesterday, then the day", () => {
  assert.equal(dateLabel(NOW, NOW - 60_000), "today");
  assert.equal(dateLabel(NOW, NOW - 86_400_000), "yesterday");
  assert.equal(dateLabel(NOW, new Date(2026, 9, 28, 8, 0, 0).getTime()), "28 oct");
});

test("grouping collapses same-sender runs within five minutes", () => {
  const t = NOW - 10 * 60_000;
  const grouped = groupMessages(
    [
      msg({ key: "a", authorId: "1", at: t, msgSeq: 1 }),
      msg({ key: "b", authorId: "1", at: t + 60_000, msgSeq: 2 }),
      msg({ key: "c", authorId: "2", at: t + 120_000, msgSeq: 3 }),
      msg({ key: "d", authorId: "1", at: t + 6 * 60_000, msgSeq: 4 }),
    ],
    NOW,
  );
  assert.deepEqual(grouped.map((g) => g.start), [true, false, true, true], "same sender within 5 min hides the header");
});

test("a new day always starts a group and gets a separator", () => {
  const t = NOW - 3 * 86_400_000;
  const grouped = groupMessages([msg({ key: "a", at: t, msgSeq: 1 }), msg({ key: "b", at: t + 60_000, msgSeq: 2 })], NOW);
  assert.equal(grouped[0]!.dateSeparator, "28 oct");
  assert.equal(grouped[1]!.dateSeparator, undefined);
  assert.equal(grouped[1]!.start, false);
});

test("draft keys distinguish channels and threads", () => {
  assert.equal(draftKey("42"), "xmsg.draft.42");
  assert.equal(draftKey("42", 7), "xmsg.draft.42.t7");
  assert.equal(draftKey("42", null), "xmsg.draft.42");
});

test("unread total skips muted channels", () => {
  const channel = (msgSeq: number, readSeq: number, muted = false) => ({ msgSeq, readSeq, muted } as Channel);
  assert.equal(unreadTotal([channel(5, 2), channel(3, 3), channel(9, 4, true)]), 3);
});

test("autocomplete by trigger", () => {
  const context = {
    members: [{ id: "2", name: "aiko" }, { id: "3", name: "marcus" }],
    channels: [{ id: "9", name: "general" }],
    commands: [{ name: "huddle", hint: "start a huddle" }],
  };
  assert.deepEqual(autocomplete("@", "ai", context).map((i) => i.insert), ["@aiko "]);
  assert.deepEqual(autocomplete("@", "", context).length, 2, "empty query shows everyone");
  assert.deepEqual(autocomplete("#", "gen", context).map((i) => i.insert), ["#general "]);
  assert.deepEqual(autocomplete("/", "", context).map((i) => i.label), ["/huddle"]);
  assert.deepEqual(autocomplete("#", "gen", context).map((i) => i.label), ["general"]);
});
