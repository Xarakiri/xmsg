// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import type { UserProfile } from "../src/api.ts";
import { Directory, MAX_USER_BATCH } from "../src/users.ts";

const tick = () => new Promise((resolve) => setTimeout(resolve, 0));

/** Knows users 1..1000 except 7, who shares no workspace with us. */
function fakeUsers(calls: string[][], fail = { now: false }) {
  return async (ids: string[]): Promise<UserProfile[]> => {
    calls.push(ids);
    if (fail.now) throw new Error("offline");
    return ids.filter((id) => id !== "7").map((id) => ({ user_id: id, name: `User ${id}`, avatar_url: null }));
  };
}

test("names are fetched once, in batches, with short IDs until then", async () => {
  const calls: string[][] = [];
  const directory = new Directory(fakeUsers(calls));
  let notified = 0;
  directory.subscribe(() => notified++);

  assert.equal(directory.label("123456789"), "…456789");
  assert.equal(directory.label("7"), "…7");
  directory.want(["123456789", "8"]);
  await tick();
  assert.deepEqual(calls, [["123456789", "7", "8"]], "one call for everything asked in a tick");
  assert.equal(directory.label("123456789"), "User 123456789");
  assert.equal(directory.label("7"), "…7", "invisible users stay short IDs");
  assert.ok(notified >= 1 && directory.version >= 1);

  directory.label("8");
  directory.label("7");
  await tick();
  assert.equal(calls.length, 1, "known and invisible users aren't asked again");

  const many = Array.from({ length: 450 }, (_, i) => String(1000 + i));
  directory.want(many);
  await tick();
  assert.deepEqual(
    calls.slice(1).map((c) => c.length),
    [MAX_USER_BATCH, MAX_USER_BATCH, 50],
  );
});

test("a failed fetch is retried later and remembered profiles win", async () => {
  const calls: string[][] = [];
  const fail = { now: true };
  const directory = new Directory(fakeUsers(calls, fail));
  directory.label("5");
  await tick();
  assert.equal(directory.label("5"), "…5");
  fail.now = false;
  await tick();
  assert.equal(directory.label("5"), "User 5");
  assert.equal(calls.length, 2);

  directory.remember({ user_id: "5", name: "Ada", avatar_url: null });
  assert.equal(directory.label("5"), "Ada");
});
