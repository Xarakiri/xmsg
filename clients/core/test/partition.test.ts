// SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { channelKey, murmur2, partition } from "../src/partition.ts";

const vectorsUrl = new URL("../../../spec/partitioning/vectors.csv", import.meta.url);

const toHex = (bytes: Uint8Array): string =>
  Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");

test("matches spec/partitioning/vectors.csv", () => {
  const [header, ...rows] = readFileSync(vectorsUrl, "utf8").trim().split("\n");
  const counts = header!.split(",").slice(3).map((c) => Number(c.slice(1)));

  for (const row of rows) {
    const cols = row.split(",");
    const channelId = BigInt(cols[0]!);
    const key = channelKey(channelId);
    assert.equal(toHex(key), cols[1], `key bytes for ${channelId}`);
    assert.equal(murmur2(key), Number(cols[2]), `murmur2 for ${channelId}`);
    counts.forEach((n, i) => {
      assert.equal(partition(channelId, n), Number(cols[3 + i]), `partition(${channelId}, ${n})`);
    });
  }
  assert.ok(rows.length > 200, `expected the full vector file, got ${rows.length} rows`);
});

test("rejects a non-positive partition count", () => {
  assert.throws(() => partition(1n, 0), RangeError);
});
