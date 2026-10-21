// SPDX-License-Identifier: Apache-2.0
/**
 * Kafka-compatible partitioning of channel IDs (spec/partitioning).
 * Clients use it for partition-scoped resync (ADR-0009, ADR-0021).
 * Channel IDs are 64-bit, so they are passed as bigint.
 */

const SEED = 0x9747b28c;
const M = 0x5bd1e995;
const R = 24;

/** Kafka's Utils.murmur2: the same signed 32-bit result as the Java code. */
export function murmur2(data: Uint8Array): number {
  const length = data.length;
  let h = (SEED ^ length) | 0;
  const length4 = length >>> 2;

  for (let i = 0; i < length4; i++) {
    const i4 = i * 4;
    let k =
      data[i4]! | (data[i4 + 1]! << 8) | (data[i4 + 2]! << 16) | (data[i4 + 3]! << 24);
    k = Math.imul(k, M);
    k ^= k >>> R;
    k = Math.imul(k, M);
    h = Math.imul(h, M);
    h ^= k;
  }

  const base = length & ~3;
  switch (length % 4) {
    case 3:
      h ^= data[base + 2]! << 16;
    // falls through
    case 2:
      h ^= data[base + 1]! << 8;
    // falls through
    case 1:
      h ^= data[base]!;
      h = Math.imul(h, M);
  }

  h ^= h >>> 13;
  h = Math.imul(h, M);
  h ^= h >>> 15;
  return h | 0;
}

/** The Kafka key for a channel: its ID as 8-byte big-endian two's complement. */
export function channelKey(channelId: bigint): Uint8Array {
  const key = new Uint8Array(8);
  new DataView(key.buffer).setBigInt64(0, BigInt.asIntN(64, channelId), false);
  return key;
}

/** Partition of `channelId` in a topic with `partitionCount` partitions. */
export function partition(channelId: bigint, partitionCount: number): number {
  if (!Number.isInteger(partitionCount) || partitionCount <= 0) {
    throw new RangeError("partitionCount must be a positive integer");
  }
  return (murmur2(channelKey(channelId)) & 0x7fffffff) % partitionCount;
}
