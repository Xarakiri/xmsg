#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Generate spec/partitioning/vectors.csv.

A direct port of Kafka's Utils.murmur2 / Utils.toPositive. The Kotlin test in
services/sequencer cross-checks every row against Kafka's own implementation,
so this script is never the only source of truth.
"""
import struct
import sys
from pathlib import Path

COUNTS = (8, 16, 64, 512)


def to_i32(x: int) -> int:
    x &= 0xFFFFFFFF
    return x - (1 << 32) if x & 0x80000000 else x


def murmur2(data: bytes) -> int:
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24
    h = (seed ^ length) & 0xFFFFFFFF
    for i in range(length // 4):
        k = int.from_bytes(data[i * 4 : i * 4 + 4], "little")
        k = (k * m) & 0xFFFFFFFF
        k ^= k >> r
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k
    rest = length % 4
    base = length & ~3
    if rest == 3:
        h ^= data[base + 2] << 16
    if rest >= 2:
        h ^= data[base + 1] << 8
    if rest >= 1:
        h ^= data[base]
        h = (h * m) & 0xFFFFFFFF
    h ^= h >> 13
    h = (h * m) & 0xFFFFFFFF
    h ^= h >> 15
    return to_i32(h)


def key(channel_id: int) -> bytes:
    return struct.pack(">q", channel_id)


def ids():
    fixed = [0, 1, 2, 255, 256, 65535, 2**31 - 1, 2**31, 2**32, 2**53 + 1, 2**62,
             2**63 - 1, -1, -(2**63), 1_234_567_890_123_456_789]
    yield from fixed
    # Deterministic pseudo-random channel IDs (64-bit LCG), including Snowflake-shaped values.
    x = 0x2545F4914F6CDD1D
    for _ in range(200):
        x = (x * 6364136223846793005 + 1442695040888963407) & 0xFFFFFFFFFFFFFFFF
        yield x >> 1  # positive int64


def main() -> None:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "spec/partitioning/vectors.csv")
    lines = ["channel_id,key_hex,murmur2," + ",".join(f"p{n}" for n in COUNTS)]
    for cid in ids():
        k = key(cid)
        h = murmur2(k)
        pos = h & 0x7FFFFFFF
        lines.append(f"{cid},{k.hex()},{h}," + ",".join(str(pos % n) for n in COUNTS))
    out.write_text("\n".join(lines) + "\n")
    print(f"wrote {len(lines) - 1} vectors to {out}")


if __name__ == "__main__":
    main()
