#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Generate placeholder xmsg desktop icons: an `x_` mark on the dark background colour."""
import struct
import zlib
import os

BG = (9, 13, 17)        # --bg #090d11
FG = (214, 221, 226)    # --fg #d6dde2

SS = 4  # supersampling factor


def render(size):
    n = size * SS
    buf = bytearray()
    for y in range(n):
        buf.append(0)  # filter byte
        for x in range(n):
            buf.extend(pixel(x / n, y / n))
            buf.append(255)  # opaque alpha

    def px(u, v):
        return buf[1 + (v * n + u) * 4:1 + (v * n + u) * 4 + 3]

    out = bytearray()
    for y in range(size):
        out.append(0)
        for x in range(size):
            r = g = b = 0
            for dy in range(SS):
                for dx in range(SS):
                    c = px(x * SS + dx, y * SS + dy)
                    r += c[0]; g += c[1]; b += c[2]
            k = SS * SS
            out.extend((r // k, g // k, b // k, 255))
    return out, size


def pixel(u, v):
    # Coordinates in a 0..1 box; mark occupies the central area.
    def sd_segment(px, py, ax, ay, bx, by):
        pax, pay = px - ax, py - ay
        bax, bay = bx - ax, by - ay
        h = max(0.0, min(1.0, (pax * bax + pay * bay) / (bax * bax + bay * bay)))
        dx, dy = pax - bax * h, pay - bay * h
        return (dx * dx + dy * dy) ** 0.5

    # Box of the glyph: x from .18 to .62, underscore .18..82 at y .70
    d = min(
        sd_segment(u, v, 0.20, 0.20, 0.56, 0.64),
        sd_segment(u, v, 0.56, 0.20, 0.20, 0.64),
    )
    w = 0.045  # half stroke width
    ink = 0.0
    if d < w + 0.01:
        ink = max(0.0, min(1.0, (w + 0.01 - d) / 0.02))
    # underscore bar: x .20..82, y .70..78
    if 0.20 <= u <= 0.82 and 0.70 <= v <= 0.78:
        ink = max(ink, min(1.0, min(u - 0.19, 0.83 - u, v - 0.69, 0.79 - v) / 0.02))
    return tuple(round(BG[i] + (FG[i] - BG[i]) * ink) for i in range(3))


def write_png(path, size, raw):
    def chunk(tag, data):
        c = tag + data
        return struct.pack(">I", len(data)) + c + struct.pack(">I", zlib.crc32(c))

    ihdr = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
        f.write(chunk(b"IHDR", ihdr))
        f.write(chunk(b"IDAT", zlib.compress(bytes(raw), 9)))
        f.write(chunk(b"IEND", b""))


here = os.path.dirname(os.path.abspath(__file__))
raw, size = render(1024)
write_png(os.path.join(here, "icon_1024.png"), size, raw)
for s in (32, 128, 256):
    r, _ = render(s)
    write_png(os.path.join(here, f"{s}x{s}.png" if s != 256 else "128x128@2x.png"), s, r)
print("icons written")
