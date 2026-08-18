"""
A raw feed response (the exact bytes BKK sent back) is the one thing that
can never be regenerated once the moment has passed. Everything else --
the Parquet tables, any future re-parsing with better code -- can be
rebuilt from this. So it gets written first, before any parsing is even
attempted, and parsing failures never prevent it from being written.

File format: one file per (feed, UTC date). Each record is:
    8 bytes  : float64 unix timestamp (when the poll happened)
    4 bytes  : uint32 length of the gzip-compressed payload
    N bytes  : gzip-compressed raw protobuf bytes
Records are simply appended, so writing is an O(1) open-append-close and
reading is a straightforward sequential scan. No index, no database --
deliberately boring, because boring is what you want for the one file
you cannot afford to corrupt.
"""

from __future__ import annotations

import gzip
import struct
from pathlib import Path
from typing import BinaryIO, Iterator, Tuple

_HEADER = struct.Struct(">dI")  # timestamp (float64), length (uint32)


def append_record(path: Path, timestamp: float, raw_bytes: bytes) -> None:
    compressed = gzip.compress(raw_bytes, compresslevel=6)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "ab") as f:
        f.write(_HEADER.pack(timestamp, len(compressed)))
        f.write(compressed)


def iter_records(f: BinaryIO) -> Iterator[Tuple[float, bytes]]:
    """Yields (timestamp, raw_bytes) for every complete record in the file.
    Stops cleanly (without raising) if the file ends mid-record -- e.g. the
    process was killed mid-write -- rather than losing every record that
    came before the truncated one."""
    while True:
        header = f.read(_HEADER.size)
        if len(header) < _HEADER.size:
            return
        timestamp, length = _HEADER.unpack(header)
        payload = f.read(length)
        if len(payload) < length:
            return
        try:
            yield timestamp, gzip.decompress(payload)
        except OSError:
            # Corrupt record (rare, but possible after a hard crash mid-write).
            # Skip it and keep going rather than aborting the whole file.
            continue
