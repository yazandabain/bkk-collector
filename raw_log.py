"""
A raw feed response (the exact bytes BKK sent back) is the one thing that
can never be regenerated once the moment has passed. Responses selected by
the feed's configured raw cadence are written before parsing. A parse/spool
failure also forces that poll into the raw archive, even when it was not due.

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
import os
import shutil
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterator, Tuple

from atomic_io import atomic_write_json, fsync_directory, read_json

_HEADER = struct.Struct(">dI")  # timestamp (float64), length (uint32)
MAX_COMPRESSED_RECORD_BYTES = 256 * 1024 * 1024


def _checkpoint_path(path: Path) -> Path:
    return path.with_name(f"{path.name}.checkpoint.json")


def append_record(path: Path, timestamp: float, raw_bytes: bytes, *, fsync: bool = True) -> None:
    compressed = gzip.compress(raw_bytes, compresslevel=6)
    if len(compressed) > MAX_COMPRESSED_RECORD_BYTES:
        raise ValueError(f"compressed raw record is unexpectedly large: {len(compressed)} bytes")
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.exists()
    record = _HEADER.pack(timestamp, len(compressed)) + compressed
    with open(path, "ab", buffering=0) as f:
        written = f.write(record)
        if written != len(record):
            raise OSError(f"short raw-log append: wrote {written}/{len(record)} bytes")
        if fsync:
            os.fsync(f.fileno())
    if fsync and not existed:
        fsync_directory(path.parent)
    if fsync:
        atomic_write_json(
            _checkpoint_path(path),
            {"version": 1, "valid_bytes": path.stat().st_size, "updated_at_timestamp": timestamp},
        )


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
        if length > MAX_COMPRESSED_RECORD_BYTES:
            return
        payload = f.read(length)
        if len(payload) < length:
            return
        try:
            yield timestamp, gzip.decompress(payload)
        except (OSError, EOFError):
            # Corrupt record (rare, but possible after a hard crash mid-write).
            # Skip it and keep going rather than aborting the whole file.
            continue


@dataclass(frozen=True)
class RawLogScan:
    complete_records: int
    valid_bytes: int
    file_bytes: int
    clean: bool


def scan_raw_log(path: Path, *, validate_gzip: bool = True) -> RawLogScan:
    """Return the last definitely complete record boundary."""
    count = 0
    valid_end = 0
    file_size = path.stat().st_size
    with open(path, "rb") as handle:
        while True:
            start = handle.tell()
            header = handle.read(_HEADER.size)
            if not header:
                return RawLogScan(count, valid_end, file_size, valid_end == file_size)
            if len(header) != _HEADER.size:
                return RawLogScan(count, valid_end, file_size, False)
            _timestamp, length = _HEADER.unpack(header)
            if length > MAX_COMPRESSED_RECORD_BYTES:
                return RawLogScan(count, valid_end, file_size, False)
            payload = handle.read(length)
            if len(payload) != length:
                return RawLogScan(count, valid_end, file_size, False)
            if validate_gzip:
                try:
                    gzip.decompress(payload)
                except (OSError, EOFError):
                    return RawLogScan(count, valid_end, file_size, False)
            count += 1
            valid_end = handle.tell()
            if valid_end <= start:
                raise RuntimeError("raw log scanner did not advance")


def repair_truncated_tail(path: Path) -> Path | None:
    """Preserve and detach an invalid tail before future appends.

    The detached bytes are never discarded.  They are copied beside the raw
    log for forensic/manual recovery, then only the proven-valid prefix stays
    in the append target.
    """
    if not path.exists():
        return None
    checkpoint = read_json(_checkpoint_path(path), {})
    checkpoint_bytes = checkpoint.get("valid_bytes") if checkpoint.get("version") == 1 else None
    if isinstance(checkpoint_bytes, int) and 0 <= checkpoint_bytes <= path.stat().st_size:
        # A successfully fsynced append writes this checkpoint afterward. Only
        # bytes beyond it can belong to an interrupted later append.
        if checkpoint_bytes == path.stat().st_size:
            return None
        scan = _scan_raw_log_from(path, checkpoint_bytes)
    else:
        scan = scan_raw_log(path)
    if scan.clean:
        atomic_write_json(
            _checkpoint_path(path),
            {"version": 1, "valid_bytes": scan.valid_bytes, "updated_at_timestamp": None},
        )
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    recovery = path.with_name(f"{path.name}.corrupt-tail-{stamp}")
    with open(path, "rb") as source, open(recovery, "xb") as target:
        source.seek(scan.valid_bytes)
        shutil.copyfileobj(source, target)
        target.flush()
        os.fsync(target.fileno())
    with open(path, "r+b") as handle:
        handle.truncate(scan.valid_bytes)
        handle.flush()
        os.fsync(handle.fileno())
    atomic_write_json(
        _checkpoint_path(path),
        {"version": 1, "valid_bytes": scan.valid_bytes, "updated_at_timestamp": None},
    )
    return recovery


def _scan_raw_log_from(path: Path, offset: int) -> RawLogScan:
    """Validate only records after a previously fsynced boundary."""
    count = 0
    valid_end = offset
    file_size = path.stat().st_size
    with open(path, "rb") as handle:
        handle.seek(offset)
        while True:
            header = handle.read(_HEADER.size)
            if not header:
                return RawLogScan(count, valid_end, file_size, valid_end == file_size)
            if len(header) != _HEADER.size:
                return RawLogScan(count, valid_end, file_size, False)
            _timestamp, length = _HEADER.unpack(header)
            if length > MAX_COMPRESSED_RECORD_BYTES:
                return RawLogScan(count, valid_end, file_size, False)
            payload = handle.read(length)
            if len(payload) != length:
                return RawLogScan(count, valid_end, file_size, False)
            try:
                gzip.decompress(payload)
            except (OSError, EOFError):
                return RawLogScan(count, valid_end, file_size, False)
            count += 1
            valid_end = handle.tell()
