"""Single-writer JSONL append recovery; forensic bytes are never discarded."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from pathlib import Path

from atomic_io import append_jsonl, atomic_write_bytes, atomic_write_json, fsync_directory, read_json


def recent_partition_files(root: Path, filename: str, today: str) -> list[Path]:
    """Today's file and the latest prior existing file, not a historical scan."""
    found = []
    for directory in root.glob("date=*"):
        date = directory.name.removeprefix("date=")
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            continue
        path = directory / filename
        if date <= today and path.is_file():
            found.append((date, path))
    prior = [item for item in found if item[0] < today]
    return [path for date, path in sorted(found) if date == today or (prior and (date, path) == max(prior))]


def _checkpoint(path: Path) -> Path:
    return path.with_name(path.name + ".checkpoint.json")


def _last_line_start(handle, size: int) -> int:
    # Skip the final separator while looking for the preceding separator.
    end = size
    if end:
        handle.seek(end - 1)
        if handle.read(1) == b"\n":
            end -= 1
    while end:
        start = max(0, end - 4096)
        handle.seek(start)
        block = handle.read(end - start)
        separator = block.rfind(b"\n")
        if separator >= 0:
            return start + separator + 1
        end = start
    return 0


def repair_jsonl_tail(path: Path) -> Path | None:
    """Detach only an invalid final line, retaining complete records.

    A checkpoint proves the fsynced prefix. Legacy journals need only their
    final line checked here; full manifest validation still detects interior
    corruption. Invalid interior lines are never silently removed or repaired.
    """
    if not path.exists():
        return None
    size = path.stat().st_size
    checkpoint = read_json(_checkpoint(path), {})
    valid = checkpoint.get("valid_bytes") if checkpoint.get("version") == 1 else None
    if type(valid) is int and 0 <= valid <= size:
        if valid == size:
            return None
        start = valid
    else:
        with path.open("rb") as handle:
            start = _last_line_start(handle, size)
    recovery = None
    add_separator = False
    with path.open("rb") as handle:
        handle.seek(start)
        while handle.tell() < size:
            line_start = handle.tell()
            line = handle.readline()
            try:
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise ValueError("journal event is not an object")
            except (ValueError, UnicodeError):
                if handle.tell() != size:
                    raise ValueError("journal has interior corruption; refusing destructive repair")
                recovery = path.with_name(path.name + ".corrupt-tail-" + uuid.uuid4().hex)
                atomic_write_bytes(recovery, line)
                # Ordinary atomic writes allow unsupported directory syncs.
                # Destructive recovery cannot: keep the original until its
                # forensic replacement has a confirmed durable directory entry.
                fsync_directory(path.parent, strict=True)
                size = line_start
                break
            add_separator = not line.endswith(b"\n")
    if recovery is not None or add_separator:
        with path.open("r+b") as handle:
            handle.truncate(size)
            if add_separator and recovery is None:
                handle.seek(size)
                handle.write(b"\n")  # retain a complete JSON event interrupted before its separator
                size += 1
            handle.flush()
            os.fsync(handle.fileno())
    atomic_write_json(_checkpoint(path), {"version": 1, "valid_bytes": size})
    return recovery


def append_poll_jsonl(path: Path, event: dict) -> None:
    repair_jsonl_tail(path)
    append_jsonl(path, event, fsync=True)
    atomic_write_json(_checkpoint(path), {"version": 1, "valid_bytes": path.stat().st_size})
