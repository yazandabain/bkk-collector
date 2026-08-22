"""Small, dependency-free primitives for durable local state.

All replacement writes happen in the destination directory so ``os.replace``
is atomic on the mounted filesystem.  Directory fsync is best-effort because
some filesystems (notably a few Docker Desktop mounts) do not support it.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any


def fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes, mode: int = 0o644) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def atomic_write_text(path: Path, text: str, mode: int = 0o644) -> None:
    atomic_write_bytes(path, text.encode("utf-8"), mode=mode)


def atomic_write_json(path: Path, value: Any, mode: int = 0o644) -> None:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2)
    atomic_write_text(path, payload + "\n", mode=mode)


def append_jsonl(path: Path, value: Any, *, fsync: bool = True) -> None:
    """Append one complete JSON line.

    Only one process may write a particular journal.  That is an explicit
    invariant of this project: the collector owns poll journals and the
    maintenance process owns backup/static journals.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.exists()
    line = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    with open(path, "ab", buffering=0) as handle:
        written = handle.write(line)
        if written != len(line):
            raise OSError(f"short journal append: wrote {written}/{len(line)} bytes")
        if fsync:
            os.fsync(handle.fileno())
    if fsync and not existed:
        fsync_directory(path.parent)


def read_json(path: Path, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return default


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(chunk_size)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()
