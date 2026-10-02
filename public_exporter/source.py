"""Bounded readers. These never repair, append to, or lock collector files."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import math
import re
import struct
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

HEADER = struct.Struct(">dI")
MAX_FRAME_BYTES = 2 * 1024 * 1024
MAX_PROTOBUF_BYTES = 8 * 1024 * 1024
DATE = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")


def read_object(path: Path, limit: int = 8 * 1024 * 1024) -> dict[str, Any]:
    with path.open("rb") as handle:
        payload = handle.read(limit + 1)
    if len(payload) > limit:
        raise ValueError("metadata exceeds size limit")
    value = json.loads(payload)
    if not isinstance(value, dict):
        raise ValueError("metadata is not an object")
    return value


class LatestVehicleFrame:
    """Seek through headers once, then only new committed records.

    A checkpoint bounds every read. Uncommitted crash tails are ignored, not
    repaired. Only the latest frame is decompressed; old full days never enter
    memory. Cached data retains its original response timestamp.
    """

    def __init__(self, root: Path):
        self.root = root
        self.identity: tuple[Any, ...] | None = None
        self.offset = 0
        self.latest: tuple[float, bytes] | None = None

    def read(self, today: date) -> tuple[float, bytes]:
        candidates = []
        for path in self.root.glob("date=*/vehiclepositions.rawlog"):
            match = DATE.fullmatch(path.parent.name)
            if match and date.fromisoformat(match[1]) <= today:
                candidates.append(path)
        if not candidates:
            raise FileNotFoundError("no vehicle observation")
        path = max(candidates)
        checkpoint = read_object(path.with_name(path.name + ".checkpoint.json"), 4096)
        end = checkpoint.get("valid_bytes")
        if checkpoint.get("version") != 1 or type(end) is not int or end < 0:
            raise ValueError("invalid raw checkpoint")
        with path.open("rb") as handle:
            import os

            stat = os.fstat(handle.fileno())
            if end > stat.st_size:
                raise ValueError("checkpoint exceeds raw file")
            identity = (path, stat.st_dev, stat.st_ino)
            if identity != self.identity or end < self.offset:
                self.identity, self.offset, self.latest = identity, 0, None
            handle.seek(self.offset)
            last_header = None
            while handle.tell() < end:
                start = handle.tell()
                header = handle.read(HEADER.size)
                if len(header) != HEADER.size:
                    raise ValueError("checkpoint splits a header")
                timestamp, length = HEADER.unpack(header)
                if not math.isfinite(timestamp) or length == 0 or length > MAX_FRAME_BYTES:
                    raise ValueError("invalid vehicle frame")
                if start + HEADER.size + length > end:
                    raise ValueError("checkpoint splits a frame")
                last_header = (timestamp, handle.tell(), length)
                handle.seek(length, 1)
            if last_header is not None:
                timestamp, position, length = last_header
                handle.seek(position)
                compressed = handle.read(length)
                if len(compressed) != length:
                    raise ValueError("raw frame shortened during read")
                with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as decoder:
                    raw = decoder.read(MAX_PROTOBUF_BYTES + 1)
                if len(raw) > MAX_PROTOBUF_BYTES:
                    raise ValueError("vehicle protobuf exceeds size limit")
                self.latest = (timestamp, raw)
            self.offset = end  # Advance only after a complete successful decode.
        if self.latest is None:
            raise ValueError("no committed vehicle frame")
        return self.latest


MODE_COLORS = {
    "bus": "#2477b7", "tram": "#c58b12", "trolleybus": "#c64652",
    "metro": "#745fbb", "rail": "#338b70", "ferry": "#258b9b", "other": "#657489",
}


def mode_for(route_type: str) -> str:
    if route_type in {"0", "900"}:
        return "tram"
    if route_type in {"1", "400", "401", "402"}:
        return "metro"
    if route_type == "800":
        return "trolleybus"
    if route_type == "3" or route_type.startswith("7"):
        return "bus"
    if route_type in {"4", "1000"}:
        return "ferry"
    if route_type == "2" or route_type.startswith("1"):
        return "rail"
    return "other"


def label(value: Any, limit: int = 48) -> str:
    if not isinstance(value, str):
        return ""
    return "".join(character for character in value if character.isprintable())[:limit]


class RouteCatalog:
    def __init__(self, root: Path):
        self.root = root
        self.digest: str | None = None
        self.routes: dict[str, dict[str, str]] = {}

    def read(self) -> dict[str, dict[str, str]]:
        state = read_object(self.root / "state.json", 65536)
        digest, value = state.get("latest_sha256"), state.get("latest_version_path")
        if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest) or not isinstance(value, str):
            raise ValueError("static catalog unavailable")
        if digest == self.digest:
            return self.routes
        relative = Path(value)
        path = (self.root / relative).resolve()
        if relative.is_absolute() or ".." in relative.parts or not path.is_relative_to(self.root.resolve()):
            raise ValueError("unsafe static catalog path")
        checksum = hashlib.sha256()
        with path.open("rb") as handle:
            while block := handle.read(1024 * 1024):
                checksum.update(block)
        if checksum.hexdigest() != digest:
            raise ValueError("static catalog checksum mismatch")
        routes = {}
        with zipfile.ZipFile(path) as archive:
            info = archive.getinfo("routes.txt")
            if info.file_size > 4 * 1024 * 1024:
                raise ValueError("routes catalog exceeds size limit")
            with archive.open(info) as source, io.TextIOWrapper(source, encoding="utf-8-sig", newline="") as text:
                for row in csv.DictReader(text):
                    if len(routes) >= 10000:
                        raise ValueError("too many routes")
                    mode = mode_for(row.get("route_type", ""))
                    color = row.get("route_color", "")
                    routes[row.get("route_id", "")] = {
                        "label": label(row.get("route_short_name")) or label(row.get("route_id")),
                        "mode": mode,
                        "color": "#" + color.lower() if re.fullmatch(r"[a-fA-F0-9]{6}", color) else MODE_COLORS[mode],
                    }
        self.routes, self.digest = routes, digest
        return routes
