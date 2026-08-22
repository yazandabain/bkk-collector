"""Content-addressed daily static GTFS version collection."""

from __future__ import annotations

import csv
import os
import time
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from atomic_io import append_jsonl, atomic_write_json, fsync_directory, read_json, sha256_file
from config import STATIC_GTFS_URL
from monitoring import iter_jsonl, utc_iso


REQUIRED_GTFS_FILES = {"agency.txt", "routes.txt", "trips.txt", "stops.txt", "stop_times.txt"}


def validate_gtfs_zip(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as archive:
        names = {name.rsplit("/", 1)[-1] for name in archive.namelist() if not name.endswith("/")}
        missing = sorted(REQUIRED_GTFS_FILES - names)
        if missing:
            raise ValueError(f"static GTFS is missing required files: {', '.join(missing)}")
        corrupt = archive.testzip()
        if corrupt:
            raise ValueError(f"static GTFS contains a corrupt member: {corrupt}")
        feed_version = None
        feed_info_name = next((name for name in archive.namelist() if name.rsplit("/", 1)[-1] == "feed_info.txt"), None)
        if feed_info_name:
            with archive.open(feed_info_name) as binary:
                rows = csv.DictReader(line.decode("utf-8-sig") for line in binary)
                first = next(rows, None)
                feed_version = first.get("feed_version") if first else None
    return {"file_count": len(names), "feed_version": feed_version}


class StaticGtfsStore:
    def __init__(self, data_dir: Path, *, check_interval_seconds: float, retry_seconds: float):
        self.root = data_dir / "static_gtfs"
        self.versions = self.root / "versions"
        self.history_path = self.root / "history.jsonl"
        self.state_path = self.root / "state.json"
        self.check_interval_seconds = check_interval_seconds
        self.retry_seconds = retry_seconds
        self.root.mkdir(parents=True, exist_ok=True)

    def history(self) -> list[dict[str, Any]]:
        if not self.history_path.exists():
            return []
        return [value for _line, value in iter_jsonl(self.history_path) if value]

    def migrate_legacy_archives(self) -> int:
        """Index old dated ZIPs without renaming, copying, or deleting them."""
        known_paths = {event.get("version_path") for event in self.history()}
        added = 0
        for path in sorted(self.root.glob("budapest_gtfs_*.zip")):
            relative = str(path.relative_to(self.root))
            if relative in known_paths:
                continue
            try:
                details = validate_gtfs_zip(path)
                digest = sha256_file(path)
            except Exception:
                continue
            date_part = path.stem.removeprefix("budapest_gtfs_")
            try:
                checked = datetime.strptime(date_part, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                checked = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            append_jsonl(
                self.history_path,
                {
                    "version": 1,
                    "checked_at": checked.isoformat(),
                    "sha256": digest,
                    "size": path.stat().st_size,
                    "version_path": relative,
                    "changed": True,
                    "source": "legacy_archive_migration",
                    **details,
                },
            )
            known_paths.add(relative)
            added += 1
        if added:
            latest = max(
                self.history(),
                key=lambda event: event.get("checked_at", ""),
            )
            state = read_json(self.state_path, {})
            state.update(
                {
                    "version": 1,
                    "latest_sha256": latest.get("sha256"),
                    "latest_version_path": latest.get("version_path"),
                    "latest_feed_version": latest.get("feed_version"),
                }
            )
            atomic_write_json(self.state_path, state)
        return added

    def due(self, now_ts: float | None = None) -> bool:
        now_ts = time.time() if now_ts is None else now_ts
        state = read_json(self.state_path, {})
        last_success = float(state.get("last_success_timestamp", 0))
        last_attempt = float(state.get("last_attempt_timestamp", 0))
        if last_success and now_ts - last_success < self.check_interval_seconds:
            return False
        return not last_attempt or now_ts - last_attempt >= self.retry_seconds

    def record_failure(self, error: str, now_ts: float | None = None) -> None:
        now_ts = time.time() if now_ts is None else now_ts
        state = read_json(self.state_path, {})
        state.update({"version": 1, "last_attempt_timestamp": now_ts, "last_attempt_at": utc_iso(now_ts), "last_error": error[:1000]})
        atomic_write_json(self.state_path, state)

    def check(self, session: requests.Session, now_ts: float | None = None) -> dict[str, Any]:
        now_ts = time.time() if now_ts is None else now_ts
        self.versions.mkdir(parents=True, exist_ok=True)
        temporary = self.root / f".download-{uuid.uuid4().hex}.zip"
        try:
            with session.get(STATIC_GTFS_URL, timeout=(10, 180), stream=True) as response:
                response.raise_for_status()
                with open(temporary, "xb") as handle:
                    for block in response.iter_content(chunk_size=1024 * 1024):
                        if block:
                            handle.write(block)
                    handle.flush()
                    os.fsync(handle.fileno())
            details = validate_gtfs_zip(temporary)
            digest = sha256_file(temporary)
            size = temporary.stat().st_size
            state = read_json(self.state_path, {})
            previous_hash = state.get("latest_sha256")
            changed = digest != previous_hash
            matching_history = next((event for event in reversed(self.history()) if event.get("sha256") == digest), None)
            destination = self.root / matching_history["version_path"] if matching_history else self.versions / f"{digest}.zip"
            if not destination.exists():
                os.replace(temporary, destination)
                fsync_directory(destination.parent)
            else:
                temporary.unlink()
            relative = str(destination.relative_to(self.root))
            event = {
                "version": 1,
                "checked_at": utc_iso(now_ts),
                "sha256": digest,
                "size": size,
                "version_path": relative,
                "changed": changed,
                "source": STATIC_GTFS_URL,
                **details,
            }
            append_jsonl(self.history_path, event)
            atomic_write_json(
                self.state_path,
                {
                    "version": 1,
                    "last_attempt_timestamp": now_ts,
                    "last_attempt_at": utc_iso(now_ts),
                    "last_success_timestamp": now_ts,
                    "last_success_at": utc_iso(now_ts),
                    "latest_sha256": digest,
                    "latest_version_path": relative,
                    "latest_feed_version": details.get("feed_version"),
                    "last_error": None,
                },
            )
            return event
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def applicable_version(self, date_str: str) -> dict[str, Any] | None:
        end = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() + 86400
        candidates = []
        for event in self.history():
            try:
                checked = datetime.fromisoformat(event["checked_at"].replace("Z", "+00:00")).timestamp()
            except (KeyError, TypeError, ValueError):
                continue
            if checked < end:
                candidates.append((checked, event))
        return max(candidates, key=lambda item: item[0])[1] if candidates else None
