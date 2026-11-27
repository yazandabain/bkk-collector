"""Evict archived static ZIPs outside the working cache."""

from __future__ import annotations

import fcntl
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from bkk_collector.archive.retention import _validated_receipt, _verify_receipt, safe_path
from bkk_collector.monitoring import iter_jsonl, utc_iso
from bkk_collector.storage.atomic_io import append_jsonl, fsync_directory, read_json, sha256_file


def prune_static_versions(manager, after_days: int) -> list[Path]:
    if after_days <= 0 or not manager.enabled:
        return []
    root = manager.data_dir
    lock_path = root / "maintenance" / "retention.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return []
        try:
            return _prune_locked(manager, after_days)
        except Exception as error:
            # Network errors may include signed URLs.
            manager.logger.warning("Refusing static GTFS prune: %s", type(error).__name__)
            return []


def _prune_locked(manager, after_days: int) -> list[Path]:
    root = manager.data_dir
    store = manager.static_store
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    dates = sorted(date for date in manager.confirmed_dates() if date < today)
    if not dates:
        return []
    cutoff = datetime.combine(now.date() - timedelta(days=after_days), datetime.min.time(),
                              tzinfo=timezone.utc).timestamp()
    history_path = safe_path(root, "static_gtfs/history.jsonl")
    state_path = safe_path(root, "static_gtfs/state.json")
    history_sha = sha256_file(history_path)
    state_sha = sha256_file(state_path)
    observed = {}
    for _, event in iter_jsonl(history_path):
        checked = store._checked_timestamp(event) if isinstance(event, dict) else None
        if checked is None or not isinstance(event.get("version_path"), str):
            raise ValueError("invalid static chronology")
        path = "static_gtfs/" + event["version_path"]
        observed[path] = max(observed.get(path, checked), checked)
    latest = store.observed_version_at(now.timestamp())
    state = read_json(state_path, {})
    if (not latest or latest["version_path"] != state.get("latest_version_path")
            or latest["sha256"] != state.get("latest_sha256")
            or not safe_path(root, "static_gtfs/" + latest["version_path"]).is_file()):
        raise ValueError("static state and chronology disagree")

    pending = manager.pending_dates()
    # Protect the schedule at the start of the retained window as well as
    # older schedules still needed to finish an unconfirmed backup.
    protected = {latest["version_path"]}
    windows = [(cutoff, now.timestamp() + 1)]
    for date in pending:
        start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        if start < cutoff:
            windows.append((start, start + 86400))
    for start, end in windows:
        protected.update(item["version_path"] for item in store.observed_timeline(start, end))
    candidate_paths = []
    for path, checked in sorted(observed.items()):
        if (checked < cutoff and path.removeprefix("static_gtfs/") not in protected
                and re.fullmatch(r"static_gtfs/versions/[0-9a-f]{64}\.zip", path)
                and safe_path(root, path).is_file()):
            candidate_paths.append(path)
    if not candidate_paths:
        return []

    date = dates[-1]
    receipt_path, receipt_sha, receipt, indexed, manifest = _validated_receipt(manager, date)
    if not receipt.get("remote_revision"):
        return []
    manifested = {a["path"]: a for a in manifest["artifacts"] if a["kind"] == "static_gtfs"}
    candidates = []
    for path in candidate_paths:
        artifact = indexed.get(path)
        if (artifact and artifact == manifested.get(path)
                and artifact["sha256"] == safe_path(root, path).stem):
            candidates.append(artifact)
    if not candidates:
        return []
    # Bound disk reads on a first run with months of archived versions.
    candidates = candidates[:10]
    manager._validate_local(candidates)
    revision = _verify_receipt(manager, receipt_path, receipt_sha, receipt)
    manager._verify_remote(candidates, revision=revision)
    manager._validate_local(candidates)
    if (history_sha != sha256_file(history_path) or state_sha != sha256_file(state_path)
            or receipt_sha != sha256_file(receipt_path) or pending != manager.pending_dates()):
        raise ValueError("static history, pending dates, or receipt changed during validation")

    removed = []
    for artifact in candidates:
        path = safe_path(root, artifact["path"])
        event = {
            "version": 2, "operation": "static_gtfs_prune", "date": date,
            "timestamp": utc_iso(), "receipt_sha256": receipt_sha, "remote_revision": revision,
            "planned_files": [artifact["path"]], "status": "validated",
            "files_deleted": [], "bytes_deleted": 0,
        }
        append_jsonl(root / "prune_history.jsonl", event)
        path.unlink()
        fsync_directory(path.parent)
        removed.append(path)
        append_jsonl(root / "prune_history.jsonl", {
            **event, "timestamp": utc_iso(), "status": "complete",
            "files_deleted": [artifact["path"]], "bytes_deleted": artifact["size"],
        })
    return removed
