"""Verified eviction of completed per-date cache partitions.

Every check for all three feed partitions finishes before the first unlink.
Filesystem unlink is not a multi-file transaction: a crash during deletion is
resumed from a durable intent, with remote and local checks repeated. Sources
are immutable completed partitions; concurrent manual rebuilds are unsupported.
"""

from __future__ import annotations

import fcntl
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from bkk_collector.storage.atomic_io import append_jsonl, atomic_write_json, fsync_directory, read_json, sha256_file
from bkk_collector.config import FEED_NAMES
from bkk_collector.monitoring import utc_iso


def _partitions(kind: str, date: str) -> dict[str, tuple[str, str]]:
    if kind == "evidence":
        return {
            **{f"metadata/polls/{feed}/date={date}": ("poll_metadata", "polls.jsonl")
               for feed in FEED_NAMES},
            f"metadata/tripupdates_presence/date={date}": (
                "tripupdates_presence", "presence.jsonlog",
            ),
        }
    return {f"{kind}/{feed}/date={date}": (kind, "") for feed in FEED_NAMES}


def safe_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or not path.parts or ".." in path.parts or str(path) != relative:
        raise ValueError("unsafe artifact path")
    current = root
    for part in path.parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"symlink in artifact path: {relative}")
    return current


def _inventory(root: Path, kind: str, date: str) -> dict[str, tuple]:
    result = {}
    for feed in FEED_NAMES:
        spool = safe_path(root, f"spool/{feed}/date={date}")
        if spool.exists() and any(spool.iterdir()):
            raise ValueError("pending spool/recovery work")
    for relative, (_, primary) in _partitions(kind, date).items():
        directory = safe_path(root, relative)
        if not directory.exists():
            continue
        for path in directory.iterdir():
            if path.is_symlink() or not path.is_file():
                raise ValueError(f"unexpected directory/symlink: {path.name}")
            if kind == "parquet" and path.suffix != ".parquet":
                raise ValueError(f"unexpected Parquet partition entry: {path.name}")
            if primary and path.name not in (primary, f"{primary}.checkpoint.json"):
                raise ValueError(f"unexpected observation evidence entry: {path.name}")
            stat = path.stat()
            result[str(path.relative_to(root))] = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
    return result


def _validated_receipt(
    manager, date: str,
) -> tuple[Path, str, dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    root = manager.data_dir
    receipt_path = safe_path(root, f"backup_receipts/date={date}.json")
    if not receipt_path.is_file():
        raise ValueError("missing or unverified v2 receipt")
    receipt_sha = sha256_file(receipt_path)
    receipt = read_json(receipt_path, {})
    if (not isinstance(receipt, dict) or receipt.get("version") != 2 or receipt.get("date") != date
            or receipt.get("remote_verified") is not True or receipt.get("repo_id") != manager.repo_id):
        raise ValueError("missing or unverified v2 receipt")
    artifacts = receipt.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ValueError("receipt lacks artifacts")
    indexed = {}
    for artifact in artifacts:
        name = artifact["path"]
        safe_path(root, name)
        if (name in indexed or type(artifact.get("size")) is not int or artifact["size"] < 0
                or not re.fullmatch(r"[0-9a-f]{64}", artifact.get("sha256", ""))):
            raise ValueError("invalid/duplicate receipt artifact")
        indexed[name] = artifact
    manifest_name = f"metadata/manifests/date={date}.json"
    manifest_artifact = indexed.get(manifest_name)
    if not manifest_artifact or manifest_artifact["sha256"] != receipt.get("manifest_sha256"):
        raise ValueError("receipt lacks authenticated daily manifest")
    manager._validate_local([manifest_artifact])
    manifest = read_json(root / manifest_name, {})
    if manifest.get("complete") is not True:
        raise ValueError("manifest is incomplete")
    manager._validate_manifest(date, manifest)
    return receipt_path, receipt_sha, receipt, indexed, manifest


def _verify_receipt(manager, receipt_path: Path, receipt_sha: str, receipt: dict) -> str:
    head = manager.remote_revision()
    revision = receipt.get("remote_revision", head)
    if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("invalid receipt revision")
    manager._verify_remote([{
        "path": str(receipt_path.relative_to(manager.data_dir)), "size": receipt_path.stat().st_size,
        "sha256": receipt_sha,
    }], revision=head)
    return revision


def _prune_date(manager, kind: str, date: str) -> list[Path]:
    root = manager.data_dir
    actual = _inventory(root, kind, date)
    if not actual:
        return []
    receipt_path, receipt_sha, receipt, indexed, manifest = _validated_receipt(manager, date)
    partitions = _partitions(kind, date)
    kinds = {artifact_kind for artifact_kind, _ in partitions.values()}
    expected = {name: a for name, a in indexed.items() if a.get("kind") in kinds}
    for name, artifact in expected.items():
        directory, _, filename = name.rpartition("/")
        if (directory not in partitions or artifact["kind"] != partitions[directory][0]
                or (kind == "parquet" and not filename.endswith(".parquet"))):
            raise ValueError("receipt artifact is outside the requested date/kind")
    for directory, (artifact_kind, primary) in partitions.items():
        if primary:
            required = (artifact_kind == "poll_metadata"
                        or manifest["feeds"]["tripupdates"].get("presence", {}).get("records", 0) > 0)
            if required and f"{directory}/{primary}" not in expected:
                raise ValueError(f"receipt lacks required {directory}/{primary}")
        elif not any(name.rpartition("/")[0] == directory for name in expected):
            raise ValueError(f"receipt lacks {directory} evidence")
    if kind == "evidence":
        if not receipt.get("remote_revision"):
            raise ValueError("observation evidence requires a pinned receipt")
        manifest_evidence = {a["path"]: (a["kind"], a["size"], a["sha256"])
                             for a in manifest["artifacts"] if a["kind"] in kinds}
        receipt_evidence = {name: (a["kind"], a["size"], a["sha256"])
                            for name, a in expected.items()}
        if manifest_evidence != receipt_evidence:
            raise ValueError("manifest and receipt observation evidence differ")
    if set(actual) - expected.keys():
        raise ValueError("unexpected files absent from receipt")
    state_path = root / "maintenance" / f"prune-{kind}-{date}.json"
    previous = read_json(state_path, {})
    if expected.keys() - actual.keys():
        if (previous.get("receipt_sha256") != receipt_sha or previous.get("operation") != f"{kind}_prune"
                or previous.get("date") != date or previous.get("planned_files") != sorted(expected)):
            raise ValueError("missing local files without matching prune recovery intent")
    candidates = [expected[name] for name in sorted(actual)]
    manager._validate_local(candidates)
    # Old v2 receipts did not pin a revision. Revalidate their cache objects at
    # a fixed current revision before eviction. Mutable static metadata is not
    # evicted here; full historical restoration uses the original receipt commit.
    revision = _verify_receipt(manager, receipt_path, receipt_sha, receipt)
    manager._verify_remote(list(expected.values()), revision=revision)
    # Network verification can take time. Recheck ALL local artifacts and the
    # directory inventory before authorizing any deletion.
    manager._validate_local(candidates)
    if actual != _inventory(root, kind, date) or sha256_file(receipt_path) != receipt_sha:
        raise ValueError("date changed during prune validation")
    intent = {
        "version": 2, "operation": f"{kind}_prune", "date": date,
        "timestamp": utc_iso(), "receipt_sha256": receipt_sha,
        "remote_revision": revision, "planned_files": sorted(expected),
        "status": "validated", "files_deleted": [], "bytes_deleted": 0,
    }
    atomic_write_json(state_path, intent)
    append_jsonl(root / "prune_history.jsonl", intent)
    removed = []
    try:
        for artifact in candidates:
            path = root / artifact["path"]
            path.unlink()  # explicit, validated regular files only; never rmtree
            fsync_directory(path.parent)
            removed.append(path)
            intent["files_deleted"].append(artifact["path"])
            intent["bytes_deleted"] += artifact["size"]
        intent["status"] = "complete"
    finally:
        if intent["status"] != "complete":
            intent["status"] = "interrupted"
        intent["timestamp"] = utc_iso()
        atomic_write_json(state_path, intent)
        append_jsonl(root / "prune_history.jsonl", intent)
    return removed


def prune_confirmed(manager, kind: str, after_days: int, *, max_dates: int | None = None) -> list[Path]:
    if kind not in ("raw", "parquet", "evidence"):
        raise ValueError("unsupported prune operation")
    if after_days <= 0 or not manager.enabled:
        return []
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=after_days)
    removed = []
    lock_path = manager.data_dir / "maintenance" / "retention.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return []
        pruned_dates = 0
        for date in sorted(manager.confirmed_dates()):
            if max_dates is not None and pruned_dates >= max_dates:
                break
            try:
                if datetime.strptime(date, "%Y-%m-%d").date() >= cutoff:
                    continue
                paths = _prune_date(manager, kind, date)
                if paths:
                    removed.extend(paths)
                    pruned_dates += 1
            except Exception as error:
                # Refusal protects data; keep maintenance running and retry.
                # API errors may include credential-bearing URLs, so no trace.
                reason = str(error) if isinstance(error, ValueError) else type(error).__name__
                manager.logger.warning("Refusing %s prune for %s: %s", kind, date, reason)
    return removed
