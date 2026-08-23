"""Non-destructive inventory and optional off-site copy of pre-v2 data."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from atomic_io import atomic_write_json, fsync_directory, read_json, sha256_file
from backup import BackupManager
from config import FEED_NAMES
from manifests import discover_completed_dates
from monitoring import poll_journal_path, utc_iso
from parquet_store import parquet_row_count
from raw_log import scan_raw_log
from static_gtfs import StaticGtfsStore, validate_gtfs_zip


INVENTORY_VERSION = 1


def _inventory_file(data_dir: Path, path: Path, kind: str) -> dict[str, Any]:
    if path.is_symlink():
        raise ValueError(f"refusing symlinked legacy artifact: {path}")
    before = path.stat()
    digest = sha256_file(path)
    after = path.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise IOError(f"artifact changed while it was inventoried: {path}")
    return {
        "path": str(path.relative_to(data_dir)),
        "kind": kind,
        "size": after.st_size,
        "mtime_ns": after.st_mtime_ns,
        "sha256": digest,
    }


def _date_files(data_dir: Path, date_str: str) -> list[tuple[Path, str]]:
    found: dict[Path, str] = {}
    for feed_name in FEED_NAMES:
        for root, kind in (
            (data_dir / "raw" / feed_name / f"date={date_str}", "legacy_raw"),
            (data_dir / "parquet" / feed_name / f"date={date_str}", "legacy_parquet"),
            (data_dir / "spool" / feed_name / f"date={date_str}", "legacy_spool"),
            (data_dir / "metadata" / "polls" / feed_name / f"date={date_str}", "poll_metadata"),
        ):
            if root.exists():
                for path in root.rglob("*"):
                    if path.is_file() or path.is_symlink():
                        found[path] = kind
    manifest = data_dir / "metadata" / "manifests" / f"date={date_str}.json"
    if manifest.exists() or manifest.is_symlink():
        found[manifest] = "daily_manifest"
    return sorted(found.items(), key=lambda item: str(item[0]))


def _static_files(data_dir: Path) -> list[tuple[Path, str]]:
    root = data_dir / "static_gtfs"
    if not root.exists():
        return []
    found = []
    for path in root.rglob("*"):
        if not (path.is_file() or path.is_symlink()):
            continue
        kind = "static_gtfs_zip" if path.suffix.lower() == ".zip" else "static_gtfs_metadata"
        found.append((path, kind))
    return sorted(found, key=lambda item: str(item[0]))


def _validate_legacy_date(data_dir: Path, date_str: str) -> list[str]:
    problems: list[str] = []
    for feed_name in FEED_NAMES:
        raw = data_dir / "raw" / feed_name / f"date={date_str}" / f"{feed_name}.rawlog"
        if not raw.is_file():
            problems.append(f"{feed_name}:raw_missing")
        else:
            try:
                scan = scan_raw_log(raw)
                if not scan.clean:
                    problems.append(f"{feed_name}:raw_invalid_tail")
                if scan.complete_records < 1:
                    problems.append(f"{feed_name}:raw_empty")
            except Exception as error:
                problems.append(f"{feed_name}:raw_unreadable:{type(error).__name__}")

        parquet_dir = data_dir / "parquet" / feed_name / f"date={date_str}"
        parquet_files = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.exists() else []
        if not parquet_files:
            problems.append(f"{feed_name}:parquet_missing")
        for path in parquet_files:
            try:
                parquet_row_count(path)
            except Exception as error:
                problems.append(f"{feed_name}:parquet_unreadable:{path.name}:{type(error).__name__}")
        spool_dir = data_dir / "spool" / feed_name / f"date={date_str}"
        if spool_dir.exists() and any(spool_dir.iterdir()):
            problems.append(f"{feed_name}:uncommitted_spool_present")
    return problems


def _is_v2_complete(data_dir: Path, date_str: str, manager: BackupManager) -> bool:
    if not all(poll_journal_path(data_dir, feed, date_str).is_file() for feed in FEED_NAMES):
        return False
    manifest = read_json(data_dir / "metadata" / "manifests" / f"date={date_str}.json", {})
    try:
        if manifest.get("complete") is not True:
            return False
        manager._validate_manifest(date_str, manifest)
        manager._validate_local(manager._local_artifacts(date_str, manifest), verify_hashes=True)
    except Exception:
        return False
    return True


def _remote_statuses(manager: BackupManager, artifacts: list[dict[str, Any]]) -> dict[str, str]:
    if not manager.enabled:
        return {artifact["path"]: "not_checked_no_credentials" for artifact in artifacts}
    expected = {artifact["path"]: artifact for artifact in artifacts}
    returned: dict[str, Any] = {}
    paths = list(expected)
    for start in range(0, len(paths), 100):
        for info in manager.api.get_paths_info(
            repo_id=manager.repo_id,
            paths=paths[start : start + 100],
            repo_type="dataset",
        ):
            path = manager._remote_path(info)
            if path:
                returned[path] = info
    statuses: dict[str, str] = {}
    for path, artifact in expected.items():
        info = returned.get(path)
        if info is None:
            statuses[path] = "missing"
        elif manager._remote_size(info) != artifact["size"]:
            statuses[path] = "size_mismatch"
        else:
            lfs_sha = manager._remote_lfs_sha(info)
            if lfs_sha:
                statuses[path] = "sha256_match" if lfs_sha == artifact["sha256"] else "sha256_mismatch"
            else:
                statuses[path] = "size_match_checksum_unavailable"
    return statuses


class LegacyMigrator:
    def __init__(self, data_dir: Path, repo_id: str, token: str):
        self.data_dir = data_dir
        self.static_store = StaticGtfsStore(
            data_dir,
            check_interval_seconds=86400,
            retry_seconds=3600,
            create_root=False,
        )
        self.manager = BackupManager(data_dir, repo_id, token, self.static_store)

    def inventory(self, *, check_remote: bool = True) -> dict[str, Any]:
        dates: list[dict[str, Any]] = []
        for date_str in discover_completed_dates(self.data_dir):
            problems = _validate_legacy_date(self.data_dir, date_str)
            artifacts: list[dict[str, Any]] = []
            inventory_errors: list[str] = []
            for path, kind in _date_files(self.data_dir, date_str):
                try:
                    artifacts.append(_inventory_file(self.data_dir, path, kind))
                except Exception as error:
                    inventory_errors.append(f"{path.relative_to(self.data_dir)}:{type(error).__name__}:{error}")
            problems.extend(inventory_errors)
            if _is_v2_complete(self.data_dir, date_str, self.manager):
                classification = "v2_complete"
            elif problems:
                classification = "corrupt_or_missing"
            else:
                classification = "legacy_present_but_completeness_unverifiable"
            date_inventory_complete = not inventory_errors
            try:
                remote = _remote_statuses(self.manager, artifacts) if check_remote else {
                    artifact["path"]: "not_checked" for artifact in artifacts
                }
            except Exception as error:
                remote = {artifact["path"]: f"remote_check_failed:{type(error).__name__}" for artifact in artifacts}
            would_upload = [path for path, status in remote.items() if status != "sha256_match"]
            dates.append(
                {
                    "date": date_str,
                    "classification": classification,
                    "inventory_complete": date_inventory_complete,
                    "legacy_handled": classification != "v2_complete" and date_inventory_complete,
                    "scientific_complete": classification == "v2_complete",
                    "problems": problems,
                    "files": artifacts,
                    "file_count": len(artifacts),
                    "local_bytes": sum(artifact["size"] for artifact in artifacts),
                    "remote_status": remote,
                    "files_that_would_be_uploaded": would_upload,
                    "files_that_remain_untouched": [artifact["path"] for artifact in artifacts],
                }
            )

        static_artifacts: list[dict[str, Any]] = []
        static_problems: list[str] = []
        static_inventory_errors: list[str] = []
        for path, kind in _static_files(self.data_dir):
            try:
                artifact = _inventory_file(self.data_dir, path, kind)
                static_artifacts.append(artifact)
                if kind == "static_gtfs_zip":
                    validate_gtfs_zip(path)
            except Exception as error:
                message = f"{path.relative_to(self.data_dir)}:{type(error).__name__}:{error}"
                static_problems.append(message)
                if not any(artifact["path"] == str(path.relative_to(self.data_dir)) for artifact in static_artifacts):
                    static_inventory_errors.append(message)
        if not any(artifact["kind"] == "static_gtfs_zip" for artifact in static_artifacts):
            static_problems.append("static_gtfs_zip_missing")
        try:
            static_remote = _remote_statuses(self.manager, static_artifacts) if check_remote else {
                artifact["path"]: "not_checked" for artifact in static_artifacts
            }
        except Exception as error:
            static_remote = {
                artifact["path"]: f"remote_check_failed:{type(error).__name__}"
                for artifact in static_artifacts
            }
        return {
            "inventory_version": INVENTORY_VERSION,
            "generated_at": utc_iso(),
            "inventory_complete": True,
            "dates": dates,
            "static_gtfs": {
                "classification": "corrupt_or_missing" if static_problems else "legacy_present_but_completeness_unverifiable",
                "inventory_complete": not static_inventory_errors,
                "problems": static_problems,
                "files": static_artifacts,
                "file_count": len(static_artifacts),
                "local_bytes": sum(artifact["size"] for artifact in static_artifacts),
                "remote_status": static_remote,
                "files_that_would_be_uploaded": [
                    path for path, status in static_remote.items() if status != "sha256_match"
                ],
                "files_that_remain_untouched": [artifact["path"] for artifact in static_artifacts],
            },
        }

    def apply(self, inventory: dict[str, Any]) -> dict[str, Any]:
        """Persist inventory and copy legacy bytes without altering originals."""
        result = inventory
        if self.manager.enabled:
            self.manager.api.create_repo(
                repo_id=self.manager.repo_id,
                repo_type="dataset",
                exist_ok=True,
                private=True,
            )
        for date in result["dates"]:
            if date["classification"] == "v2_complete":
                continue
            statuses = date["remote_status"]
            upload_errors: list[str] = []
            if self.manager.enabled:
                for artifact in date["files"]:
                    if statuses.get(artifact["path"]) == "sha256_match":
                        continue
                    try:
                        self.manager.api.upload_file(
                            repo_id=self.manager.repo_id,
                            repo_type="dataset",
                            path_or_fileobj=str(self.data_dir / artifact["path"]),
                            path_in_repo=artifact["path"],
                            commit_message=f"Inventory legacy BKK artifacts for {date['date']}",
                        )
                    except Exception as error:
                        upload_errors.append(f"{artifact['path']}:{type(error).__name__}")
                try:
                    statuses = _remote_statuses(self.manager, date["files"])
                except Exception as error:
                    upload_errors.append(f"verification:{type(error).__name__}")
            date["remote_status"] = statuses
            date["upload_errors"] = upload_errors
            remote_confirmed = bool(self.manager.enabled) and not upload_errors and all(
                status in {"sha256_match", "size_match_checksum_unavailable"}
                for status in statuses.values()
            )
            date["remote_copy_confirmed"] = remote_confirmed
            receipt = {
                "version": 1,
                "date": date["date"],
                "recorded_at": utc_iso(),
                "repo_id": self.manager.repo_id or None,
                "classification": date["classification"],
                "scientific_complete": False,
                "remote_copy_confirmed": remote_confirmed,
                "verification": (
                    "remote path and size; SHA-256 additionally checked for LFS objects"
                    if remote_confirmed
                    else "remote copy not confirmed"
                ),
                "artifacts": date["files"],
                "remote_status": statuses,
                "upload_errors": upload_errors,
            }
            receipt_path = self.data_dir / "backup_receipts" / "legacy" / f"date={date['date']}.json"
            pending_receipt = receipt_path.with_name(f".{receipt_path.name}.pending")
            atomic_write_json(pending_receipt, receipt)
            receipt_remote_verified = False
            if self.manager.enabled and remote_confirmed:
                receipt_artifact = {
                    "path": str(receipt_path.relative_to(self.data_dir)),
                    "kind": "legacy_backup_receipt",
                    "size": pending_receipt.stat().st_size,
                    "sha256": sha256_file(pending_receipt),
                }
                try:
                    self.manager.api.upload_file(
                        repo_id=self.manager.repo_id,
                        repo_type="dataset",
                        path_or_fileobj=str(pending_receipt),
                        path_in_repo=receipt_artifact["path"],
                        commit_message=f"Record legacy BKK inventory receipt {date['date']}",
                    )
                    self.manager._verify_remote([receipt_artifact])
                    receipt_remote_verified = True
                except Exception as error:
                    date["upload_errors"].append(f"legacy_receipt:{type(error).__name__}")
            os.replace(pending_receipt, receipt_path)
            fsync_directory(receipt_path.parent)
            date["legacy_receipt_remote_verified"] = receipt_remote_verified

        static = result["static_gtfs"]
        static_upload_errors: list[str] = []
        if self.manager.enabled:
            for artifact in static["files"]:
                if static["remote_status"].get(artifact["path"]) == "sha256_match":
                    continue
                try:
                    self.manager.api.upload_file(
                        repo_id=self.manager.repo_id,
                        repo_type="dataset",
                        path_or_fileobj=str(self.data_dir / artifact["path"]),
                        path_in_repo=artifact["path"],
                        commit_message="Inventory legacy BKK static GTFS artifacts",
                    )
                except Exception as error:
                    static_upload_errors.append(f"{artifact['path']}:{type(error).__name__}")
            try:
                static["remote_status"] = _remote_statuses(self.manager, static["files"])
            except Exception as error:
                static_upload_errors.append(f"verification:{type(error).__name__}")
        static["upload_errors"] = static_upload_errors
        static_remote_confirmed = (
            bool(self.manager.enabled)
            and bool(static["files"])
            and not static_upload_errors
            and all(
                status in {"sha256_match", "size_match_checksum_unavailable"}
                for status in static["remote_status"].values()
            )
        )
        static["remote_copy_confirmed"] = static_remote_confirmed
        static_receipt = {
            "version": 1,
            "recorded_at": utc_iso(),
            "repo_id": self.manager.repo_id or None,
            "classification": static["classification"],
            "scientific_complete": False,
            "remote_copy_confirmed": static_remote_confirmed,
            "verification": (
                "remote path and size; SHA-256 additionally checked for LFS objects"
                if static_remote_confirmed
                else "remote copy not confirmed"
            ),
            "artifacts": static["files"],
            "remote_status": static["remote_status"],
            "upload_errors": static_upload_errors,
        }
        static_receipt_path = self.data_dir / "backup_receipts" / "legacy" / "static_gtfs.json"
        pending_static_receipt = static_receipt_path.with_name(".static_gtfs.json.pending")
        atomic_write_json(pending_static_receipt, static_receipt)
        static_receipt_remote_verified = False
        if self.manager.enabled and static_remote_confirmed:
            receipt_artifact = {
                "path": str(static_receipt_path.relative_to(self.data_dir)),
                "kind": "legacy_static_gtfs_receipt",
                "size": pending_static_receipt.stat().st_size,
                "sha256": sha256_file(pending_static_receipt),
            }
            try:
                self.manager.api.upload_file(
                    repo_id=self.manager.repo_id,
                    repo_type="dataset",
                    path_or_fileobj=str(pending_static_receipt),
                    path_in_repo=receipt_artifact["path"],
                    commit_message="Record legacy BKK static GTFS inventory receipt",
                )
                self.manager._verify_remote([receipt_artifact])
                static_receipt_remote_verified = True
            except Exception as error:
                static["upload_errors"].append(f"legacy_receipt:{type(error).__name__}")
        os.replace(pending_static_receipt, static_receipt_path)
        fsync_directory(static_receipt_path.parent)
        static["legacy_receipt_remote_verified"] = static_receipt_remote_verified
        inventory_path = self.data_dir / "metadata" / "legacy_inventory.json"
        atomic_write_json(inventory_path, result)
        return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="hash, classify, and report without writing/uploading")
    parser.add_argument("--data-dir", default=os.environ.get("DATA_DIR", "/data"))
    args = parser.parse_args(argv)
    migrator = LegacyMigrator(
        Path(args.data_dir),
        os.environ.get("HF_REPO_ID", "").strip(),
        os.environ.get("HF_TOKEN", "").strip(),
    )
    try:
        inventory = migrator.inventory(check_remote=True)
        if not args.dry_run:
            inventory = migrator.apply(inventory)
    except Exception as error:
        print(f"legacy migration failed: {type(error).__name__}: {error}", file=sys.stderr)
        return 1
    print(json.dumps({"dry_run": args.dry_run, **inventory}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
