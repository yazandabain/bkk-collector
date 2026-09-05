"""Read-only, streamed restore/authentication test; downloads only into /tmp."""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import requests
from huggingface_hub import hf_hub_url

from atomic_io import read_json, sha256_file
from backup import BackupManager
from config import MaintenanceConfig
from retention import safe_path
from static_gtfs import StaticGtfsStore


def receipt_revision(manager: BackupManager, receipt_path: Path, receipt: dict) -> str:
    """Resolve old unpinned receipts without inventing historical metadata."""
    digest = sha256_file(receipt_path)
    remote_path = str(receipt_path.relative_to(manager.data_dir))
    manager._verify_remote([{"path": remote_path, "size": receipt_path.stat().st_size,
                             "sha256": digest}])
    revision = receipt.get("remote_revision")
    if revision:
        return revision
    title = f"Record verified BKK backup receipt {receipt['date']}"
    for commit in manager.api.list_repo_commits(manager.repo_id, repo_type="dataset"):
        if commit.title == title and manager._download_sha256(remote_path, commit.commit_id) == digest:
            return commit.commit_id
    raise ValueError("no historical commit matches this legacy v2 receipt; restore refused")


def restore_artifact(manager: BackupManager, artifact: dict, revision: str, destination: Path) -> dict:
    digest = hashlib.sha256()
    size = 0
    try:
        with requests.get(
            hf_hub_url(manager.repo_id, artifact["path"], repo_type="dataset", revision=revision),
            headers={"Authorization": f"Bearer {manager.token}"}, stream=True, timeout=(10, 60),
        ) as response:
            response.raise_for_status()
            with destination.open("xb") as out:
                for chunk in response.iter_content(1024 * 1024):
                    out.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
    except requests.RequestException as error:
        raise IOError(f"restore download failed: {type(error).__name__}") from None
    if size != artifact["size"] or digest.hexdigest() != artifact["sha256"]:
        raise IOError(f"restored artifact fails size/SHA-256 validation: {artifact['path']}")
    return {"path": artifact["path"], "bytes": size, "sha256": digest.hexdigest(), "result": "PASS"}


def verify_day(manager: BackupManager, date: str, *, all_files: bool = False) -> dict:
    path = safe_path(manager.data_dir, f"backup_receipts/date={date}.json")
    receipt = read_json(path, {})
    if (receipt.get("date") != date or receipt.get("version") != 2
            or receipt.get("remote_verified") is not True or receipt.get("repo_id") != manager.repo_id):
        raise ValueError("verified v2 receipt required")
    revision = receipt_revision(manager, path, receipt)
    artifacts = receipt["artifacts"]
    manager._verify_remote(artifacts, revision=revision)
    if not all_files:
        artifacts = [min(
            (a for a in artifacts if a["kind"] == kind and (kind != "raw" or a["path"].endswith(".rawlog"))),
            key=lambda a: a["size"],
        ) for kind in ("raw", "parquet", "daily_manifest", "poll_metadata", "static_gtfs",
                       "static_gtfs_history", "static_gtfs_state")]
    results = []
    # TemporaryDirectory only owns this newly created directory. Never use
    # repository/data roots for cleanup. One downloaded file occupies disk.
    with tempfile.TemporaryDirectory(prefix="bkk-restore-") as temporary:
        for index, artifact in enumerate(artifacts):
            destination = Path(temporary) / str(index)
            results.append(restore_artifact(manager, artifact, revision, destination))
            destination.unlink()
    return {"date": date, "revision": revision, "restored": results,
            "temporary_directory_removed": not Path(temporary).exists()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("date")
    parser.add_argument("--all", action="store_true", help="download and hash every receipt artifact")
    args = parser.parse_args()
    config = MaintenanceConfig.from_env()
    manager = BackupManager(config.data_dir, config.hf_repo_id, config.hf_token,
                            StaticGtfsStore(config.data_dir,
                                            check_interval_seconds=config.static_check_interval_seconds,
                                            retry_seconds=config.static_retry_seconds, create_root=False))
    try:
        if not manager.enabled:
            raise ValueError("HF credentials are required")
        print(json.dumps(verify_day(manager, args.date, all_files=args.all), indent=2))
        return 0
    except Exception as error:
        # Neither tokens nor signed HTTP exception URLs are printed.
        print(f"FAIL: {type(error).__name__}; no historical data changed")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
