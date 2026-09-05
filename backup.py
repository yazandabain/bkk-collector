"""Strict, retryable, independently runnable Hugging Face backup."""

from __future__ import annotations

import logging
import os
import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from atomic_io import append_jsonl, atomic_write_json, fsync_directory, read_json, sha256_file
from config import FEED_NAMES
from manifests import build_daily_manifest, discover_completed_dates
from monitoring import utc_iso
from static_gtfs import StaticGtfsStore


@dataclass(frozen=True)
class BackupResult:
    date: str
    success: bool
    reason: str


class BackupManager:
    def __init__(
        self,
        data_dir: Path,
        repo_id: str,
        token: str,
        static_store: StaticGtfsStore,
        *,
        api: Any | None = None,
        logger: logging.Logger | None = None,
    ):
        self.data_dir = data_dir
        self.repo_id = repo_id
        self.token = token
        self.static_store = static_store
        self._api = api
        self.logger = logger or logging.getLogger("bkk_backup")
        self.receipts_dir = data_dir / "backup_receipts"
        # The old backup_success_dates.txt is intentionally not trusted: older
        # code could write it after a partial upload. Receipts are authoritative.
        self.history_path = data_dir / "backup_history.jsonl"
        self.status_path = data_dir / "backup_status.json"

    @property
    def enabled(self) -> bool:
        return bool(self.repo_id and self.token)

    @property
    def api(self):
        if self._api is None:
            from huggingface_hub import HfApi

            self._api = HfApi(token=self.token)
        return self._api

    def confirmed_dates(self) -> set[str]:
        if not self.receipts_dir.exists():
            return set()
        confirmed: set[str] = set()
        for path in self.receipts_dir.glob("date=*.json"):
            receipt = read_json(path, {})
            if (
                isinstance(receipt, dict)
                and receipt.get("version") == 2
                and receipt.get("remote_verified") is True
                and receipt.get("repo_id") == self.repo_id
                and receipt.get("date") == path.name.removeprefix("date=").removesuffix(".json")
                and isinstance(receipt.get("artifacts"), list)
                and bool(receipt["artifacts"])
            ):
                confirmed.add(path.name.removeprefix("date=").removesuffix(".json"))
        return confirmed

    def pending_dates(self) -> list[str]:
        # Explicitly inventoried legacy dates use separate, non-scientific
        # receipts and must not be rebuilt forever as impossible v2 manifests.
        # They are also never returned by confirmed_dates(), so pruning cannot
        # mistake a legacy copy for verified complete collection evidence.
        inventory = read_json(self.data_dir / "metadata" / "legacy_inventory.json", {})
        handled_legacy: set[str] = set()
        if inventory.get("inventory_version") == 1 and inventory.get("inventory_complete") is True:
            for item in inventory.get("dates", []):
                if (
                    isinstance(item, dict)
                    and item.get("inventory_complete") is True
                    and item.get("legacy_handled") is True
                    and item.get("classification") != "v2_complete"
                    and isinstance(item.get("date"), str)
                ):
                    handled_legacy.add(item["date"])
        return sorted(
            set(discover_completed_dates(self.data_dir))
            - self.confirmed_dates()
            - handled_legacy
        )

    def _local_artifacts(self, date_str: str, manifest: dict[str, Any]) -> list[dict[str, Any]]:
        artifacts = list(manifest["artifacts"])
        manifest_path = self.data_dir / "metadata" / "manifests" / f"date={date_str}.json"
        artifacts.append(
            {
                "path": str(manifest_path.relative_to(self.data_dir)),
                "kind": "daily_manifest",
                "size": manifest_path.stat().st_size,
                "sha256": sha256_file(manifest_path),
            }
        )
        deduplicated = {artifact["path"]: artifact for artifact in artifacts}
        return [deduplicated[path] for path in sorted(deduplicated)]

    def _validate_local(self, artifacts: list[dict[str, Any]], *, verify_hashes: bool = True) -> None:
        from retention import safe_path

        for artifact in artifacts:
            relative = Path(artifact["path"])
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"unsafe backup artifact path: {artifact['path']}")
            path = safe_path(self.data_dir, artifact["path"])
            if path.is_symlink():
                raise ValueError(f"refusing symlinked backup artifact: {artifact['path']}")
            if not path.is_file():
                raise FileNotFoundError(f"required backup artifact disappeared: {artifact['path']}")
            if path.stat().st_size != artifact["size"]:
                raise IOError(f"required backup artifact size changed: {artifact['path']}")
            if verify_hashes and sha256_file(path) != artifact["sha256"]:
                raise IOError(f"required backup artifact checksum changed: {artifact['path']}")

    @staticmethod
    def _validate_manifest(date_str: str, manifest: dict[str, Any]) -> None:
        """Do not trust a lone ``complete`` boolean as backup authorization."""
        if manifest.get("manifest_version") != 1 or manifest.get("date") != date_str:
            raise ValueError("daily manifest version/date is invalid")
        feeds = manifest.get("feeds")
        artifacts = manifest.get("artifacts")
        static = manifest.get("static_gtfs")
        timeline = manifest.get("static_gtfs_timeline")
        if (
            not isinstance(feeds, dict)
            or not isinstance(artifacts, list)
            or not isinstance(static, dict)
            or not isinstance(timeline, list)
            or not timeline
        ):
            raise ValueError("daily manifest is missing feed, artifact, or static GTFS evidence")
        artifact_paths = {
            artifact.get("path")
            for artifact in artifacts
            if isinstance(artifact, dict) and isinstance(artifact.get("path"), str)
        }
        for feed_name in FEED_NAMES:
            stats = feeds.get(feed_name)
            if not isinstance(stats, dict):
                raise ValueError(f"daily manifest lacks {feed_name} statistics")
            if (
                int(stats.get("successful_http_polls", 0)) < 1
                or int(stats.get("successful_parse_polls", 0)) < 1
                or int(stats.get("raw_snapshots", 0)) < 1
                or int(stats.get("parquet_files", 0)) < 1
                or int(stats.get("pending_spool_segments", 0)) != 0
                or stats.get("raw_log_clean") is not True
            ):
                raise ValueError(f"daily manifest has incomplete {feed_name} evidence")
            raw_path = f"raw/{feed_name}/date={date_str}/{feed_name}.rawlog"
            poll_path = f"metadata/polls/{feed_name}/date={date_str}/polls.jsonl"
            parquet_prefix = f"parquet/{feed_name}/date={date_str}/"
            if raw_path not in artifact_paths or poll_path not in artifact_paths:
                raise ValueError(f"daily manifest lacks required {feed_name} raw/poll artifacts")
            if not any(path and path.startswith(parquet_prefix) and path.endswith(".parquet") for path in artifact_paths):
                raise ValueError(f"daily manifest lacks required {feed_name} Parquet artifact")
        version_path = static.get("version_path")
        if not isinstance(version_path, str) or f"static_gtfs/{version_path}" not in artifact_paths:
            raise ValueError("daily manifest lacks its applicable static GTFS archive")
        for segment in timeline:
            timeline_path = segment.get("version_path") if isinstance(segment, dict) else None
            if not isinstance(timeline_path, str) or f"static_gtfs/{timeline_path}" not in artifact_paths:
                raise ValueError("daily manifest lacks an archive from its static GTFS timeline")

    @staticmethod
    def _remote_path(info: Any) -> str | None:
        if isinstance(info, dict):
            return info.get("path") or info.get("rfilename")
        return getattr(info, "path", None) or getattr(info, "rfilename", None)

    @staticmethod
    def _remote_size(info: Any) -> int | None:
        if isinstance(info, dict):
            return info.get("size")
        return getattr(info, "size", None)

    @staticmethod
    def _remote_lfs_sha(info: Any) -> str | None:
        lfs = info.get("lfs") if isinstance(info, dict) else getattr(info, "lfs", None)
        if isinstance(lfs, dict):
            return lfs.get("sha256")
        return getattr(lfs, "sha256", None) if lfs else None

    def remote_revision(self) -> str:
        revision = self.api.repo_info(repo_id=self.repo_id, repo_type="dataset").sha
        if not isinstance(revision, str) or not re.fullmatch(r"[0-9a-f]{40}", revision):
            raise ValueError("HF did not return an immutable commit revision")
        return revision

    def _download_sha256(self, path: str, revision: str) -> str:
        """Verify ordinary Git files too; equal size is not equal content."""
        import requests
        from huggingface_hub import hf_hub_url

        digest = hashlib.sha256()
        try:
            with requests.get(
                hf_hub_url(self.repo_id, path, repo_type="dataset", revision=revision),
                headers={"Authorization": f"Bearer {self.token}"},
                stream=True, timeout=(10, 60),
            ) as response:
                response.raise_for_status()
                for chunk in response.iter_content(1024 * 1024):
                    digest.update(chunk)
        except requests.RequestException as error:
            # Response exceptions can contain signed storage URLs.
            raise IOError(f"remote download failed: {type(error).__name__}") from None
        return digest.hexdigest()

    def _verify_remote(self, artifacts: list[dict[str, Any]], *, revision: str | None = None) -> None:
        revision = revision or self.remote_revision()
        expected = {artifact["path"]: artifact for artifact in artifacts}
        returned: dict[str, Any] = {}
        paths = list(expected)
        for start in range(0, len(paths), 100):
            infos = self.api.get_paths_info(
                repo_id=self.repo_id,
                paths=paths[start : start + 100],
                repo_type="dataset",
                revision=revision,
            )
            for info in infos:
                remote_path = self._remote_path(info)
                if remote_path:
                    returned[remote_path] = info
        for path, artifact in expected.items():
            info = returned.get(path)
            if info is None:
                raise IOError(f"remote verification could not find {path}")
            if self._remote_size(info) != artifact["size"]:
                raise IOError(f"remote size mismatch for {path}")
            lfs_sha = self._remote_lfs_sha(info)
            digest = lfs_sha or self._download_sha256(path, revision)
            if digest != artifact["sha256"]:
                raise IOError(f"remote checksum mismatch for {path}")

    def _remote_lfs_matches(self, artifacts: list[dict[str, Any]]) -> set[str]:
        """Return content-verified LFS paths that need no repeat upload."""
        matched: set[str] = set()
        paths = [artifact["path"] for artifact in artifacts]
        expected = {artifact["path"]: artifact for artifact in artifacts}
        for start in range(0, len(paths), 100):
            infos = self.api.get_paths_info(
                repo_id=self.repo_id,
                paths=paths[start : start + 100],
                repo_type="dataset",
            )
            for info in infos:
                path = self._remote_path(info)
                artifact = expected.get(path)
                if (
                    artifact
                    and self._remote_size(info) == artifact["size"]
                    and self._remote_lfs_sha(info) == artifact["sha256"]
                ):
                    matched.add(path)
        return matched

    def backup_date(
        self,
        date_str: str,
        *,
        manifest: dict[str, Any] | None = None,
    ) -> BackupResult:
        try:
            if date_str in self.confirmed_dates():
                return BackupResult(date_str, True, "immutable receipt already exists")
            manifest = manifest or build_daily_manifest(self.data_dir, date_str, self.static_store)
            if not manifest["complete"]:
                reason = "local date is incomplete: " + "; ".join(manifest["completeness_errors"])
                self._record_status(date_str, False, reason)
                return BackupResult(date_str, False, reason)
            self._validate_manifest(date_str, manifest)
            artifacts = self._local_artifacts(date_str, manifest)
            self._validate_local(artifacts, verify_hashes=True)
            self.api.create_repo(repo_id=self.repo_id, repo_type="dataset", exist_ok=True, private=True)
            already_verified = self._remote_lfs_matches(artifacts)
            for artifact in artifacts:
                if artifact["path"] in already_verified:
                    continue
                self.api.upload_file(
                    repo_id=self.repo_id,
                    repo_type="dataset",
                    path_or_fileobj=str(self.data_dir / artifact["path"]),
                    path_in_repo=artifact["path"],
                    commit_message=f"Back up BKK collection date {date_str}",
                )
            revision = self.remote_revision()
            self._verify_remote(artifacts, revision=revision)
            self._validate_local(artifacts, verify_hashes=True)
            manifest_artifact = next(artifact for artifact in artifacts if artifact["kind"] == "daily_manifest")
            receipt = {
                "version": 2,
                "date": date_str,
                "confirmed_at": utc_iso(),
                "repo_id": self.repo_id,
                "remote_verified": True,
                "verification": "size and SHA-256 for every artifact at remote_revision (LFS hash or streamed download)",
                "remote_revision": revision,
                "manifest_sha256": manifest_artifact["sha256"],
                "artifacts": artifacts,
            }
            receipt_path = self.receipts_dir / f"date={date_str}.json"
            pending_receipt = self.receipts_dir / f".date={date_str}.pending.json"
            atomic_write_json(pending_receipt, receipt)
            receipt_artifact = {
                "path": str(receipt_path.relative_to(self.data_dir)),
                "kind": "backup_receipt",
                "size": pending_receipt.stat().st_size,
                "sha256": sha256_file(pending_receipt),
            }
            self.api.upload_file(
                repo_id=self.repo_id,
                repo_type="dataset",
                path_or_fileobj=str(pending_receipt),
                path_in_repo=receipt_artifact["path"],
                commit_message=f"Record verified BKK backup receipt {date_str}",
            )
            self._verify_remote([receipt_artifact])
            os.replace(pending_receipt, receipt_path)
            fsync_directory(self.receipts_dir)
            append_jsonl(self.history_path, {"date": date_str, "confirmed_at": receipt["confirmed_at"], "version": 2})
            self._record_status(date_str, True, "remote artifacts verified")
            self.logger.info("Backup verified for %s (%d artifacts)", date_str, len(artifacts))
            return BackupResult(date_str, True, "remote artifacts verified")
        except Exception as error:
            reason = re.sub(r"https?://\S+", "<redacted-url>", f"{type(error).__name__}: {error}")
            if self.token:
                reason = reason.replace(self.token, "<redacted>")
            self._record_status(date_str, False, reason)
            self.logger.error("Backup failed for %s; it remains pending: %s", date_str, reason)
            return BackupResult(date_str, False, reason)

    def _record_status(self, date_str: str, success: bool, reason: str) -> None:
        state = read_json(self.status_path, {"version": 1, "dates": {}})
        dates = state.setdefault("dates", {})
        previous = dates.get(date_str, {})
        dates[date_str] = {
            "last_attempt_at": utc_iso(),
            "success": success,
            "reason": reason[:2000],
            "attempts": int(previous.get("attempts", 0)) + 1,
        }
        state["updated_at"] = utc_iso()
        atomic_write_json(self.status_path, state)

    def run_backlog(self, *, max_upload_attempts: int) -> list[BackupResult]:
        results: list[BackupResult] = []
        upload_attempts = 0
        for date_str in self.pending_dates():
            # Build first so a permanently incomplete legacy date cannot starve
            # newer complete dates. backup_date repeats this under one error path.
            try:
                manifest = self._load_or_build_manifest(date_str)
            except Exception as error:
                results.append(BackupResult(date_str, False, f"manifest failed: {error}"))
                continue
            if not manifest["complete"]:
                reason = "local date is incomplete: " + "; ".join(manifest["completeness_errors"])
                self._record_status(date_str, False, reason)
                results.append(BackupResult(date_str, False, reason))
                continue
            if upload_attempts >= max_upload_attempts:
                break
            upload_attempts += 1
            result = self.backup_date(date_str, manifest=manifest)
            results.append(result)
            if not result.success:
                # A network/auth failure is likely to affect every date; retain
                # the rest for the next retry instead of producing noisy traffic.
                break
        return results

    def _load_or_build_manifest(self, date_str: str) -> dict[str, Any]:
        path = self.data_dir / "metadata" / "manifests" / f"date={date_str}.json"
        manifest = read_json(path, {})
        if (
            manifest.get("complete")
            and manifest.get("artifacts")
            and manifest.get("static_gtfs_timeline")
        ):
            current = True
            for artifact in manifest["artifacts"]:
                local = self.data_dir / artifact["path"]
                if (
                    not local.is_file()
                    or local.stat().st_size != artifact.get("size")
                    or local.stat().st_mtime_ns != artifact.get("mtime_ns")
                ):
                    current = False
                    break
            if current:
                return manifest
        return build_daily_manifest(self.data_dir, date_str, self.static_store)

    def prune_confirmed_raw(self, after_days: int) -> list[Path]:
        from retention import prune_confirmed
        return prune_confirmed(self, "raw", after_days)

    def prune_confirmed_parquet(self, after_days: int) -> list[Path]:
        from retention import prune_confirmed
        return prune_confirmed(self, "parquet", after_days)
