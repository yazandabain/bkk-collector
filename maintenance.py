"""Independent background maintenance process.

No code in this module runs in the realtime collector process. A hung remote
upload can only stall/restart this worker, never the collection schedule.
"""

from __future__ import annotations

import signal
import time
from datetime import datetime, timezone
from typing import Any

import requests

from atomic_io import atomic_write_json, read_json
from backup import BackupManager
from config import MaintenanceConfig
from collector import configure_logging
from manifests import build_daily_manifest
from monitoring import poll_journal_path, utc_iso
from parquet_compact import compact_partition
from static_gtfs import StaticGtfsStore


_shutdown_requested = False


def _handle_signal(_signum, _frame) -> None:
    global _shutdown_requested
    _shutdown_requested = True


class MaintenanceWorker:
    def __init__(self, config: MaintenanceConfig):
        self.config = config
        self.logger = configure_logging(config.data_dir, "maintenance")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "bkk-collector-maintenance/2"})
        self.static_store = StaticGtfsStore(
            config.data_dir,
            check_interval_seconds=config.static_check_interval_seconds,
            retry_seconds=config.static_retry_seconds,
        )
        self.backup = BackupManager(
            config.data_dir,
            config.hf_repo_id,
            config.hf_token,
            self.static_store,
            logger=self.logger,
        )
        self.last_backup_run_monotonic = 0.0
        self.last_health_ping_monotonic = 0.0
        self.last_manifest_run_monotonic = 0.0
        self.last_compaction_run_monotonic = 0.0
        self.last_backup_results: list[dict[str, Any]] = []
        self.compaction_errors: list[str] = []

    def maintain_static_gtfs(self) -> None:
        added = self.static_store.migrate_legacy_archives()
        if added:
            self.logger.info("Indexed %d existing static GTFS archive(s) without moving them", added)
        if not self.static_store.due():
            return
        try:
            event = self.static_store.check(self.session)
            self.logger.info(
                "Static GTFS checked: sha256=%s changed=%s feed_version=%s",
                event["sha256"], event["changed"], event.get("feed_version"),
            )
        except Exception as error:
            self.static_store.record_failure(f"{type(error).__name__}: {error}")
            self.logger.exception("Static GTFS check failed; retry remains scheduled")

    def _past_backup_hour(self) -> bool:
        now = datetime.now(timezone.utc)
        if now.hour < self.config.backup_hour_utc:
            return False
        minutes_after_midnight = now.hour * 60 + now.minute
        return minutes_after_midnight >= self.config.backup_date_grace_minutes

    def compact_pending_dates(self) -> None:
        if not self.config.compact_parquet:
            return
        self.compaction_errors = []
        # Never compact legacy partitions: compaction deletes its source parts
        # after an atomic replacement, while the legacy migration invariant is
        # byte-for-byte preservation of original artifacts. Only dates with a
        # full set of v2 poll journals enter this path.
        candidates = self._v2_pending_dates()
        for date_str in candidates[: self.config.backup_max_dates_per_run]:
            for feed_name in ("vehiclepositions", "tripupdates", "alerts"):
                try:
                    output = compact_partition(
                        self.config.data_dir,
                        feed_name,
                        date_str,
                        min_files=self.config.compaction_min_files,
                    )
                    if output:
                        self.logger.info("Compacted %s/%s -> %s", feed_name, date_str, output.name)
                except Exception as error:
                    message = f"{feed_name}/{date_str}: {type(error).__name__}: {error}"
                    self.compaction_errors.append(message)
                    self.logger.exception("Parquet compaction failed for %s/%s", feed_name, date_str)

    def maintain_backups(self) -> None:
        if not self.backup.enabled or not self._past_backup_hour():
            return
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_backup_run_monotonic < self.config.backup_retry_seconds:
            return
        self.last_backup_run_monotonic = now_monotonic
        results = self.backup.run_backlog(max_upload_attempts=self.config.backup_max_dates_per_run)
        self.last_backup_results = [result.__dict__ for result in results]
        removed = self.backup.prune_confirmed_raw(self.config.prune_local_raw_after_days)
        removed += self.backup.prune_confirmed_parquet(self.config.prune_local_parquet_after_days)
        for path in removed:
            self.logger.info("Pruned local %s after receipt-backed remote verification", path)

    def maintain_compaction(self) -> None:
        if not self._past_backup_hour():
            return
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_compaction_run_monotonic < self.config.backup_retry_seconds:
            return
        self.last_compaction_run_monotonic = now_monotonic
        self.compact_pending_dates()

    def maintain_local_manifests(self) -> None:
        """Still produce quality evidence when remote backup is disabled."""
        if self.backup.enabled or not self._past_backup_hour():
            return
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_manifest_run_monotonic < self.config.backup_retry_seconds:
            return
        self.last_manifest_run_monotonic = now_monotonic
        dates = self.backup.pending_dates()
        for date_str in dates[-self.config.backup_max_dates_per_run :]:
            path = self.config.data_dir / "metadata" / "manifests" / f"date={date_str}.json"
            existing = read_json(path, {})
            if existing.get("complete"):
                continue
            try:
                manifest = build_daily_manifest(self.config.data_dir, date_str, self.static_store)
                self.logger.info("Daily manifest %s complete=%s", date_str, manifest["complete"])
            except Exception:
                self.logger.exception("Daily manifest generation failed for %s", date_str)

    def _v2_pending_dates(self) -> list[str]:
        pending = []
        for date_str in self.backup.pending_dates():
            if all(poll_journal_path(self.config.data_dir, feed, date_str).exists() for feed in ("vehiclepositions", "tripupdates", "alerts")):
                pending.append(date_str)
        return pending

    def write_status(self) -> dict[str, Any]:
        reasons: list[str] = []
        warnings: list[str] = []
        if not self.backup.enabled:
            warnings.append("offsite_backup_disabled")
        static_state = read_json(self.config.data_dir / "static_gtfs" / "state.json", {})
        static_success = float(static_state.get("last_success_timestamp", 0))
        if not static_success or time.time() - static_success > max(2 * self.config.static_check_interval_seconds, 172800):
            reasons.append("static_gtfs_stale")
        pending = self._v2_pending_dates() if self.backup.enabled else []
        if pending:
            oldest = datetime.strptime(pending[0], "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
            if time.time() - oldest > self.config.backup_stale_hours * 3600:
                reasons.append("backup_backlog_stale")
        legacy_inventory = read_json(self.config.data_dir / "metadata" / "legacy_inventory.json", {})
        legacy_dates = legacy_inventory.get("dates", []) if isinstance(legacy_inventory, dict) else []
        unconfirmed_legacy = [
            item.get("date")
            for item in legacy_dates
            if isinstance(item, dict)
            and item.get("legacy_handled") is True
            and item.get("remote_copy_confirmed") is not True
        ]
        if unconfirmed_legacy:
            warnings.append("legacy_remote_copy_unconfirmed")
        incomplete_legacy_inventory = [
            item.get("date")
            for item in legacy_dates
            if isinstance(item, dict) and item.get("inventory_complete") is not True
        ]
        if incomplete_legacy_inventory:
            warnings.append("legacy_inventory_incomplete")
        legacy_static = legacy_inventory.get("static_gtfs", {}) if isinstance(legacy_inventory, dict) else {}
        legacy_static_unconfirmed = bool(legacy_static.get("files")) and legacy_static.get("remote_copy_confirmed") is not True
        if legacy_static_unconfirmed:
            warnings.append("legacy_static_gtfs_remote_copy_unconfirmed")
        reasons.extend(f"compaction:{error}" for error in self.compaction_errors)
        status = {
            "version": 1,
            "updated_at": utc_iso(),
            "updated_timestamp": time.time(),
            "healthy": not reasons,
            "reasons": reasons,
            "warnings": warnings,
            "backup_enabled": self.backup.enabled,
            "pending_backup_dates": pending,
            "confirmed_backup_dates": len(self.backup.confirmed_dates()),
            "last_backup_results": self.last_backup_results,
            "legacy_dates_inventoried": len(legacy_dates),
            "legacy_remote_copy_unconfirmed_dates": unconfirmed_legacy,
            "legacy_inventory_incomplete_dates": incomplete_legacy_inventory,
            "legacy_static_gtfs_remote_copy_unconfirmed": legacy_static_unconfirmed,
            "static_gtfs": static_state,
        }
        atomic_write_json(self.config.data_dir / "maintenance" / "status.json", status)
        return status

    def maybe_ping_healthcheck(self, maintenance_status: dict[str, Any]) -> None:
        # A dead-man ping must not advertise fully protected operation when
        # off-site backup is disabled, even though local-only use remains a
        # supported maintenance mode.
        if not self.config.healthcheck_url or not self.backup.enabled:
            return
        now_monotonic = time.monotonic()
        if now_monotonic - self.last_health_ping_monotonic < self.config.healthcheck_ping_interval_seconds:
            return
        collector_status = read_json(self.config.data_dir / "health" / "status.json", {})
        collector_age = time.time() - float(collector_status.get("updated_timestamp", 0))
        if not collector_status.get("healthy") or collector_age > 180 or not maintenance_status.get("healthy"):
            return
        try:
            response = self.session.get(self.config.healthcheck_url, timeout=(5, 10))
            response.raise_for_status()
            self.last_health_ping_monotonic = now_monotonic
        except Exception as error:
            # Never log the URL: a healthchecks.io ping URL contains a secret.
            self.logger.warning("External healthcheck ping failed: %s", type(error).__name__)

    def run(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        if not self.backup.enabled:
            self.logger.warning("HF_TOKEN/HF_REPO_ID not both set; remote backup is disabled")
        self.logger.info("Starting independent maintenance worker")
        while not _shutdown_requested:
            self.maintain_static_gtfs()
            self.maintain_compaction()
            self.maintain_local_manifests()
            self.maintain_backups()
            status = self.write_status()
            if not status["healthy"]:
                self.logger.error("Maintenance health degraded: %s", ", ".join(status["reasons"]))
            self.maybe_ping_healthcheck(status)
            deadline = time.monotonic() + self.config.maintenance_interval_seconds
            while not _shutdown_requested and time.monotonic() < deadline:
                time.sleep(min(1.0, deadline - time.monotonic()))
        self.session.close()
        self.logger.info("Maintenance worker stopped")


def main() -> None:
    MaintenanceWorker(MaintenanceConfig.from_env()).run()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    main()
