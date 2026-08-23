"""Environment configuration shared by collector and maintenance workers."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from pathlib import Path


FEED_NAMES = ("vehiclepositions", "tripupdates", "alerts")
DEFAULT_FEED_INTERVALS = {
    "vehiclepositions": 10.0,
    "tripupdates": 10.0,
    "alerts": 30.0,
}
MIN_REALTIME_INTERVAL_SECONDS = 5.0
STATIC_GTFS_URL = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def feed_urls(api_key: str) -> dict[str, str]:
    root = "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full"
    return {
        "vehiclepositions": f"{root}/VehiclePositions.pb?key={api_key}",
        "tripupdates": f"{root}/TripUpdates.pb?key={api_key}",
        "alerts": f"{root}/Alerts.pb?key={api_key}",
    }


@dataclass(frozen=True)
class CollectorConfig:
    api_key: str
    data_dir: Path
    # POLL_INTERVAL_SECONDS is retained only as a migration fallback.  A
    # feed-specific value always wins; when neither is set the production
    # defaults in DEFAULT_FEED_INTERVALS apply.
    poll_interval_seconds: float | None = None
    vehicle_positions_interval_seconds: float | None = None
    trip_updates_interval_seconds: float | None = None
    alerts_interval_seconds: float | None = None
    parquet_flush_seconds: float = 300.0
    tripupdates_raw_archive_seconds: float = 300.0
    tripupdates_analysis_sample_seconds: float = 0.0
    delay_change_threshold_seconds: int = 15
    prediction_time_change_threshold_seconds: int = 5
    bkk_stop_distance_change_threshold: int | None = None
    change_tracker_null_guard_rows: int = 1000
    heartbeat_seconds: int = 1800
    disk_warn_free_gb: float = 2.0
    disk_critical_free_gb: float = 0.5
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 15.0
    http_connect_retries: int = 1
    http_backoff_seconds: float = 0.3
    feed_stale_seconds: float = 180.0
    feed_absent_seconds: float = 180.0
    frozen_payload_seconds: float = 300.0
    alerts_frozen_payload_seconds: float = 86400.0
    raw_fsync: bool = True

    def __post_init__(self) -> None:
        if self.poll_interval_seconds is not None and (
            not math.isfinite(self.poll_interval_seconds)
            or self.poll_interval_seconds < MIN_REALTIME_INTERVAL_SECONDS
        ):
            raise ValueError(
                f"POLL_INTERVAL_SECONDS must be at least {MIN_REALTIME_INTERVAL_SECONDS:g} seconds"
            )
        for feed_name, interval in self.feed_intervals.items():
            if not math.isfinite(interval) or interval < MIN_REALTIME_INTERVAL_SECONDS:
                env_name = {
                    "vehiclepositions": "VEHICLE_POSITIONS_INTERVAL_SECONDS",
                    "tripupdates": "TRIP_UPDATES_INTERVAL_SECONDS",
                    "alerts": "ALERTS_INTERVAL_SECONDS",
                }[feed_name]
                raise ValueError(f"{env_name} must be at least {MIN_REALTIME_INTERVAL_SECONDS:g} seconds")
        positive = {
            "heartbeat_seconds": self.heartbeat_seconds,
            "change_tracker_null_guard_rows": self.change_tracker_null_guard_rows,
            "connect_timeout_seconds": self.connect_timeout_seconds,
            "read_timeout_seconds": self.read_timeout_seconds,
            "feed_stale_seconds": self.feed_stale_seconds,
            "feed_absent_seconds": self.feed_absent_seconds,
            "frozen_payload_seconds": self.frozen_payload_seconds,
            "alerts_frozen_payload_seconds": self.alerts_frozen_payload_seconds,
        }
        for name, value in positive.items():
            if not math.isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be greater than zero")
        nonnegative = {
            "parquet_flush_seconds": self.parquet_flush_seconds,
            "tripupdates_raw_archive_seconds": self.tripupdates_raw_archive_seconds,
            "tripupdates_analysis_sample_seconds": self.tripupdates_analysis_sample_seconds,
            "delay_change_threshold_seconds": self.delay_change_threshold_seconds,
            "prediction_time_change_threshold_seconds": self.prediction_time_change_threshold_seconds,
            "bkk_stop_distance_change_threshold": (
                self.bkk_stop_distance_change_threshold
                if self.bkk_stop_distance_change_threshold is not None
                else self.delay_change_threshold_seconds
            ),
            "disk_warn_free_gb": self.disk_warn_free_gb,
            "disk_critical_free_gb": self.disk_critical_free_gb,
            "http_connect_retries": self.http_connect_retries,
            "http_backoff_seconds": self.http_backoff_seconds,
        }
        for name, value in nonnegative.items():
            if not math.isfinite(float(value)) or value < 0:
                raise ValueError(f"{name} must not be negative")
        if self.disk_warn_free_gb < self.disk_critical_free_gb:
            raise ValueError("DISK_WARN_FREE_GB must be at least DISK_CRITICAL_FREE_GB")

    @classmethod
    def from_env(cls) -> "CollectorConfig":
        api_key = os.environ.get("BKK_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("BKK_API_KEY is required")
        analysis_interval = float(os.environ.get("TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS", "0"))
        return cls(
            api_key=api_key,
            data_dir=Path(os.environ.get("DATA_DIR", "/data")),
            poll_interval_seconds=(
                float(os.environ["POLL_INTERVAL_SECONDS"])
                if "POLL_INTERVAL_SECONDS" in os.environ
                else None
            ),
            vehicle_positions_interval_seconds=(
                float(os.environ["VEHICLE_POSITIONS_INTERVAL_SECONDS"])
                if "VEHICLE_POSITIONS_INTERVAL_SECONDS" in os.environ
                else None
            ),
            trip_updates_interval_seconds=(
                float(os.environ["TRIP_UPDATES_INTERVAL_SECONDS"])
                if "TRIP_UPDATES_INTERVAL_SECONDS" in os.environ
                else None
            ),
            alerts_interval_seconds=(
                float(os.environ["ALERTS_INTERVAL_SECONDS"])
                if "ALERTS_INTERVAL_SECONDS" in os.environ
                else None
            ),
            parquet_flush_seconds=float(os.environ.get("PARQUET_FLUSH_MINUTES", "5")) * 60,
            tripupdates_raw_archive_seconds=float(os.environ.get("TRIPUPDATES_RAW_ARCHIVE_SECONDS", "300")),
            tripupdates_analysis_sample_seconds=analysis_interval,
            delay_change_threshold_seconds=int(os.environ.get("DELAY_CHANGE_THRESHOLD_SECONDS", "15")),
            # PREDICTION_TIME_CHANGE_THRESHOLD_SECONDS was the brief pre-release
            # name. Keep it as a fallback so an already prepared deployment
            # does not silently change tolerance.
            prediction_time_change_threshold_seconds=int(
                os.environ.get(
                    "TRIPUPDATE_TIME_TOLERANCE_SECONDS",
                    os.environ.get("PREDICTION_TIME_CHANGE_THRESHOLD_SECONDS", "5"),
                )
            ),
            bkk_stop_distance_change_threshold=(
                int(os.environ["BKK_STOP_DISTANCE_CHANGE_THRESHOLD"])
                if "BKK_STOP_DISTANCE_CHANGE_THRESHOLD" in os.environ
                else None
            ),
            change_tracker_null_guard_rows=int(os.environ.get("CHANGE_TRACKER_NULL_GUARD_ROWS", "1000")),
            heartbeat_seconds=int(os.environ.get("HEARTBEAT_SECONDS", "1800")),
            disk_warn_free_gb=float(os.environ.get("DISK_WARN_FREE_GB", "2.0")),
            disk_critical_free_gb=float(os.environ.get("DISK_CRITICAL_FREE_GB", "0.5")),
            connect_timeout_seconds=float(os.environ.get("HTTP_CONNECT_TIMEOUT_SECONDS", "5")),
            read_timeout_seconds=float(os.environ.get("HTTP_READ_TIMEOUT_SECONDS", "15")),
            http_connect_retries=int(os.environ.get("HTTP_CONNECT_RETRIES", "1")),
            http_backoff_seconds=float(os.environ.get("HTTP_BACKOFF_SECONDS", "0.3")),
            feed_stale_seconds=float(os.environ.get("FEED_STALE_SECONDS", "180")),
            feed_absent_seconds=float(os.environ.get("FEED_ABSENT_SECONDS", "180")),
            frozen_payload_seconds=float(os.environ.get("FROZEN_PAYLOAD_SECONDS", "300")),
            alerts_frozen_payload_seconds=float(os.environ.get("ALERTS_FROZEN_PAYLOAD_SECONDS", "86400")),
            raw_fsync=env_bool("RAW_FSYNC", True),
        )

    @property
    def raw_archive_intervals(self) -> dict[str, float]:
        trip_interval = self.tripupdates_raw_archive_seconds
        if self.tripupdates_analysis_sample_seconds > 0:
            trip_interval = min(trip_interval, self.tripupdates_analysis_sample_seconds)
        return {"vehiclepositions": 0.0, "tripupdates": trip_interval, "alerts": 0.0}

    @property
    def feed_intervals(self) -> dict[str, float]:
        overrides = {
            "vehiclepositions": self.vehicle_positions_interval_seconds,
            "tripupdates": self.trip_updates_interval_seconds,
            "alerts": self.alerts_interval_seconds,
        }
        return {
            feed_name: float(
                overrides[feed_name]
                if overrides[feed_name] is not None
                else self.poll_interval_seconds
                if self.poll_interval_seconds is not None
                else DEFAULT_FEED_INTERVALS[feed_name]
            )
            for feed_name in FEED_NAMES
        }

    @property
    def trip_update_numeric_tolerances(self) -> dict[str, float]:
        prediction = self.prediction_time_change_threshold_seconds
        distance = (
            self.bkk_stop_distance_change_threshold
            if self.bkk_stop_distance_change_threshold is not None
            else self.delay_change_threshold_seconds
        )
        return {
            "arrival_time": float(prediction),
            "departure_time": float(prediction),
            "bkk_stop_distance": float(distance),
        }


@dataclass(frozen=True)
class MaintenanceConfig:
    data_dir: Path
    hf_token: str
    hf_repo_id: str
    backup_hour_utc: int = 3
    backup_retry_seconds: float = 900.0
    backup_max_dates_per_run: int = 7
    backup_date_grace_minutes: int = 30
    prune_local_raw_after_days: int = 14
    static_check_interval_seconds: float = 86400.0
    static_retry_seconds: float = 3600.0
    maintenance_interval_seconds: float = 60.0
    healthcheck_url: str = ""
    healthcheck_ping_interval_seconds: float = 300.0
    compact_parquet: bool = True
    compaction_min_files: int = 12
    backup_stale_hours: float = 48.0

    def __post_init__(self) -> None:
        if not 0 <= self.backup_hour_utc <= 23:
            raise ValueError("BACKUP_HOUR_UTC must be between 0 and 23")
        positive = {
            "backup_retry_seconds": self.backup_retry_seconds,
            "backup_max_dates_per_run": self.backup_max_dates_per_run,
            "static_check_interval_seconds": self.static_check_interval_seconds,
            "static_retry_seconds": self.static_retry_seconds,
            "maintenance_interval_seconds": self.maintenance_interval_seconds,
            "healthcheck_ping_interval_seconds": self.healthcheck_ping_interval_seconds,
            "backup_stale_hours": self.backup_stale_hours,
        }
        for name, value in positive.items():
            if not math.isfinite(float(value)) or value <= 0:
                raise ValueError(f"{name} must be greater than zero")
        if self.backup_date_grace_minutes < 0:
            raise ValueError("BACKUP_DATE_GRACE_MINUTES must not be negative")
        if self.backup_date_grace_minutes > 1439:
            raise ValueError("BACKUP_DATE_GRACE_MINUTES must be below 1440")
        if self.prune_local_raw_after_days < 0:
            raise ValueError("PRUNE_LOCAL_RAW_AFTER_DAYS must not be negative")
        if self.compaction_min_files < 2:
            raise ValueError("PARQUET_COMPACTION_MIN_FILES must be at least 2")

    @classmethod
    def from_env(cls) -> "MaintenanceConfig":
        return cls(
            data_dir=Path(os.environ.get("DATA_DIR", "/data")),
            hf_token=os.environ.get("HF_TOKEN", "").strip(),
            hf_repo_id=os.environ.get("HF_REPO_ID", "").strip(),
            backup_hour_utc=int(os.environ.get("BACKUP_HOUR_UTC", "3")),
            backup_retry_seconds=float(os.environ.get("BACKUP_RETRY_SECONDS", "900")),
            backup_max_dates_per_run=int(os.environ.get("BACKUP_MAX_DATES_PER_RUN", "7")),
            backup_date_grace_minutes=int(os.environ.get("BACKUP_DATE_GRACE_MINUTES", "30")),
            prune_local_raw_after_days=int(os.environ.get("PRUNE_LOCAL_RAW_AFTER_DAYS", "14")),
            static_check_interval_seconds=float(os.environ.get("STATIC_GTFS_CHECK_INTERVAL_SECONDS", "86400")),
            static_retry_seconds=float(os.environ.get("STATIC_GTFS_RETRY_SECONDS", "3600")),
            maintenance_interval_seconds=float(os.environ.get("MAINTENANCE_INTERVAL_SECONDS", "60")),
            healthcheck_url=os.environ.get("HEALTHCHECK_URL", "").strip(),
            healthcheck_ping_interval_seconds=float(os.environ.get("HEALTHCHECK_PING_INTERVAL_SECONDS", "300")),
            compact_parquet=env_bool("PARQUET_COMPACTION_ENABLED", True),
            compaction_min_files=int(os.environ.get("PARQUET_COMPACTION_MIN_FILES", "12")),
            backup_stale_hours=float(os.environ.get("BACKUP_STALE_HOURS", "48")),
        )
