"""Closed public projection: never forward operational objects or errors."""

from __future__ import annotations

import hashlib
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bkk_collector.realtime.gtfs_rt_parse import parse_feed, parse_vehicle_positions
from public_exporter.source import LatestVehicleFrame, MODE_COLORS, RouteCatalog, label, read_object

FEEDS = ("vehiclepositions", "tripupdates", "alerts")
ISSUES = frozenset({
    "absent", "http_failed", "poll_journal_failed", "protobuf_parse_failed", "source_timestamp_stale",
    "source_timestamp_frozen", "entity_timestamp_stale", "payload_frozen", "raw_archive_failed",
    "derived_spool_failed", "tripupdates_presence_failed", "change_tracker_failed", "request_stuck",
    "scheduler_overdue", "source_timestamp_unchanged_warning", "payload_unchanged_warning",
    "request_exceeds_cadence", "scheduler_missed_deadline",
})


def iso(timestamp: Any) -> str | None:
    if not isinstance(timestamp, (float, int)) or isinstance(timestamp, bool) or not math.isfinite(timestamp):
        return None
    try:
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except (ValueError, OverflowError, OSError):
        return None


def safe_iso(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return iso(parsed.timestamp()) if parsed.tzinfo else None
    except ValueError:
        return None


def count(value: Any) -> int | None:
    return value if type(value) is int and 0 <= value <= 9_000_000_000_000_000 else None


def load_optional(path: Path) -> dict[str, Any]:
    try:
        return read_object(path)
    except (OSError, ValueError, TypeError):
        return {}


def health(root: Path, now: float) -> dict[str, Any]:
    status = load_optional(root / "health/status.json")
    stamp = status.get("updated_timestamp")
    current = isinstance(stamp, (int, float)) and -30 <= now - stamp <= 180
    state = "unknown" if not current else "healthy" if status.get("healthy") is True else "degraded"
    feeds = {}
    for name in FEEDS:
        source = status.get("feeds", {}).get(name, {})
        flags = set(source.get("freshness_flags", []))
        # Private diagnostic names can evolve. Explicit failure booleans still
        # map to closed public codes instead of making a failed feed look healthy.
        for field, code in {
            "parse_ok": "protobuf_parse_failed", "raw_ok": "raw_archive_failed",
            "spool_ok": "derived_spool_failed", "presence_ok": "tripupdates_presence_failed",
            "change_tracking_ok": "change_tracker_failed", "poll_journal_ok": "poll_journal_failed",
        }.items():
            if source.get(field) is False:
                flags.add(code)
        for item in status.get("reasons", []) + status.get("warnings", []):
            if isinstance(item, str) and item.startswith(name + ":"):
                flags.add(item.partition(":")[2])
        codes = sorted(flag for flag in flags if flag in ISSUES)
        cadence = source.get("scheduler", {}).get("interval_seconds")
        cadence = cadence if type(cadence) in (int, float) and 5 <= cadence <= 3600 else None
        feeds[name] = {
            "state": "unknown" if not current or not source else "degraded" if any(not code.endswith("_warning") and code not in
                        {"request_exceeds_cadence", "scheduler_missed_deadline"} for code in codes) else "healthy",
            "observed_at": iso(source.get("last_success_timestamp")),
            "source_at": iso(source.get("header_timestamp")),
            "cadence_seconds": cadence,
            "entities": count(source.get("entity_count")),
            "issues": codes,
        }
    maintenance = load_optional(root / "maintenance/status.json")
    maintenance_stamp = maintenance.get("updated_timestamp")
    maintenance_current = isinstance(maintenance_stamp, (int, float)) and -30 <= now - maintenance_stamp <= 300
    return {
        "state": state, "observed_at": iso(stamp), "feeds": feeds,
        "maintenance": {
            "state": "unknown" if not maintenance_current else "healthy" if maintenance.get("healthy") is True else "degraded",
            "archive_enabled": maintenance.get("backup_enabled") is True,
            "pending_days": len(maintenance.get("pending_backup_dates", [])),
        },
    }


def valid_date(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 10:
        return False
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%Y-%m-%d") == value
    except ValueError:
        return False


def statistics(root: Path, now: float) -> dict[str, Any]:
    today = datetime.fromtimestamp(now, timezone.utc).date().isoformat()
    evidenced_days = polls = event_rows = raw_snapshots = 0
    earliest = None
    latest = None
    for path in sorted((root / "manifests").glob("date=*.json")):
        manifest = load_optional(path)
        date = manifest.get("date")
        feeds = manifest.get("feeds", {})
        if valid_date(date) and date < today and any((count(feeds.get(name, {}).get("attempted_polls")) or 0) > 0 for name in FEEDS):
            evidenced_days += 1
            for name in FEEDS:
                source = feeds.get(name, {})
                stamp = safe_iso(source.get("first_success_at"))
                if stamp and (earliest is None or stamp < earliest):
                    earliest = stamp
                polls += count(source.get("attempted_polls")) or 0
                event_rows += count(source.get("parquet_rows")) or 0
                raw_snapshots += count(source.get("raw_snapshots")) or 0
            # Retain only the public projection, never months of artifact lists.
            if latest is None or date > latest["date"]:
                latest = {
                    "date": date, "quality": "good" if manifest.get("quality_ok") is True else "flagged",
                    "feeds": {name: {
                        "expected_polls": count(feeds.get(name, {}).get("expected_polls")),
                        "recorded_polls": count(feeds.get(name, {}).get("attempted_polls")),
                        "data_polls": count(feeds.get(name, {}).get("successful_data_polls")),
                    } for name in FEEDS},
                }
    verified_dates = []
    for path in (root / "receipts").glob("date=*.json"):
        receipt = load_optional(path)
        if (receipt.get("version") == 2 and receipt.get("remote_verified") is True and valid_date(receipt.get("date"))
                and path.name == f"date={receipt['date']}.json" and receipt["date"] < today):
            verified_dates.append(receipt["date"])
    return {
        "as_of": iso(now), "evidenced_since": earliest, "evidenced_days": evidenced_days,
        "verified_days": len(verified_dates), "last_verified_date": max(verified_dates, default=None),
        "polls_recorded": polls, "event_rows": event_rows, "raw_snapshots": raw_snapshots, "latest_day": latest,
    }


class SnapshotBuilder:
    def __init__(self, root: Path):
        self.root = root
        self.raw = LatestVehicleFrame(root / "raw")
        self.catalog = RouteCatalog(root / "static")
        self.stats: dict[str, Any] | None = None
        self.stats_at = 0.0

    def build(self, now: float) -> dict[str, Any]:
        issues = []
        try:
            routes = self.catalog.read()
        except (OSError, ValueError, KeyError, TypeError):
            routes = {}
            issues.append("route_catalog_unavailable")
        vehicles = {"type": "FeatureCollection", "observed_at": None, "source_at": None,
                    "records_in_source": 0, "omitted_records": 0, "features": []}
        try:
            timestamp, payload = self.raw.read(datetime.fromtimestamp(now, timezone.utc).date())
            feed = parse_feed(payload)
            # A differential feed is not a full fleet snapshot; do not present
            # an incomplete subset as all current vehicles.
            if feed.header.incrementality != 0:
                raise ValueError("not a full vehicle snapshot")
            rows = parse_vehicle_positions(feed, iso(timestamp) or "")
            vehicles.update(observed_at=iso(timestamp), source_at=iso(feed.header.timestamp) if feed.header.HasField("timestamp") else None,
                            records_in_source=len(rows))
            seen = set()
            for row in rows:
                lon, lat = row.get("longitude"), row.get("latitude")
                if row.get("entity_is_deleted") or not all(type(value) in (float, int) and math.isfinite(value) for value in (lon, lat)):
                    continue
                if not -180 <= lon <= 180 or not -90 <= lat <= 90 or (lon == 0 and lat == 0):
                    continue
                identifier = hashlib.sha256(str(row.get("vehicle_id") or row.get("entity_id")).encode()).hexdigest()[:20]
                if identifier in seen or len(seen) >= 10000:
                    continue
                seen.add(identifier)
                route_id = label(row.get("route_id"))
                route = routes.get(route_id, {"label": route_id or "Unassigned", "mode": "other", "color": MODE_COLORS["other"]})
                bearing = row.get("bearing")
                bearing = float(bearing) if type(bearing) in (float, int) and math.isfinite(bearing) and 0 <= bearing <= 360 else None
                vehicles["features"].append({
                    "type": "Feature", "id": identifier, "geometry": {"type": "Point", "coordinates": [round(lon, 6), round(lat, 6)]},
                    "properties": {"route_label": route["label"], "mode": route["mode"], "color": route["color"],
                                   "bearing": bearing, "recorded_at": iso(row.get("vehicle_timestamp"))},
                })
            vehicles["omitted_records"] = len(rows) - len(vehicles["features"])
        except Exception:
            # No exception message/raw dump can cross the public boundary.
            issues.append("vehicle_snapshot_unavailable")
        if self.stats is None or now - self.stats_at >= 300:
            self.stats = statistics(self.root, now)
            self.stats_at = now
        return {"schema_version": 1, "generated_at": iso(now), "vehicles": vehicles,
                "health": health(self.root, now), "statistics": self.stats, "public_layer_issues": issues}
