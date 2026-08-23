"""Streaming, read-only diagnostics over archived TripUpdates and static GTFS."""

from __future__ import annotations

import csv
import sqlite3
import tempfile
import zipfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from atomic_io import sha256_file
from gtfs_rt_parse import parse_feed
from raw_log import iter_records
from static_gtfs import StaticGtfsStore, validate_gtfs_zip


DEFAULT_REVISION_THRESHOLDS = (0, 1, 2, 5, 10, 15, 20, 30, 60)


class RevisionDistribution:
    """Exact integer-second distribution without retaining millions of values."""

    def __init__(self) -> None:
        self.histogram: Counter[int] = Counter()
        self.count = 0

    def add(self, value: int) -> None:
        self.histogram[abs(int(value))] += 1
        self.count += 1

    def quantile(self, fraction: float) -> int | None:
        if not self.count:
            return None
        target = int((self.count - 1) * fraction)
        cumulative = 0
        for value, count in sorted(self.histogram.items()):
            cumulative += count
            if cumulative > target:
                return value
        raise AssertionError("revision histogram count mismatch")

    def report(self, thresholds: Iterable[int] = DEFAULT_REVISION_THRESHOLDS) -> dict[str, Any]:
        return {
            "comparisons": self.count,
            "p50_seconds": self.quantile(0.50),
            "p75_seconds": self.quantile(0.75),
            "p90_seconds": self.quantile(0.90),
            "p95_seconds": self.quantile(0.95),
            "p99_seconds": self.quantile(0.99),
            "max_seconds": max(self.histogram) if self.histogram else None,
            "above_threshold": {
                str(threshold): {
                    "count": sum(count for value, count in self.histogram.items() if value > threshold),
                    "percent": (
                        100.0 * sum(count for value, count in self.histogram.items() if value > threshold) / self.count
                        if self.count
                        else None
                    ),
                }
                for threshold in thresholds
            },
            "most_common_seconds": self.histogram.most_common(20),
        }


def prediction_revision_report(
    raw_path: Path,
    *,
    start_hour: int = 0,
    end_hour: int = 24,
    max_snapshots: int | None = None,
) -> dict[str, Any]:
    """Measure absolute prediction revisions from complete raw snapshots.

    Large revisions are reported unchanged.  This command is diagnostic only;
    it never filters, clamps, or writes collector data.
    """
    if not 0 <= start_hour < end_hour <= 24:
        raise ValueError("hours must satisfy 0 <= start-hour < end-hour <= 24")
    if max_snapshots is not None and max_snapshots <= 0:
        raise ValueError("max_snapshots must be greater than zero")
    last: dict[tuple[Any, ...], int] = {}
    arrivals = RevisionDistribution()
    departures = RevisionDistribution()
    snapshots = 0
    with open(raw_path, "rb") as handle:
        for timestamp, raw in iter_records(handle):
            hour = int(timestamp // 3600) % 24
            if hour < start_hour or hour >= end_hour:
                continue
            feed = parse_feed(raw)
            snapshots += 1
            for entity in feed.entity:
                if not entity.HasField("trip_update"):
                    continue
                update = entity.trip_update
                trip = update.trip
                for fallback_index, stop in enumerate(update.stop_time_update):
                    sequence = stop.stop_sequence if stop.HasField("stop_sequence") else None
                    identity = (
                        entity.id or None,
                        trip.trip_id or None,
                        trip.start_date or None,
                        trip.start_time or None,
                        sequence,
                        fallback_index if sequence is None else None,
                        stop.stop_id or None,
                    )
                    for event_name, event, distribution in (
                        ("arrival", stop.arrival, arrivals),
                        ("departure", stop.departure, departures),
                    ):
                        if not stop.HasField(event_name) or not event.HasField("time"):
                            continue
                        key = (*identity, event_name)
                        predicted = int(event.time)
                        previous = last.get(key)
                        if previous is not None:
                            distribution.add(predicted - previous)
                        last[key] = predicted
            if max_snapshots is not None and snapshots >= max_snapshots:
                break
    return {
        "raw_path": str(raw_path),
        "snapshot_interval_note": "interpret rates at the raw archive cadence, not the realtime polling cadence",
        "snapshots_analyzed": snapshots,
        "arrival": arrivals.report(),
        "departure": departures.report(),
    }


def _zip_member(archive: zipfile.ZipFile, basename: str) -> str:
    matches = [name for name in archive.namelist() if name.rsplit("/", 1)[-1] == basename]
    if len(matches) != 1:
        raise ValueError(f"static GTFS must contain exactly one {basename}")
    return matches[0]


def _route_mode(route_type: int | None) -> str:
    if route_type in (0, 900):
        return "tram"
    if route_type == 1:
        return "metro"
    if route_type == 11 or route_type == 800:
        return "trolleybus"
    if route_type == 3 or (route_type is not None and 700 <= route_type < 800):
        return "bus"
    if route_type == 4 or (route_type is not None and 1000 <= route_type < 1100):
        return "ferry"
    if route_type == 109:
        return "hev"
    if route_type == 2 or (route_type is not None and 100 <= route_type < 200):
        return "rail"
    return f"route_type_{route_type}" if route_type is not None else "static_route_type_unknown"


class _StaticJoinIndex:
    """Disk-backed exact join index to keep diagnostic memory bounded."""

    def __init__(self, zip_path: Path, database_path: Path):
        validate_gtfs_zip(zip_path)
        self.connection = sqlite3.connect(database_path)
        self.connection.executescript(
            """
            PRAGMA journal_mode=OFF;
            PRAGMA synchronous=OFF;
            CREATE TABLE routes(route_id TEXT PRIMARY KEY, route_type INTEGER);
            CREATE TABLE trips(trip_id TEXT PRIMARY KEY, route_id TEXT);
            CREATE TABLE stop_times(
                trip_id TEXT NOT NULL,
                stop_sequence INTEGER NOT NULL,
                stop_id TEXT NOT NULL,
                PRIMARY KEY(trip_id, stop_sequence, stop_id)
            ) WITHOUT ROWID;
            CREATE TABLE observed(
                trip_id TEXT NOT NULL,
                stop_sequence INTEGER NOT NULL,
                stop_id TEXT NOT NULL,
                realtime_route_id TEXT NOT NULL,
                observations INTEGER NOT NULL,
                PRIMARY KEY(trip_id, stop_sequence, stop_id, realtime_route_id)
            ) WITHOUT ROWID;
            """
        )
        with zipfile.ZipFile(zip_path) as archive:
            self._load_csv(
                archive,
                "routes.txt",
                "INSERT OR REPLACE INTO routes VALUES (?, ?)",
                lambda row: (row.get("route_id", ""), _optional_int(row.get("route_type"))),
            )
            self._load_csv(
                archive,
                "trips.txt",
                "INSERT OR REPLACE INTO trips VALUES (?, ?)",
                lambda row: (row.get("trip_id", ""), row.get("route_id", "")),
            )
            self._load_csv(
                archive,
                "stop_times.txt",
                "INSERT OR IGNORE INTO stop_times VALUES (?, ?, ?)",
                lambda row: (
                    row.get("trip_id", ""),
                    int(row.get("stop_sequence", "-1")),
                    row.get("stop_id", ""),
                ),
            )
        self.connection.commit()

    def _load_csv(self, archive: zipfile.ZipFile, name: str, sql: str, transform: Any) -> None:
        member = _zip_member(archive, name)
        batch: list[tuple[Any, ...]] = []
        with archive.open(member) as binary:
            rows = csv.DictReader((line.decode("utf-8-sig") for line in binary))
            for row in rows:
                batch.append(transform(row))
                if len(batch) >= 10_000:
                    self.connection.executemany(sql, batch)
                    batch.clear()
        if batch:
            self.connection.executemany(sql, batch)

    def add(self, observations: Counter[tuple[str, int, str, str]]) -> None:
        self.connection.executemany(
            """
            INSERT INTO observed VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(trip_id, stop_sequence, stop_id, realtime_route_id)
            DO UPDATE SET observations=observations+excluded.observations
            """,
            ((*key, count) for key, count in observations.items()),
        )
        self.connection.commit()

    def summarize(self) -> tuple[dict[str, Counter[str]], Counter[str]]:
        modes: dict[str, Counter[str]] = {}
        external_routes: Counter[str] = Counter()
        rows = self.connection.execute(
            """
            SELECT r.route_type, t.trip_id IS NOT NULL, s.trip_id IS NOT NULL,
                   o.realtime_route_id, SUM(o.observations)
            FROM observed o
            LEFT JOIN trips t ON t.trip_id=o.trip_id
            LEFT JOIN routes r ON r.route_id=t.route_id
            LEFT JOIN stop_times s ON s.trip_id=o.trip_id
              AND s.stop_sequence=o.stop_sequence AND s.stop_id=o.stop_id
            GROUP BY r.route_type, t.trip_id IS NOT NULL, s.trip_id IS NOT NULL,
                     o.realtime_route_id
            """
        )
        for route_type, trip_matched, exact_matched, realtime_route_id, count in rows:
            count = int(count)
            mode = _route_mode(route_type) if trip_matched else "external_or_unmatched_valid_realtime"
            stats = modes.setdefault(mode, Counter())
            stats["stop_updates"] += count
            if trip_matched:
                stats["trip_id_matched"] += count
            if exact_matched:
                stats["exact_stop_matched"] += count
            if not trip_matched:
                external_routes[realtime_route_id or "(missing_route_id)"] += count
        return modes, external_routes

    def close(self) -> None:
        self.connection.close()


def _optional_int(value: str | None) -> int | None:
    try:
        return int(value) if value not in (None, "") else None
    except ValueError:
        return None


def tripupdate_static_join_report(
    data_dir: Path,
    date_str: str,
    *,
    warning_rate: float = 0.90,
    max_snapshots: int | None = None,
) -> dict[str, Any]:
    """Report exact TripUpdate-to-static joins without dropping unmatched rows."""
    try:
        if datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d") != date_str:
            raise ValueError
    except ValueError as error:
        raise ValueError("date must be YYYY-MM-DD") from error
    if not 0 <= warning_rate <= 1:
        raise ValueError("warning rate must be between zero and one")
    if max_snapshots is not None and max_snapshots <= 0:
        raise ValueError("max_snapshots must be greater than zero")
    raw_path = data_dir / "raw" / "tripupdates" / f"date={date_str}" / "tripupdates.rawlog"
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)
    store = StaticGtfsStore(data_dir, check_interval_seconds=86400, retry_seconds=3600, create_root=False)
    versions: dict[str, dict[str, Any]] = {}
    unresolved_stop_updates = 0
    snapshots = 0
    with tempfile.TemporaryDirectory(prefix="bkk-static-join-") as temporary:
        indexes: dict[str, _StaticJoinIndex] = {}
        try:
            with open(raw_path, "rb") as handle:
                for timestamp, raw in iter_records(handle):
                    feed = parse_feed(raw)
                    snapshots += 1
                    version = store.observed_version_at(timestamp)
                    observations: Counter[tuple[str, int, str, str]] = Counter()
                    for entity in feed.entity:
                        if not entity.HasField("trip_update"):
                            continue
                        update = entity.trip_update
                        trip = update.trip
                        for stop in update.stop_time_update:
                            sequence = int(stop.stop_sequence) if stop.HasField("stop_sequence") else -1
                            observations[(
                                trip.trip_id or "",
                                sequence,
                                stop.stop_id or "",
                                trip.route_id or "",
                            )] += 1
                    if version is None:
                        unresolved_stop_updates += sum(observations.values())
                    else:
                        digest = str(version.get("sha256") or "")
                        relative = version.get("version_path")
                        if not digest or not isinstance(relative, str):
                            raise ValueError("static history event lacks hash/path")
                        zip_path = store.archive_path(version)
                        if digest not in indexes:
                            if not zip_path.exists() or sha256_file(zip_path) != digest:
                                raise ValueError(f"static archive missing or hash mismatch: {relative}")
                            indexes[digest] = _StaticJoinIndex(
                                zip_path, Path(temporary) / f"{digest}.sqlite"
                            )
                            versions[digest] = {
                                "sha256": digest,
                                "version_path": relative,
                                "feed_version": version.get("feed_version"),
                                "source": version.get("source"),
                                "applicability_confidence": version.get("applicability_confidence"),
                                "confidence_snapshots": {},
                                "snapshots": 0,
                            }
                        versions[digest]["snapshots"] += 1
                        confidence = str(version.get("applicability_confidence") or "unknown")
                        confidence_counts = versions[digest]["confidence_snapshots"]
                        confidence_counts[confidence] = int(confidence_counts.get(confidence, 0)) + 1
                        if len(confidence_counts) > 1:
                            versions[digest]["applicability_confidence"] = "mixed"
                        indexes[digest].add(observations)
                    if max_snapshots is not None and snapshots >= max_snapshots:
                        break

            combined_modes: dict[str, Counter[str]] = {}
            external_routes: Counter[str] = Counter()
            for index in indexes.values():
                modes, routes = index.summarize()
                for mode, stats in modes.items():
                    combined_modes.setdefault(mode, Counter()).update(stats)
                external_routes.update(routes)
        finally:
            for index in indexes.values():
                index.close()

    total = unresolved_stop_updates + sum(stats["stop_updates"] for stats in combined_modes.values())
    trip_matches = sum(stats["trip_id_matched"] for stats in combined_modes.values())
    exact_matches = sum(stats["exact_stop_matched"] for stats in combined_modes.values())
    warnings: list[str] = []
    if unresolved_stop_updates:
        warnings.append(f"{unresolved_stop_updates} stop updates have no observed static version")
    if any(
        "legacy_schedule_uncertain" in value.get("confidence_snapshots", {})
        for value in versions.values()
    ):
        warnings.append("legacy static schedule applicability is uncertain; no missing version was inferred")
    if total and trip_matches / total < warning_rate:
        warnings.append(
            f"overall trip ID join rate {trip_matches / total:.2%} is below {warning_rate:.2%}; "
            "unmatched external services remain valid observations"
        )
    if total and exact_matches / total < warning_rate:
        warnings.append(
            f"overall exact stop join rate {exact_matches / total:.2%} is below {warning_rate:.2%}; "
            "check static-version confidence and unmatched external services"
        )
    mode_report: dict[str, Any] = {}
    for mode, stats in sorted(combined_modes.items()):
        count = stats["stop_updates"]
        exact = stats["exact_stop_matched"]
        rate = exact / count if count else None
        mode_report[mode] = {
            "stop_updates": count,
            "trip_id_matched": stats["trip_id_matched"],
            "exact_stop_matched": exact,
            "exact_stop_match_rate": rate,
        }
        if mode != "external_or_unmatched_valid_realtime" and rate is not None and rate < warning_rate:
            warnings.append(f"{mode} exact stop join rate {rate:.2%} is below {warning_rate:.2%}")
    return {
        "date": date_str,
        "raw_path": str(raw_path),
        "snapshots_analyzed": snapshots,
        "static_versions_used": list(versions.values()),
        "unresolved_static_stop_updates": unresolved_stop_updates,
        "overall": {
            "stop_updates": total,
            "trip_id_matched": trip_matches,
            "trip_id_match_rate": trip_matches / total if total else None,
            "exact_stop_matched": exact_matches,
            "exact_stop_match_rate": exact_matches / total if total else None,
        },
        "by_mode": mode_report,
        "unmatched_realtime_route_ids": dict(external_routes.most_common()),
        "quality_warnings": warnings,
        "collector_health_impact": "none; join quality is diagnostic evidence, not collector liveness",
    }
