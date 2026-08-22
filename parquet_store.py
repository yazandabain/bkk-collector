"""Crash-safe disk spool and typed Parquet writing."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from atomic_io import atomic_write_bytes, atomic_write_json, fsync_directory, read_json
from config import FEED_NAMES


COMMON_COLUMNS = (
    "schema_version", "poll_id", "request_started_at", "response_received_at", "fetched_at",
    "feed_header_timestamp", "feed_header_version", "feed_incrementality", "feed_incrementality_name",
    "entity_id", "entity_is_deleted",
)
TRIP_COLUMNS = (
    "trip_id", "route_id", "direction_id", "start_time", "start_date", "schedule_relationship",
    "schedule_relationship_name", "modified_trip_json",
)
VEHICLE_COLUMNS = (
    "vehicle_id", "vehicle_label", "vehicle_license_plate", "vehicle_wheelchair_accessible",
    "vehicle_wheelchair_accessible_name", "bkk_vehicle_model", "bkk_deviated", "bkk_vehicle_type",
    "bkk_door_open", "bkk_stop_distance",
)

PARQUET_COLUMNS: dict[str, tuple[str, ...]] = {
    "vehiclepositions": COMMON_COLUMNS + VEHICLE_COLUMNS + (
        "vehicle_timestamp", "latitude", "longitude", "bearing", "odometer", "speed",
        "current_stop_sequence", "stop_id", "current_status", "current_status_name",
        "congestion_level", "congestion_level_name", "occupancy_status", "occupancy_status_name",
        "occupancy_percentage", "multi_carriage_details_json",
    ) + TRIP_COLUMNS,
    "tripupdates": COMMON_COLUMNS + (
        "trip_update_timestamp", "trip_delay", "trip_properties_json",
    ) + VEHICLE_COLUMNS + TRIP_COLUMNS + (
        "stop_time_update_index", "stop_visit_fallback_index", "stop_sequence", "stop_id",
        "stop_schedule_relationship", "stop_schedule_relationship_name",
        "departure_occupancy_status", "departure_occupancy_status_name", "stop_time_properties_json",
        "arrival_delay", "arrival_time", "arrival_uncertainty", "arrival_scheduled_time",
        "departure_delay", "departure_time", "departure_uncertainty", "departure_scheduled_time",
        "bkk_scheduled_arrival_delay", "bkk_scheduled_arrival_time",
        "bkk_scheduled_arrival_uncertainty", "bkk_scheduled_arrival_scheduled_time",
        "bkk_scheduled_departure_delay", "bkk_scheduled_departure_time",
        "bkk_scheduled_departure_uncertainty", "bkk_scheduled_departure_scheduled_time",
    ),
    "alerts": COMMON_COLUMNS + (
        "cause", "cause_name", "effect", "effect_name", "severity_level", "severity_level_name",
        "header_text", "description_text", "url", "header_text_json", "description_text_json",
        "url_json", "tts_header_text_json", "tts_description_text_json", "image_json",
        "image_alternative_text_json", "cause_detail_json", "effect_detail_json",
        "active_period_start", "active_period_end", "active_periods_json", "communication_periods_json",
        "impact_periods_json", "informed_entities_json", "bkk_start_text_json", "bkk_end_text_json",
        "bkk_modified_time", "bkk_route_details_json", "affected_agency_id", "affected_route_id",
        "affected_route_type", "affected_stop_id", "affected_direction_id", "affected_trip_id",
        "affected_trip_route_id", "affected_trip_direction_id", "affected_trip_start_time",
        "affected_trip_start_date", "affected_trip_schedule_relationship",
        "affected_trip_schedule_relationship_name",
    ),
}

BOOL_COLUMNS = {"entity_is_deleted", "bkk_deviated", "bkk_door_open"}
FLOAT_COLUMNS = {"latitude", "longitude", "bearing", "odometer", "speed"}
INT_COLUMNS = {
    "feed_header_timestamp", "feed_incrementality", "vehicle_timestamp", "vehicle_wheelchair_accessible",
    "bkk_vehicle_type", "bkk_stop_distance", "direction_id", "schedule_relationship",
    "current_stop_sequence", "current_status", "congestion_level", "occupancy_status", "occupancy_percentage",
    "trip_update_timestamp", "trip_delay", "stop_time_update_index", "stop_visit_fallback_index",
    "stop_sequence", "stop_schedule_relationship",
    "departure_occupancy_status", "arrival_delay", "arrival_time", "arrival_uncertainty",
    "arrival_scheduled_time", "departure_delay", "departure_time", "departure_uncertainty",
    "departure_scheduled_time", "bkk_scheduled_arrival_delay", "bkk_scheduled_arrival_time",
    "bkk_scheduled_arrival_uncertainty", "bkk_scheduled_arrival_scheduled_time",
    "bkk_scheduled_departure_delay", "bkk_scheduled_departure_time",
    "bkk_scheduled_departure_uncertainty", "bkk_scheduled_departure_scheduled_time",
    "cause", "effect", "severity_level", "active_period_start", "active_period_end", "bkk_modified_time",
    "affected_route_type", "affected_direction_id", "affected_trip_direction_id",
    "affected_trip_schedule_relationship",
}
MAX_SEGMENTS_PER_COMMIT = 20


def parquet_schema(feed_name: str):
    import pyarrow as pa

    fields = []
    for name in PARQUET_COLUMNS[feed_name]:
        if name == "schema_version":
            data_type = pa.int16()
        elif name in BOOL_COLUMNS:
            data_type = pa.bool_()
        elif name in FLOAT_COLUMNS:
            data_type = pa.float64()
        elif name in INT_COLUMNS:
            data_type = pa.int64()
        else:
            data_type = pa.string()
        fields.append(pa.field(name, data_type, nullable=True))
    return pa.schema(fields, metadata={b"bkk_collector_schema_version": b"2", b"feed": feed_name.encode("ascii")})


def rows_to_table(feed_name: str, rows: list[dict[str, Any]]):
    import pyarrow as pa

    schema = parquet_schema(feed_name)
    columns = {name: [row.get(name) for row in rows] for name in PARQUET_COLUMNS[feed_name]}
    return pa.Table.from_pydict(columns, schema=schema)


def write_parquet_atomic(feed_name: str, rows: list[dict[str, Any]], path: Path) -> None:
    import pyarrow.parquet as pq

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        pq.write_table(rows_to_table(feed_name, rows), temporary, compression="zstd", write_statistics=True)
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def parquet_row_count(path: Path) -> int:
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).metadata.num_rows


@dataclass
class FlushResult:
    files_written: list[Path] = field(default_factory=list)
    rows_written: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


class DurableParquetSpool:
    """Small atomic poll batches that survive until a Parquet commit succeeds."""

    def __init__(self, data_dir: Path, flush_seconds: float):
        self.data_dir = data_dir
        self.spool_dir = data_dir / "spool"
        self.parquet_dir = data_dir / "parquet"
        self.flush_seconds = flush_seconds
        # Existing crash-recovery segments should be eligible immediately.
        self._last_flush = time.monotonic() - flush_seconds

    def stage(self, feed_name: str, date_str: str, poll_id: str, rows: list[dict[str, Any]]) -> Path | None:
        if not rows:
            return None
        if feed_name not in FEED_NAMES:
            raise ValueError(f"unknown feed: {feed_name}")
        segment_dir = self.spool_dir / feed_name / f"date={date_str}"
        safe_poll_id = "".join(character for character in poll_id if character.isalnum() or character in "-_")
        path = segment_dir / f"batch-{safe_poll_id}-{uuid.uuid4().hex}.json.gz"
        payload = {"version": 1, "feed": feed_name, "date": date_str, "poll_id": poll_id, "rows": rows}
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        atomic_write_bytes(path, gzip.compress(encoded, compresslevel=6))
        return path

    @staticmethod
    def _read_segment(path: Path) -> list[dict[str, Any]]:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("version") != 1 or not isinstance(payload.get("rows"), list):
            raise ValueError(f"unsupported or corrupt spool segment: {path}")
        return payload["rows"]

    def pending_segments(self) -> list[Path]:
        return sorted(self.spool_dir.glob("*/date=*/batch-*.json.gz")) if self.spool_dir.exists() else []

    def pending_by_feed_date(self) -> dict[tuple[str, str], list[Path]]:
        groups: dict[tuple[str, str], list[Path]] = {}
        for path in self.pending_segments():
            feed_name = path.parent.parent.name
            date_str = path.parent.name.removeprefix("date=")
            groups.setdefault((feed_name, date_str), []).append(path)
        return groups

    def _recover_commits(self, result: FlushResult) -> set[tuple[str, str]]:
        """Finish cleanup for Parquet files committed before an interruption."""
        blocked: set[tuple[str, str]] = set()
        if not self.spool_dir.exists():
            return blocked
        for marker in sorted(self.spool_dir.glob("*/date=*/commit-*.json")):
            feed_name = marker.parent.parent.name
            date_str = marker.parent.name.removeprefix("date=")
            key = (feed_name, date_str)
            try:
                transaction = read_json(marker, {})
                source_names = transaction.get("sources")
                output_name = transaction.get("output")
                expected_rows = transaction.get("expected_rows")
                if (
                    transaction.get("version") != 1
                    or feed_name not in FEED_NAMES
                    or not isinstance(source_names, list)
                    or not source_names
                    or any(not isinstance(name, str) or Path(name).name != name for name in source_names)
                    or not isinstance(output_name, str)
                    or Path(output_name).name != output_name
                    or not isinstance(expected_rows, int)
                    or expected_rows < 0
                ):
                    raise ValueError(f"invalid spool commit marker: {marker}")
                sources = [marker.parent / name for name in source_names]
                output = self.parquet_dir / feed_name / f"date={date_str}" / output_name
                if output.exists() and parquet_row_count(output) == expected_rows:
                    for source in sources:
                        try:
                            source.unlink()
                        except FileNotFoundError:
                            pass
                    marker.unlink()
                    fsync_directory(marker.parent)
                    continue
                if all(source.exists() for source in sources):
                    # No source was consumed, so discard the interrupted intent
                    # and let the normal deterministic commit retry below.
                    marker.unlink()
                    fsync_directory(marker.parent)
                    continue
                raise IOError("spool commit has missing sources and no validated Parquet output")
            except Exception as error:
                blocked.add(key)
                result.errors.append(f"{feed_name}/{date_str} recovery: {type(error).__name__}: {error}")
        return blocked

    def _commit_segments(
        self,
        feed_name: str,
        date_str: str,
        segments: list[Path],
        result: FlushResult,
    ) -> None:
        rows: list[dict[str, Any]] = []
        for segment in segments:
            rows.extend(self._read_segment(segment))
        identity = "\n".join(path.name for path in segments).encode("utf-8")
        digest = hashlib.sha256(identity).hexdigest()[:20]
        out_dir = self.parquet_dir / feed_name / f"date={date_str}"
        out_path = out_dir / f"part-{segments[0].stem[6:22]}-{digest}.parquet"
        marker = segments[0].parent / f"commit-{digest}.json"
        atomic_write_json(
            marker,
            {
                "version": 1,
                "feed": feed_name,
                "date": date_str,
                "sources": [path.name for path in segments],
                "output": out_path.name,
                "expected_rows": len(rows),
            },
        )
        if not out_path.exists() or parquet_row_count(out_path) != len(rows):
            write_parquet_atomic(feed_name, rows, out_path)
        if parquet_row_count(out_path) != len(rows):
            raise IOError(f"row-count validation failed for {out_path}")
        for segment in segments:
            segment.unlink()
        marker.unlink()
        fsync_directory(segments[0].parent)
        result.files_written.append(out_path)
        result.rows_written += len(rows)

    def flush(self, *, force: bool = False) -> FlushResult:
        result = FlushResult()
        blocked = self._recover_commits(result)
        if not force and time.monotonic() - self._last_flush < self.flush_seconds:
            return result
        groups = self.pending_by_feed_date()
        for (feed_name, date_str), segments in sorted(groups.items()):
            if (feed_name, date_str) in blocked:
                continue
            for start in range(0, len(segments), MAX_SEGMENTS_PER_COMMIT):
                batch = segments[start : start + MAX_SEGMENTS_PER_COMMIT]
                try:
                    self._commit_segments(feed_name, date_str, batch, result)
                except Exception as error:
                    # This batch and every not-yet-attempted segment remain in
                    # place. Bounded batches avoid an OOM after a long outage.
                    result.errors.append(f"{feed_name}/{date_str}: {type(error).__name__}: {error}")
                    break
        if not result.errors:
            self._last_flush = time.monotonic()
        return result


def ensure_empty_parquet(data_dir: Path, feed_name: str, date_str: str) -> Path:
    out = data_dir / "parquet" / feed_name / f"date={date_str}" / "part-empty.parquet"
    if not out.exists():
        write_parquet_atomic(feed_name, [], out)
    return out
