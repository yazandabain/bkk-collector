"""Chunked, crash-safe reconstruction of derived Parquet from raw logs."""

from __future__ import annotations

import argparse
import os
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from atomic_io import atomic_write_json, fsync_directory, sha256_file
from config import FEED_NAMES
from dedup import ChangeTracker
from gtfs_rt_parse import PARSERS, parse_feed
from parquet_store import write_parquet_atomic
from raw_log import iter_records, scan_raw_log


DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"
HEARTBEAT_SECONDS = int(os.environ.get("HEARTBEAT_SECONDS", "1800"))
DELAY_CHANGE_THRESHOLD_SECONDS = int(os.environ.get("DELAY_CHANGE_THRESHOLD_SECONDS", "15"))


def make_tracker(feed_name: str) -> ChangeTracker | None:
    if feed_name == "tripupdates":
        return ChangeTracker(
            key_fields=(
                "entity_id", "trip_id", "start_date", "start_time", "stop_sequence",
                "stop_visit_fallback_index", "stop_id",
            ),
            value_fields=(
                "route_id", "direction_id", "schedule_relationship", "vehicle_id",
                "stop_schedule_relationship", "departure_occupancy_status", "stop_time_properties_json",
                "arrival_uncertainty", "departure_uncertainty",
                "bkk_scheduled_arrival_time", "bkk_scheduled_departure_time",
            ),
            numeric_tolerance_fields=("arrival_delay", "departure_delay", "trip_delay", "arrival_time", "departure_time"),
            tolerance=DELAY_CHANGE_THRESHOLD_SECONDS,
            heartbeat_seconds=HEARTBEAT_SECONDS,
        )
    if feed_name == "alerts":
        return ChangeTracker(
            key_fields=(
                "entity_id", "affected_agency_id", "affected_route_id", "affected_route_type",
                "affected_stop_id", "affected_direction_id", "affected_trip_id",
                "affected_trip_start_date", "affected_trip_start_time",
            ),
            value_fields=(
                "cause", "effect", "severity_level", "header_text_json", "description_text_json", "url_json",
                "active_periods_json", "communication_periods_json", "impact_periods_json", "informed_entities_json",
                "bkk_start_text_json", "bkk_end_text_json", "bkk_modified_time", "bkk_route_details_json",
            ),
            heartbeat_seconds=HEARTBEAT_SECONDS,
        )
    return None


def _flush_chunk(feed_name: str, rows: list[dict], staging: Path, index: int) -> int:
    path = staging / f"part-rebuilt-{index:06d}.parquet"
    write_parquet_atomic(feed_name, rows, path)
    return len(rows)


def _install_rebuild(feed_name: str, date_str: str, staging: Path) -> Path | None:
    destination = PARQUET_DIR / feed_name / f"date={date_str}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    previous = None
    if destination.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        previous = DATA_DIR / "parquet_rebuild_previous" / feed_name / f"date={date_str}-{stamp}-{uuid.uuid4().hex}"
        previous.parent.mkdir(parents=True, exist_ok=True)
        os.replace(destination, previous)
    try:
        os.replace(staging, destination)
        fsync_directory(destination.parent)
    except Exception:
        if previous is not None and previous.exists() and not destination.exists():
            os.replace(previous, destination)
        raise
    return previous


def rebuild_one(
    feed_name: str,
    date_str: str,
    *,
    chunk_rows: int = 100_000,
    allow_parse_errors: bool = False,
    allow_current_date: bool = False,
) -> dict:
    if date_str >= datetime.now(timezone.utc).strftime("%Y-%m-%d") and not allow_current_date:
        raise ValueError("refusing to rebuild the current/future UTC date while the collector may still write it")
    raw_path = RAW_DIR / feed_name / f"date={date_str}" / f"{feed_name}.rawlog"
    if not raw_path.exists():
        raise FileNotFoundError(raw_path)
    scan = scan_raw_log(raw_path)
    if not scan.clean and not allow_parse_errors:
        raise ValueError("raw log has an invalid tail; pass --allow-parse-errors only after reviewing it")

    staging = PARQUET_DIR / ".rebuild-staging" / feed_name / f"date={date_str}-{uuid.uuid4().hex}"
    staging.mkdir(parents=True, exist_ok=False)
    tracker = make_tracker(feed_name)
    buffer: list[dict] = []
    records = 0
    failures = 0
    written_rows = 0
    part = 0
    try:
        with open(raw_path, "rb") as handle:
            for record_index, (timestamp, raw_bytes) in enumerate(iter_records(handle)):
                records += 1
                received = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                poll_id = f"rebuild-{feed_name}-{date_str}-{record_index:08d}"
                try:
                    feed = parse_feed(raw_bytes)
                    rows = PARSERS[feed_name](
                        feed,
                        {
                            "poll_id": poll_id,
                            "request_started_at": received,
                            "response_received_at": received,
                        },
                    )
                    if tracker:
                        rows = tracker.filter(rows, date_str, timestamp)
                    buffer.extend(rows)
                    while len(buffer) >= chunk_rows:
                        chunk = buffer[:chunk_rows]
                        del buffer[:chunk_rows]
                        written_rows += _flush_chunk(feed_name, chunk, staging, part)
                        part += 1
                except Exception as error:
                    failures += 1
                    print(f"  parse failed at {received}: {type(error).__name__}: {error}")
        if failures and not allow_parse_errors:
            raise RuntimeError(f"{failures} raw record(s) failed parsing; existing Parquet was left untouched")
        if buffer or part == 0:
            written_rows += _flush_chunk(feed_name, buffer, staging, part)
        rebuild_manifest = {
            "version": 1,
            "feed": feed_name,
            "date": date_str,
            "raw_path": str(raw_path.relative_to(DATA_DIR)),
            "raw_sha256": sha256_file(raw_path),
            "raw_records": records,
            "raw_log_clean": scan.clean,
            "parse_failures": failures,
            "rows": written_rows,
            "chunk_rows": chunk_rows,
            "rebuilt_at": datetime.now(timezone.utc).isoformat(),
            "limitation": "resolution is limited to timestamps present in the raw log",
        }
        atomic_write_json(staging / "rebuild_manifest.json", rebuild_manifest)
        previous = _install_rebuild(feed_name, date_str, staging)
        return {**rebuild_manifest, "previous_partition": str(previous) if previous else None}
    except Exception:
        # Only this invocation's never-installed staging data is removed.
        shutil.rmtree(staging, ignore_errors=True)
        raise


def all_dates_for(feed_name: str) -> list[str]:
    root = RAW_DIR / feed_name
    if not root.exists():
        return []
    return sorted(path.name.removeprefix("date=") for path in root.glob("date=*"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="YYYY-MM-DD; default is all completed raw dates")
    parser.add_argument("--feed", choices=FEED_NAMES, help="default: all feeds")
    parser.add_argument("--chunk-rows", type=int, default=100_000)
    parser.add_argument("--allow-parse-errors", action="store_true")
    parser.add_argument("--allow-current-date", action="store_true")
    args = parser.parse_args()
    if args.chunk_rows <= 0:
        parser.error("--chunk-rows must be positive")

    for feed_name in ([args.feed] if args.feed else FEED_NAMES):
        dates = [args.date] if args.date else all_dates_for(feed_name)
        print(f"{feed_name}: rebuilding {len(dates)} date(s)")
        for date_str in dates:
            try:
                result = rebuild_one(
                    feed_name,
                    date_str,
                    chunk_rows=args.chunk_rows,
                    allow_parse_errors=args.allow_parse_errors,
                    allow_current_date=args.allow_current_date,
                )
            except Exception as error:
                print(f"  {date_str}: FAILED: {type(error).__name__}: {error}")
            else:
                print(
                    f"  {date_str}: {result['raw_records']} snapshots -> {result['rows']} rows; "
                    f"previous={result['previous_partition'] or 'none'}"
                )


if __name__ == "__main__":
    main()
