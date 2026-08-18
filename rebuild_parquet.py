"""
Re-derives Parquet from the raw archived protobuf logs.

This is the only kind of "backfill" a realtime feed permits: you can't get
data for a moment you didn't poll, but you CAN reparse a moment you did poll
with better/fixed code. Run this whenever you improve gtfs_rt_parse.py and
want history to reflect the fix, or if a Parquet file gets corrupted/lost.

Usage:
    python rebuild_parquet.py                     # rebuild everything
    python rebuild_parquet.py --date 2026-09-01    # rebuild one date
    python rebuild_parquet.py --feed tripupdates   # rebuild one feed only

Note on TripUpdates and Alerts: the raw archive itself is already throttled
(TripUpdates every TRIPUPDATES_RAW_ARCHIVE_SECONDS, default 120s -- see
collector.py) because BKK retransmits the full ~5MB feed every poll. A
rebuild can only reconstruct as much temporal resolution as the raw archive
actually captured -- it cannot recover 30-second granularity for a feed that
was only archived every 2 minutes. The same change/heartbeat filter the live
collector applies is applied here too, for consistency between a live run
and a rebuilt one.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from gtfs_rt_parse import PARSERS, parse_feed
from raw_log import iter_records
from dedup import ChangeTracker

DATA_DIR = Path(__import__("os").environ.get("DATA_DIR", "/data"))
RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"

HEARTBEAT_SECONDS = int(__import__("os").environ.get("HEARTBEAT_SECONDS", "1800"))
DELAY_CHANGE_THRESHOLD_SECONDS = int(__import__("os").environ.get("DELAY_CHANGE_THRESHOLD_SECONDS", "15"))


def _make_trackers() -> dict:
    return {
        "tripupdates": ChangeTracker(
            key_fields=("trip_id", "start_date", "stop_id"),
            numeric_tolerance_fields=("arrival_delay", "departure_delay", "trip_delay"),
            tolerance=DELAY_CHANGE_THRESHOLD_SECONDS,
            heartbeat_seconds=HEARTBEAT_SECONDS,
        ),
        "alerts": ChangeTracker(
            key_fields=("entity_id", "affected_route_id", "affected_stop_id", "affected_trip_id"),
            value_fields=("cause", "effect", "header_text", "description_text", "active_period_start", "active_period_end"),
            heartbeat_seconds=HEARTBEAT_SECONDS,
        ),
    }


def rebuild_one(feed_name: str, date_str: str, trackers: dict) -> None:
    raw_path = RAW_DIR / feed_name / f"date={date_str}" / f"{feed_name}.rawlog"
    if not raw_path.exists():
        print(f"  no raw log for {feed_name} on {date_str}, skipping")
        return

    tracker = trackers.get(feed_name)
    rows: list[dict] = []
    n_records, n_failed = 0, 0
    with open(raw_path, "rb") as f:
        for ts, raw_bytes in iter_records(f):
            n_records += 1
            fetched_at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
            try:
                feed = parse_feed(raw_bytes)
                parsed_rows = PARSERS[feed_name](feed, fetched_at)
                if tracker is not None:
                    parsed_rows = tracker.filter(parsed_rows, date_str, ts)
                rows.extend(parsed_rows)
            except Exception as e:
                n_failed += 1
                print(f"  parse failed for one record at {fetched_at}: {e}")

    out_dir = PARQUET_DIR / feed_name / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    # Wipe any previous parts for this date before rewriting, so a rebuild
    # doesn't leave stale files mixed in with the fresh ones.
    for old in out_dir.glob("part-*.parquet"):
        old.unlink()

    if rows:
        df = pd.DataFrame(rows)
        out_path = out_dir / "part-rebuilt.parquet"
        df.to_parquet(out_path, index=False, compression="zstd")
        print(f"  {feed_name} {date_str}: {n_records} polls -> {len(df)} rows ({n_failed} parse failures) -> {out_path}")
    else:
        print(f"  {feed_name} {date_str}: {n_records} polls, 0 usable rows ({n_failed} parse failures)")


def all_dates_for(feed_name: str) -> list[str]:
    feed_dir = RAW_DIR / feed_name
    if not feed_dir.exists():
        return []
    return sorted(p.name.replace("date=", "") for p in feed_dir.glob("date=*"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="YYYY-MM-DD, default: all dates found")
    ap.add_argument("--feed", choices=list(PARSERS), help="default: all three feeds")
    args = ap.parse_args()

    feeds = [args.feed] if args.feed else list(PARSERS)
    for feed_name in feeds:
        trackers = _make_trackers()  # fresh per feed: dedup state must not leak across feeds
        dates = [args.date] if args.date else all_dates_for(feed_name)
        print(f"{feed_name}: rebuilding {len(dates)} date(s)")
        for d in dates:
            rebuild_one(feed_name, d, trackers)


if __name__ == "__main__":
    main()
