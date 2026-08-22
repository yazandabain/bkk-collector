"""Analyze TripUpdates delay changes at the raw log's actual cadence.

The production default raw log is five-minute data. It cannot validate a
threshold used on 30-second observations. For an apples-to-apples experiment,
temporarily set TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS=30, collect a representative
window, and use --require-high-frequency.
"""

from __future__ import annotations

import argparse
import collections
import statistics
import sys
from datetime import datetime, timezone

from gtfs_rt_parse import parse_feed, parse_trip_updates
from raw_log import iter_records


def analyze(path: str, expected_interval: float) -> tuple[list[int], list[float], int]:
    last: dict[tuple, int] = {}
    diffs: list[int] = []
    timestamps: list[float] = []
    polls = 0
    with open(path, "rb") as handle:
        for timestamp, raw in iter_records(handle):
            polls += 1
            timestamps.append(timestamp)
            try:
                received = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                rows = parse_trip_updates(parse_feed(raw), received)
            except Exception:
                continue
            for row in rows:
                key = (
                    row.get("entity_id"), row.get("trip_id"), row.get("start_date"),
                    row.get("start_time"), row.get("stop_sequence"),
                    row.get("stop_visit_fallback_index"), row.get("stop_id"),
                )
                value = row.get("arrival_delay")
                if value is None:
                    continue
                if key in last:
                    diffs.append(abs(value - last[key]))
                last[key] = value
    intervals = [later - earlier for earlier, later in zip(timestamps, timestamps[1:]) if later >= earlier]
    return diffs, intervals, polls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="TripUpdates .rawlog to analyze")
    parser.add_argument("--expected-interval", type=float, default=30.0, help="production observation cadence in seconds")
    parser.add_argument("--require-high-frequency", action="store_true", help="fail if raw cadence is not comparable")
    args = parser.parse_args(argv)
    diffs, intervals, polls = analyze(args.path, args.expected_interval)
    median_interval = statistics.median(intervals) if intervals else float("inf")
    comparable = median_interval <= args.expected_interval * 1.5
    print(f"polls analysed: {polls}")
    print(f"median raw-snapshot interval: {median_interval:.1f}s")
    print(f"target live observation interval: {args.expected_interval:.1f}s")
    if not comparable:
        print(
            "WARNING: this raw log is too sparse for an apples-to-apples validation of the live threshold.\n"
            "Five-minute changes combine ten 30-second transitions and cannot estimate how many live rows a threshold suppresses.\n"
            "Temporarily set TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS to the target cadence and collect a representative sample."
        )
        if args.require_high_frequency:
            return 2

    diffs.sort()
    count = len(diffs)
    print(f"consecutive-observation pairs: {count}")
    if not count:
        return 1
    print("\nabsolute arrival_delay change at the observed cadence:")
    for percentile in (50, 75, 90, 95, 99):
        print(f"  p{percentile}: {diffs[min(int(count * percentile / 100), count - 1)]}s")
    print(f"  max: {diffs[-1]}s")
    print("\nfraction suppressed at the observed cadence (not the target cadence unless comparable):")
    for threshold in (0, 5, 10, 15, 20, 30, 60):
        suppressed = sum(1 for change in diffs if change < threshold)
        print(f"  threshold {threshold:3d}s -> suppresses {suppressed / count * 100:5.1f}%")
    small = collections.Counter(change for change in diffs if change <= 20)
    print("\ndistribution of small changes (0-20s):")
    for change in sorted(small):
        print(f"  {change:3d}s: {small[change]:8d} ({small[change] / count * 100:5.2f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
