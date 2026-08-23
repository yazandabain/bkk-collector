"""Analyze one TripUpdates prediction field at the raw log's actual cadence.

The production default raw log is five-minute data. It cannot validate a
threshold used on 10-second observations. For an apples-to-apples experiment,
temporarily set TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS=10, collect a representative
window, and use --require-high-frequency.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from datetime import datetime, timezone

from gtfs_rt_parse import parse_feed, parse_trip_updates
from quality_diagnostics import RevisionDistribution
from raw_log import iter_records
from trip_update_policy import TRIP_UPDATE_KEY_FIELDS


SUPPORTED_FIELDS = ("arrival_time", "departure_time", "arrival_delay", "departure_delay", "trip_delay")


def analyze(path: str, field: str) -> tuple[RevisionDistribution, list[float], int]:
    last: dict[tuple, int] = {}
    distribution = RevisionDistribution()
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
                key = tuple(row.get(field) for field in TRIP_UPDATE_KEY_FIELDS)
                value = row.get(field)
                if value is None:
                    continue
                if key in last:
                    distribution.add(value - last[key])
                last[key] = value
    intervals = [later - earlier for earlier, later in zip(timestamps, timestamps[1:]) if later >= earlier]
    return distribution, intervals, polls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", help="TripUpdates .rawlog to analyze")
    parser.add_argument("--expected-interval", type=float, default=10.0, help="production observation cadence in seconds")
    parser.add_argument("--require-high-frequency", action="store_true", help="fail if raw cadence is not comparable")
    parser.add_argument(
        "--field",
        choices=SUPPORTED_FIELDS,
        default="arrival_time",
        help="prediction field to analyze (BKK currently populates absolute times, not delays)",
    )
    args = parser.parse_args(argv)
    distribution, intervals, polls = analyze(args.path, args.field)
    median_interval = statistics.median(intervals) if intervals else float("inf")
    comparable = median_interval <= args.expected_interval * 1.5
    print(f"polls analysed: {polls}")
    print(f"median raw-snapshot interval: {median_interval:.1f}s")
    print(f"target live observation interval: {args.expected_interval:.1f}s")
    if not comparable:
        print(
            "WARNING: this raw log is too sparse for an apples-to-apples validation of the live threshold.\n"
            "Five-minute changes combine thirty 10-second transitions and cannot estimate how many live rows a threshold suppresses.\n"
            "Temporarily set TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS to the target cadence and collect a representative sample."
        )
        if args.require_high_frequency:
            return 2

    report = distribution.report()
    count = distribution.count
    print(f"consecutive-observation pairs: {count}")
    if not count:
        return 1
    print(f"\nabsolute {args.field} change at the observed cadence:")
    for percentile in (50, 75, 90, 95, 99):
        print(f"  p{percentile}: {report[f'p{percentile}_seconds']}s")
    print(f"  max: {report['max_seconds']}s")
    print("\nfraction suppressed at the observed cadence (not the target cadence unless comparable):")
    for threshold in (0, 5, 10, 15, 20, 30, 60):
        suppressed = sum(
            observations
            for change, observations in distribution.histogram.items()
            if change <= threshold
        )
        print(f"  threshold {threshold:3d}s -> suppresses {suppressed / count * 100:5.1f}%")
    print("\ndistribution of small changes (0-20s):")
    for change in sorted(value for value in distribution.histogram if value <= 20):
        observations = distribution.histogram[change]
        print(f"  {change:3d}s: {observations:8d} ({observations / count * 100:5.2f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
