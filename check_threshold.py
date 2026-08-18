"""
Measures the real distribution of delay changes between consecutive polls,
straight from the archived raw logs, and reports what fraction of rows each
candidate threshold would suppress. Use this to pick
DELAY_CHANGE_THRESHOLD_SECONDS on evidence rather than by guessing.

Usage (inside the container):
    python3 check_threshold.py
    python3 check_threshold.py /data/raw/tripupdates/date=2026-08-19/tripupdates.rawlog
"""
import sys
import collections
from datetime import datetime, timezone

from raw_log import iter_records
from gtfs_rt_parse import parse_feed, parse_trip_updates

DEFAULT = '/data/raw/tripupdates/date=2026-08-18/tripupdates.rawlog'


def main(path: str) -> None:
    last, diffs, n_polls = {}, [], 0
    with open(path, 'rb') as f:
        for ts, raw in iter_records(f):
            n_polls += 1
            try:
                rows = parse_trip_updates(
                    parse_feed(raw),
                    datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                )
            except Exception:
                continue
            for r in rows:
                key = (r.get('trip_id'), r.get('start_date'), r.get('stop_id'))
                v = r.get('arrival_delay')
                if v is None:
                    continue
                if key in last:
                    diffs.append(abs(v - last[key]))
                last[key] = v

    diffs.sort()
    n = len(diffs)
    print(f"polls analysed: {n_polls}")
    print(f"consecutive-observation pairs: {n}")
    if n == 0:
        print("no usable pairs found")
        return

    print("\nabsolute change in arrival_delay between consecutive polls:")
    for p in (50, 75, 90, 95, 99):
        print(f"  p{p}: {diffs[min(int(n * p / 100), n - 1)]}s")
    print(f"  max: {diffs[-1]}s")

    print("\nfraction of observations SUPPRESSED at each candidate threshold:")
    for thr in (0, 5, 10, 15, 20, 30, 60):
        s = sum(1 for d in diffs if d < thr)
        print(f"  threshold {thr:3d}s -> suppresses {s / n * 100:5.1f}%  (writes {100 - s / n * 100:5.1f}%)")

    c = collections.Counter(d for d in diffs if d <= 20)
    print("\ndistribution of small changes (0-20s):")
    for d in sorted(c):
        print(f"  {d:3d}s: {c[d]:8d}  ({c[d] / n * 100:5.2f}%)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else DEFAULT)
