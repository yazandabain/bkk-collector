"""
Change-and-heartbeat filtering for high-churn GTFS-RT feeds.

BKK retransmits the FULL feed on every poll -- there's no "give me only what
changed" option. For TripUpdates that means ~5MB of mostly-repeated stop
predictions every 30 seconds, which is not something you can store forever
at full resolution (measured: see README).

Worse, BKK recalculates ETAs continuously off live GPS, so a delay value
typically wobbles by a second or two on nearly every poll even when nothing
meaningful has happened. Exact-equality comparison treats that as a change
and defeats the whole point.

A ChangeTracker keeps the last-WRITTEN value for each row's identity key and
only lets a row through when:
  - it's the first time this key has been seen this day, or
  - a tracked field changed (see comparison rules below), or
  - more than `heartbeat_seconds` have passed since this key was last written
    (so a genuinely static value still gets reconfirmed periodically).

Comparison rules:
  - Fields in `value_fields` are compared for exact inequality. Right for
    categorical things: stop ids, alert cause/effect, text.
  - Fields in `numeric_tolerance_fields` are compared with a tolerance:
    the row is only "changed" if the value moved by >= `tolerance` from the
    value last written for that key. Because the comparison is against the
    last written value rather than a fixed grid, a value hovering anywhere
    can wobble freely without triggering writes -- there are no bucket
    boundaries to flap across. (An earlier version used floor-division
    buckets and failed exactly this way: a delay oscillating 43<->47 crossed
    the 45 boundary and wrote on most polls.)
  - None is handled explicitly: None -> number and number -> None both count
    as changes; None -> None does not.

Used identically by collector.py (live) and rebuild_parquet.py (offline), so
a rebuild applies the same filtering the live run did.
"""

from __future__ import annotations

from typing import Optional


class ChangeTracker:
    def __init__(
        self,
        key_fields: tuple,
        value_fields: tuple = (),
        heartbeat_seconds: int = 1800,
        numeric_tolerance_fields: tuple = (),
        tolerance: int = 0,
    ):
        self.key_fields = key_fields
        self.value_fields = value_fields
        self.numeric_tolerance_fields = numeric_tolerance_fields
        self.tolerance = tolerance
        self.heartbeat_seconds = heartbeat_seconds
        # key -> (exact_values_tuple, numeric_values_tuple, last_written_unix_ts)
        self._last: dict = {}
        self._day: Optional[str] = None

    def _maybe_reset(self, date_str: str) -> None:
        # Bounds memory and matches the daily partitioning used everywhere else.
        if self._day != date_str:
            self._last = {}
            self._day = date_str

    def _numeric_changed(self, prev_vals: tuple, new_vals: tuple) -> bool:
        for prev, new in zip(prev_vals, new_vals):
            if prev is None and new is None:
                continue
            if prev is None or new is None:
                return True
            if abs(new - prev) >= self.tolerance:
                return True
        return False

    def filter(self, rows: list, date_str: str, now_ts: float) -> list:
        self._maybe_reset(date_str)
        out = []
        for row in rows:
            key = tuple(row.get(f) for f in self.key_fields)
            exact_vals = tuple(row.get(f) for f in self.value_fields)
            numeric_vals = tuple(row.get(f) for f in self.numeric_tolerance_fields)

            prev = self._last.get(key)
            if prev is None:
                write = True
            else:
                prev_exact, prev_numeric, prev_ts = prev
                write = (
                    prev_exact != exact_vals
                    or self._numeric_changed(prev_numeric, numeric_vals)
                    or (now_ts - prev_ts) >= self.heartbeat_seconds
                )

            if write:
                out.append(row)
                self._last[key] = (exact_vals, numeric_vals, now_ts)
        return out
