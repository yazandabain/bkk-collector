"""
Change-and-heartbeat filtering for high-churn GTFS-RT feeds.

BKK retransmits the FULL feed on every poll -- there's no "give me only what
changed" option. For TripUpdates that means ~5MB of mostly-repeated stop
predictions every 10 seconds, which is not something you can store forever
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
    the row is only "changed" if the value moved by more than `tolerance` from the
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

import math
from typing import Optional


class ChangeTrackerSignalError(RuntimeError):
    """The upstream rows contain none of a tracker's required signal fields."""


class ChangeTracker:
    def __init__(
        self,
        key_fields: tuple,
        value_fields: tuple = (),
        heartbeat_seconds: int = 1800,
        numeric_tolerance_fields: tuple = (),
        tolerance: int = 0,
        numeric_tolerances: dict[str, float] | None = None,
        required_signal_fields: tuple = (),
        null_guard_min_rows: int = 1000,
    ):
        self.key_fields = key_fields
        self.value_fields = value_fields
        self.numeric_tolerance_fields = numeric_tolerance_fields
        self.tolerance = tolerance
        self.numeric_tolerances = numeric_tolerances or {}
        unknown_tolerances = set(self.numeric_tolerances) - set(self.numeric_tolerance_fields)
        if unknown_tolerances:
            raise ValueError(f"numeric tolerances configured for untracked fields: {sorted(unknown_tolerances)}")
        tolerances = [self.tolerance, *self.numeric_tolerances.values()]
        if any(not math.isfinite(float(value)) or value < 0 for value in tolerances):
            raise ValueError("numeric tolerances must be finite and not negative")
        tracked_fields = set(self.value_fields) | set(self.numeric_tolerance_fields)
        if not tracked_fields:
            raise ValueError("ChangeTracker must configure at least one mutable tracked field")
        self.required_signal_fields = required_signal_fields or tuple(sorted(tracked_fields))
        unknown_signals = set(self.required_signal_fields) - tracked_fields
        if unknown_signals:
            raise ValueError(f"required signals are not tracked fields: {sorted(unknown_signals)}")
        if null_guard_min_rows <= 0:
            raise ValueError("null_guard_min_rows must be greater than zero")
        self.null_guard_min_rows = null_guard_min_rows
        self._consecutive_all_null_rows = 0
        self.heartbeat_seconds = heartbeat_seconds
        # key -> (exact_values_tuple, numeric_values_tuple, last_written_unix_ts)
        self._last: dict = {}
        self._day: Optional[str] = None
        self._last_expiry = float("-inf")

    def _maybe_reset(self, date_str: str) -> None:
        # Bounds memory and matches the daily partitioning used everywhere else.
        if self._day != date_str:
            self._last = {}
            self._day = date_str
            self._last_expiry = float("-inf")

    def _expire_heartbeat_entries(self, now_ts: float) -> None:
        # An entry older than its heartbeat MUST emit on its next observation,
        # regardless of values. Forgetting it has exactly that same outcome.
        # Retaining a whole day's departed trips caused production OOM kills.
        if now_ts - self._last_expiry >= min(60, self.heartbeat_seconds):
            expired = [key for key, value in self._last.items()
                       if now_ts - value[2] >= self.heartbeat_seconds]
            for key in expired:
                del self._last[key]
            self._last_expiry = now_ts

    def _numeric_changed(self, prev_vals: tuple, new_vals: tuple) -> bool:
        for field, prev, new in zip(self.numeric_tolerance_fields, prev_vals, new_vals):
            if prev is None and new is None:
                continue
            if prev is None or new is None:
                return True
            tolerance = self.numeric_tolerances.get(field, self.tolerance)
            if tolerance <= 0:
                if new != prev:
                    return True
            elif abs(new - prev) > tolerance:
                return True
        return False

    def filter(self, rows: list, date_str: str, now_ts: float, *, update: bool = True) -> list:
        """Select changed rows.

        ``update=False`` supports a durable two-phase flow: select rows, write
        them to the on-disk spool, then call :meth:`commit`.  A failed spool
        write therefore cannot advance dedup state and silently suppress the
        retry on the next poll.
        """
        self._maybe_reset(date_str)
        self._expire_heartbeat_entries(now_ts)
        if rows:
            if any(
                row.get(field) is not None
                for row in rows
                for field in self.required_signal_fields
            ):
                self._consecutive_all_null_rows = 0
            else:
                self._consecutive_all_null_rows += len(rows)
                if self._consecutive_all_null_rows >= self.null_guard_min_rows:
                    raise ChangeTrackerSignalError(
                        "all required change-tracking signals are null across "
                        f"{self._consecutive_all_null_rows} consecutive rows: "
                        f"{', '.join(self.required_signal_fields)}"
                    )
        out = []
        # Only this poll's changes need an overlay. Copying the entire daily
        # dictionary on every poll amplified both memory and scheduler latency.
        prospective = {}
        for row in rows:
            key = tuple(row.get(f) for f in self.key_fields)
            exact_vals = tuple(row.get(f) for f in self.value_fields)
            numeric_vals = tuple(row.get(f) for f in self.numeric_tolerance_fields)

            prev = prospective.get(key, self._last.get(key))
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
                prospective[key] = (exact_vals, numeric_vals, now_ts)
        if update:
            self._last.update(prospective)
        return out

    def commit(self, rows: list, date_str: str, now_ts: float) -> None:
        self._maybe_reset(date_str)
        for row in rows:
            key = tuple(row.get(f) for f in self.key_fields)
            exact_vals = tuple(row.get(f) for f in self.value_fields)
            numeric_vals = tuple(row.get(f) for f in self.numeric_tolerance_fields)
            self._last[key] = (exact_vals, numeric_vals, now_ts)
