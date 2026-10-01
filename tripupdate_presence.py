"""Durable FULL_DATASET membership evidence, independent of ETA compression.

Records use the existing timestamp/gzip framing, but their payload is JSON,
not GTFS protobuf. Every valid observation is recorded, including empty deltas.
Trip identities group stop-visit identities to avoid repeating trip descriptors.
Withdrawal means only 'absent from this observation', never cancellation.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from raw_log import append_record, iter_records, repair_truncated_tail
from trip_update_policy import TRIP_UPDATE_KEY_FIELDS


PRESENCE_VERSION = 1
TRIP_IDENTITY_FIELDS = TRIP_UPDATE_KEY_FIELDS[:4]
STOP_IDENTITY_FIELDS = TRIP_UPDATE_KEY_FIELDS[4:]


def presence_path(data_dir: Path, date: str) -> Path:
    return data_dir / "metadata" / "tripupdates_presence" / f"date={date}" / "presence.jsonlog"


def iter_presence(path: Path):
    with path.open("rb") as handle:
        for timestamp, payload in iter_records(handle):
            yield timestamp, json.loads(payload)


def apply_presence_record(members: dict[tuple, set[tuple]], record: dict) -> dict[tuple, set[tuple]]:
    """Replay a validated baseline/delta. Baselines reset knowledge, not trips."""
    if (record.get("version") != PRESENCE_VERSION
            or record.get("trip_identity_fields") != list(TRIP_IDENTITY_FIELDS)
            or record.get("stop_identity_fields") != list(STOP_IDENTITY_FIELDS)
            or record.get("kind") not in ("baseline", "delta")):
        raise ValueError("unsupported presence record")
    if record["kind"] == "baseline":
        members = {}
    changed = set()
    for change in record["changes"]:
        trip = tuple(change["trip"])
        if len(trip) != len(TRIP_IDENTITY_FIELDS) or trip in changed:
            raise ValueError("invalid/duplicate trip presence identity")
        changed.add(trip)
        entered = {tuple(stop) for stop in change["entered_stops"]}
        withdrawn = {tuple(stop) for stop in change["withdrawn_stops"]}
        if any(len(stop) != len(STOP_IDENTITY_FIELDS) for stop in entered | withdrawn):
            raise ValueError("invalid stop presence identity")
        old = members.get(trip)
        if change["trip_entered"]:
            if old is not None:
                raise ValueError("trip enters twice without withdrawal")
            old = set()
        if old is None or entered & old or not withdrawn <= old:
            raise ValueError("presence delta does not match preceding membership")
        new = (old - withdrawn) | entered
        if change["trip_withdrawn"]:
            if new:
                raise ValueError("withdrawn trip retains stop visits")
            del members[trip]
        else:
            members[trip] = new
    if len(members) != record["trip_count"] or sum(map(len, members.values())) != record["stop_count"]:
        raise ValueError("presence membership counts do not match replay")
    return members


def _ordered(values):
    return sorted(values, key=lambda value: json.dumps(value, separators=(",", ":")))


class TripUpdatePresence:
    """Owned exclusively by ingestion. Advance only after fsynced persistence."""

    def __init__(self, data_dir: Path, run_id: str):
        self.data_dir = data_dir
        self.run_id = run_id
        self._date: str | None = None
        self._members: dict[tuple, set[tuple]] | None = None
        self._baseline_reason = "process_start"
        self._stream_id = uuid.uuid4().hex
        self._sequence = 0

    def invalidate(self, reason: str) -> None:
        self._members = None
        self._baseline_reason = reason

    def observe(self, rows: list[dict], date: str, timestamp: float, context: dict) -> dict:
        if context.get("feed_incrementality", 0) != 0:
            self.invalidate("unsupported_incrementality")
            raise ValueError("presence requires FULL_DATASET; raw fallback preserves unsupported feeds")
        if self._date != date:
            if self._date is not None:
                self._baseline_reason = "utc_date_boundary"
            self._date = date
            self._members = None
        current: dict[tuple, set[tuple]] = {}
        for row in rows:
            if row.get("entity_is_deleted"):
                self.invalidate("unexpected_deletion")
                raise ValueError("FULL_DATASET deletion cannot be interpreted as presence")
            trip = tuple(row.get(field) for field in TRIP_IDENTITY_FIELDS)
            stops = current.setdefault(trip, set())
            # Trip-only/canceled entities remain present, with no invented stop.
            if row.get("stop_time_update_index") is not None:
                stops.add(tuple(row.get(field) for field in STOP_IDENTITY_FIELDS))
        baseline = self._members is None
        previous = self._members or {}
        changes = []
        for trip in _ordered(current.keys() | previous.keys()):
            old_stops = previous.get(trip, set())
            new_stops = current.get(trip, set())
            entered = new_stops - old_stops
            withdrawn = old_stops - new_stops
            trip_entered = trip not in previous
            trip_withdrawn = trip not in current
            if entered or withdrawn or trip_entered or trip_withdrawn:
                changes.append({
                    "trip": trip, "trip_entered": trip_entered, "trip_withdrawn": trip_withdrawn,
                    "entered_stops": _ordered(entered), "withdrawn_stops": _ordered(withdrawn),
                })
        sequence = 1 if baseline else self._sequence + 1
        stream_id = uuid.uuid4().hex if baseline else self._stream_id
        record = {
            **context, "version": PRESENCE_VERSION, "run_id": self.run_id,
            "stream_id": stream_id, "sequence": sequence,
            "kind": "baseline" if baseline else "delta",
            "baseline_reason": self._baseline_reason if baseline else None,
            "trip_identity_fields": TRIP_IDENTITY_FIELDS,
            "stop_identity_fields": STOP_IDENTITY_FIELDS,
            "trip_count": len(current), "stop_count": sum(map(len, current.values())),
            "changes": changes,
        }
        path = presence_path(self.data_dir, date)
        try:
            # Checkpoints make the usual validation constant-time. A failed
            # append is repaired before retry; forensic bytes stay alongside it.
            repair_truncated_tail(path)
            append_record(path, timestamp, json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode(), fsync=True)
        except Exception:
            self.invalidate("presence_write_gap")
            raise
        self._members = current
        self._stream_id = stream_id
        self._sequence = sequence
        return {"presence_path": str(path.relative_to(self.data_dir)),
                "presence_kind": record["kind"], "presence_stream_id": stream_id,
                "presence_sequence": sequence, "present_trips": record["trip_count"],
                "present_stop_visits": record["stop_count"]}
