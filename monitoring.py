"""Persistent feed freshness and collector health state."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import realcity
from atomic_io import append_jsonl, atomic_write_json, read_json
from config import FEED_NAMES


def utc_iso(timestamp: float | None = None) -> str:
    value = datetime.now(timezone.utc) if timestamp is None else datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return value.isoformat()


def parse_iso_timestamp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def evaluate_freshness(
    *,
    now_ts: float,
    header_timestamp: int | None,
    header_unchanged_since: float,
    payload_unchanged_since: float,
    stale_seconds: float,
    frozen_seconds: float,
) -> list[str]:
    flags: list[str] = []
    if header_timestamp is not None and math.isfinite(float(header_timestamp)):
        source_age = now_ts - float(header_timestamp)
        if source_age > stale_seconds:
            flags.append("source_timestamp_stale")
    if now_ts - payload_unchanged_since > frozen_seconds:
        flags.append("payload_frozen")
    if now_ts - header_unchanged_since > frozen_seconds:
        flags.append("source_timestamp_frozen")
    return flags


def entity_timestamp_range(feed_name: str, feed: Any) -> tuple[int | None, int | None]:
    values: list[int] = []
    for entity in feed.entity:
        message = None
        if feed_name == "vehiclepositions" and entity.HasField("vehicle"):
            message = entity.vehicle
        elif feed_name == "tripupdates" and entity.HasField("trip_update"):
            message = entity.trip_update
        elif feed_name == "alerts" and entity.HasField("alert"):
            try:
                if entity.alert.HasExtension(realcity.alert):
                    modified = entity.alert.Extensions[realcity.alert]
                    if modified.HasField("modifiedTime"):
                        values.append(int(modified.modifiedTime))
            except (KeyError, TypeError, ValueError):
                pass
        if message is not None and "timestamp" in message.DESCRIPTOR.fields_by_name and message.HasField("timestamp"):
            values.append(int(message.timestamp))
    return (min(values), max(values)) if values else (None, None)


class HealthMonitor:
    def __init__(
        self,
        data_dir: Path,
        *,
        stale_seconds: float,
        absent_seconds: float,
        frozen_seconds: float,
        alerts_frozen_seconds: float,
    ):
        self.state_path = data_dir / "health" / "freshness_state.json"
        self.status_path = data_dir / "health" / "status.json"
        self.stale_seconds = stale_seconds
        self.absent_seconds = absent_seconds
        self.frozen_seconds = frozen_seconds
        self.alerts_frozen_seconds = alerts_frozen_seconds
        loaded = read_json(self.state_path, {})
        self.feeds: dict[str, dict[str, Any]] = loaded.get("feeds", {}) if isinstance(loaded, dict) else {}
        for feed_name in FEED_NAMES:
            self.feeds.setdefault(feed_name, {})

    def record_success(
        self,
        feed_name: str,
        *,
        now_ts: float,
        header_timestamp: int | None,
        content_sha256: str,
        min_entity_timestamp: int | None,
        max_entity_timestamp: int | None,
        parse_ok: bool,
        change_tracking_ok: bool = True,
        change_tracking_failure_flag: str | None = None,
        raw_ok: bool,
        spool_ok: bool,
        entity_count: int | None = None,
        request_started_at: str | None = None,
        response_received_at: str | None = None,
        http_status: int | None = 200,
        latency_ms: float | None = None,
        payload_size: int | None = None,
        payload_sha256: str | None = None,
    ) -> list[str]:
        state = self.feeds[feed_name]
        previous_content_hash = state.get("content_sha256", state.get("payload_sha256"))
        if previous_content_hash != content_sha256:
            state["payload_unchanged_since"] = now_ts
        if state.get("header_timestamp") != header_timestamp:
            state["header_unchanged_since"] = now_ts
        state.setdefault("payload_unchanged_since", now_ts)
        state.setdefault("header_unchanged_since", now_ts)
        frozen = self.alerts_frozen_seconds if feed_name == "alerts" else self.frozen_seconds
        flags = evaluate_freshness(
            now_ts=now_ts,
            header_timestamp=header_timestamp,
            header_unchanged_since=float(state["header_unchanged_since"]),
            payload_unchanged_since=float(state["payload_unchanged_since"]),
            stale_seconds=self.stale_seconds,
            frozen_seconds=frozen,
        )
        # Alerts are event-driven: an unchanged source timestamp is advisory.
        # HTTP absence, parse and storage failures remain liveness failures.
        if feed_name == "alerts":
            header_stale = any(flag in flags for flag in
                               ("source_timestamp_stale", "source_timestamp_frozen"))
            flags = [flag for flag in flags if flag not in
                     ("source_timestamp_stale", "source_timestamp_frozen")]
            if header_stale:
                flags.append("source_timestamp_unchanged_warning")
        if feed_name == "alerts" and "payload_frozen" in flags:
            flags = [flag for flag in flags if flag != "payload_frozen"]
            if entity_count:
                # Planned alerts can legitimately remain unchanged. Preserve
                # the signal for quality review without failing liveness.
                flags.append("payload_unchanged_warning")
        if not parse_ok:
            flags.append("protobuf_parse_failed")
        if not change_tracking_ok:
            flags.append(change_tracking_failure_flag or "change_tracker_failed")
        if (
            feed_name != "alerts"
            and max_entity_timestamp is not None
            and now_ts - max_entity_timestamp > self.stale_seconds
        ):
            flags.append("entity_timestamp_stale")
        if not raw_ok:
            flags.append("raw_archive_failed")
        if not spool_ok:
            flags.append("derived_spool_failed")
        state.update(
            {
                "last_success_at": utc_iso(now_ts),
                "last_success_timestamp": now_ts,
                "last_attempt_at": response_received_at or utc_iso(now_ts),
                "request_started_at": request_started_at,
                "response_received_at": response_received_at or utc_iso(now_ts),
                "last_http_status": http_status,
                "last_latency_ms": latency_ms,
                "last_payload_size": payload_size,
                "last_payload_sha256": payload_sha256,
                "header_timestamp": header_timestamp,
                "content_sha256": content_sha256,
                "min_entity_timestamp": min_entity_timestamp,
                "max_entity_timestamp": max_entity_timestamp,
                "entity_count": entity_count,
                "consecutive_failures": 0,
                "parse_ok": parse_ok,
                "change_tracking_ok": change_tracking_ok,
                "change_tracking_failure_flag": change_tracking_failure_flag,
                "raw_ok": raw_ok,
                "spool_ok": spool_ok,
                "freshness_flags": sorted(set(flags)),
            }
        )
        self._save_state(now_ts)
        return sorted(set(flags))

    def record_failure(self, feed_name: str, *, now_ts: float, error: str, http_status: int | None) -> None:
        state = self.feeds[feed_name]
        state["last_attempt_at"] = utc_iso(now_ts)
        state["last_error"] = error
        state["last_http_status"] = http_status
        state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
        self._save_state(now_ts)

    def _save_state(self, now_ts: float) -> None:
        atomic_write_json(self.state_path, {"version": 1, "updated_at": utc_iso(now_ts), "feeds": self.feeds})

    def write_status(
        self,
        *,
        now_ts: float,
        poll_id: str,
        disk_free_bytes: int,
        disk_warn_bytes: int,
        disk_critical_bytes: int,
        pending_spool_segments: int,
        parquet_flush_errors: list[str],
        cycle_errors: list[str],
        scheduler: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        reasons: list[str] = []
        warnings: list[str] = []
        feed_status: dict[str, Any] = {}
        for feed_name in FEED_NAMES:
            state = dict(self.feeds.get(feed_name, {}))
            schedule = dict((scheduler or {}).get(feed_name, {}))
            last_success = state.get("last_success_timestamp")
            absence = None if last_success is None else max(0.0, now_ts - float(last_success))
            state["seconds_since_success"] = absence
            interval = float(schedule.get("interval_seconds") or 0)
            absence_limit = max(self.absent_seconds, interval * 3)
            state["absence_threshold_seconds"] = absence_limit
            if last_success is None or absence is None or absence > absence_limit:
                reasons.append(f"{feed_name}:absent")
            if int(state.get("consecutive_failures", 0)):
                reasons.append(f"{feed_name}:http_failed")
            for flag in state.get("freshness_flags", []):
                target = warnings if flag.endswith("_warning") else reasons
                target.append(f"{feed_name}:{flag}")
            if schedule:
                in_flight_seconds = schedule.get("in_flight_seconds")
                if in_flight_seconds is not None and interval and float(in_flight_seconds) > interval:
                    warnings.append(f"{feed_name}:request_exceeds_cadence")
                if (
                    in_flight_seconds is not None
                    and interval
                    and float(in_flight_seconds) > max(interval * 3, 60.0)
                ):
                    reasons.append(f"{feed_name}:request_stuck")
                missed = int(schedule.get("missed_since_last_request") or 0)
                if missed:
                    warnings.append(f"{feed_name}:scheduler_missed_deadline")
                next_due = schedule.get("next_deadline_in_seconds")
                if next_due is not None and interval and float(next_due) < -interval:
                    reasons.append(f"{feed_name}:scheduler_overdue")
                state["scheduler"] = schedule
            feed_status[feed_name] = state
        if disk_free_bytes < disk_critical_bytes:
            reasons.append("disk:critical")
        elif disk_free_bytes < disk_warn_bytes:
            reasons.append("disk:low")
        if parquet_flush_errors:
            reasons.append("parquet_flush_failed")
        if cycle_errors:
            reasons.append("cycle_storage_error")
        status = {
            "version": 1,
            "updated_at": utc_iso(now_ts),
            "updated_timestamp": now_ts,
            "poll_id": poll_id,
            "healthy": not reasons,
            "reasons": sorted(set(reasons)),
            "warnings": sorted(set(warnings)),
            "disk_free_bytes": disk_free_bytes,
            "disk_warn_bytes": disk_warn_bytes,
            "disk_critical_bytes": disk_critical_bytes,
            "pending_spool_segments": pending_spool_segments,
            "parquet_flush_errors": parquet_flush_errors,
            "cycle_errors": cycle_errors,
            "scheduler": scheduler or {},
            "feeds": feed_status,
        }
        atomic_write_json(self.status_path, status)
        return status


def poll_journal_path(data_dir: Path, feed_name: str, date_str: str) -> Path:
    return data_dir / "metadata" / "polls" / feed_name / f"date={date_str}" / "polls.jsonl"


def append_poll_event(data_dir: Path, feed_name: str, date_str: str, event: dict[str, Any]) -> None:
    append_jsonl(poll_journal_path(data_dir, feed_name, date_str), event, fsync=True)


def iter_jsonl(path: Path):
    with open(path, "r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            try:
                value = json.loads(line)
            except ValueError:
                yield line_number, None
            else:
                yield line_number, value
