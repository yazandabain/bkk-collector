"""Evidence-backed daily collection manifests and completeness checks."""

from __future__ import annotations

import os
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from atomic_io import atomic_write_json, sha256_file
from config import DEFAULT_FEED_INTERVALS, FEED_NAMES, MIN_REALTIME_INTERVAL_SECONDS
from monitoring import data_poll_success, iter_jsonl, parse_iso_timestamp, poll_journal_path, utc_iso
from parquet_store import ensure_empty_parquet, parquet_row_count
from raw_log import scan_raw_log
from static_gtfs import StaticGtfsStore
from tripupdate_presence import apply_presence_record, apply_source_timestamp_record, iter_presence, presence_path


def _artifact(data_dir: Path, path: Path, kind: str) -> dict[str, Any]:
    if path.is_symlink():
        raise ValueError(f"refusing symlinked artifact: {path}")
    return {
        "path": str(path.relative_to(data_dir)),
        "kind": kind,
        "size": path.stat().st_size,
        "mtime_ns": path.stat().st_mtime_ns,
        "sha256": sha256_file(path),
    }


def _observation_gaps(events: list[dict], date: str, interval: float) -> dict:
    start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    times = sorted(timestamp for event in events
                   if (timestamp := parse_iso_timestamp(event.get("response_received_at"))) is not None
                   and start <= timestamp < start + 86400)
    gaps = [(previous, current) for previous, current in zip(times, times[1:])
            if current - previous > interval * 1.5]
    return {
        "successful_observation_gap_count": len(gaps),
        "max_successful_observation_gap_seconds": max((b - a for a, b in zip(times, times[1:])), default=None),
        "start_boundary_without_success_seconds": times[0] - start if times else 86400,
        "end_boundary_without_success_seconds": start + 86400 - times[-1] if times else 86400,
        "gap_samples": [{"previous_response_at": utc_iso(a), "next_response_at": utc_iso(b),
                         "seconds": round(b - a, 6)} for a, b in gaps[:20]],
        "gap_interpretation": "response intervals, not inferred exact lost-poll counts; HTTP/parse/storage failures and scheduler misses are separate",
    }


def _presence_stats(data_dir: Path, date: str, events: list[dict]) -> tuple[dict, list[str], list[str]]:
    promised = {event["poll_id"]: event for event in events
                if event.get("presence_required") and event.get("presence_ok") is True}
    expected = sum(bool(event.get("presence_required") and event.get("parse_ok")) for event in events)
    path = presence_path(data_dir, date)
    errors, quality = [], []
    count, baselines = 0, 0
    seen: dict[str, tuple[str, int]] = {}
    members = {}
    source_timestamps = None
    source_timestamp_observations = 0
    stream = None
    sequence = 0
    if path.exists():
        try:
            if not scan_raw_log(path).clean:
                raise ValueError("presence log has an invalid/truncated tail")
            for _timestamp, record in iter_presence(path):
                if record["kind"] == "baseline":
                    if record["sequence"] != 1:
                        raise ValueError("presence baseline sequence is invalid")
                    stream, sequence = record["stream_id"], 0
                    baselines += 1
                if record["stream_id"] != stream or record["sequence"] != sequence + 1:
                    raise ValueError("presence stream has a sequence gap")
                members = apply_presence_record(members, record)
                source_timestamps = apply_source_timestamp_record(source_timestamps, record, members)
                if source_timestamps is not None:
                    source_timestamp_observations += 1
                promised_event = promised.get(record["poll_id"], {})
                if (promised_event.get("presence_source_timestamps_version") is not None
                        and promised_event["presence_source_timestamps_version"] != record.get("source_timestamps_version")):
                    raise ValueError("journal-promised source timestamp evidence is missing/mismatched")
                sequence = record["sequence"]
                poll_id = record["poll_id"]
                if poll_id in seen:
                    raise ValueError("duplicate presence poll ID")
                seen[poll_id] = (stream, sequence)
                count += 1
        except Exception as error:
            errors.append(f"tripupdates_presence: {type(error).__name__}: {error}")
    if any(seen.get(poll_id) != (event.get("presence_stream_id"), event.get("presence_sequence"))
           for poll_id, event in promised.items()):
        errors.append("tripupdates_presence: journal-promised membership evidence is missing/mismatched")
    unavailable = expected - len(promised)
    if unavailable:
        quality.append(f"tripupdates_presence: {unavailable} parsed poll(s) lacked confirmed membership evidence; consult forced raw fallback")
    if list(path.parent.glob("presence.jsonlog.corrupt-tail-*")):
        quality.append("tripupdates_presence: recovered tail; membership observations may be missing")
    legacy_polls = sum(bool(event.get("parse_ok") and not event.get("presence_required")) for event in events)
    if expected and legacy_polls:
        quality.append(f"tripupdates_presence: mixed-version date; {legacy_polls} legacy parsed poll(s) have no membership evidence")
    return {"expected_observations": expected, "journal_confirmed_observations": len(promised),
            "records": count, "baselines": baselines,
            "legacy_pre_presence_polls": legacy_polls,
            "source_timestamp_observations": source_timestamp_observations,
            "unavailable_observations": unavailable}, errors, quality


def _feed_stats(data_dir: Path, feed_name: str, date_str: str, *, create_empty: bool) -> tuple[dict[str, Any], list[str], list[str]]:
    errors: list[str] = []
    quality_flags: list[str] = []
    journal = poll_journal_path(data_dir, feed_name, date_str)
    events: list[dict[str, Any]] = []
    corrupt_journal_lines = 0
    if journal.exists():
        for _line, event in iter_jsonl(journal):
            if event is None:
                corrupt_journal_lines += 1
            else:
                events.append(event)
    else:
        errors.append(f"{feed_name}: missing poll journal")

    attempted = len(events)
    responses = [event for event in events if event.get("success")]
    parse_successes = [event for event in responses if event.get("parse_ok")]
    raw_events = [event for event in responses if event.get("raw_archived")]
    emitted_rows = sum(int(event.get("emitted_rows") or 0) for event in parse_successes)
    selected_rows = sum(int(event.get("selected_rows", event.get("emitted_rows")) or 0) for event in parse_successes)
    parsed_rows = sum(int(event.get("parsed_rows") or 0) for event in parse_successes)
    if attempted == 0:
        errors.append(f"{feed_name}: no recorded poll attempts")
    if not responses:
        errors.append(f"{feed_name}: no successful HTTP responses")
    if responses and not parse_successes:
        errors.append(f"{feed_name}: no successfully parsed responses")
    if not raw_events:
        errors.append(f"{feed_name}: no archived raw snapshots")
    if corrupt_journal_lines:
        errors.append(f"{feed_name}: {corrupt_journal_lines} corrupt poll journal line(s)")

    observed_intervals: list[float] = []
    for event in events:
        try:
            interval = float(event.get("poll_interval_seconds"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(interval) and interval >= MIN_REALTIME_INTERVAL_SECONDS:
            observed_intervals.append(interval)
    observed_intervals.sort()
    # Journals are per-feed. Use that feed's observed median cadence (robust to
    # a deployment changing cadence part-way through a day), never another
    # feed's or a global cycle interval.
    expected_interval = (
        observed_intervals[len(observed_intervals) // 2]
        if observed_intervals
        else DEFAULT_FEED_INTERVALS[feed_name]
    )
    expected_polls = max(1, round(86400 / expected_interval))
    # One edge poll can legitimately straddle a UTC boundary. Never hide
    # sustained losses merely because more than 90% of attempts survived.
    if attempted < expected_polls - 1:
        quality_flags.append(f"{feed_name}: only {attempted}/{expected_polls} expected polls recorded")
    scheduler_misses = sum(int(event.get("missed_deadlines_before_request") or 0) for event in events)
    if scheduler_misses:
        quality_flags.append(f"{feed_name}: {scheduler_misses} scheduler deadlines missed")
    if len(set(observed_intervals)) > 1:
        quality_flags.append(f"{feed_name}: cadence changed; daily expected count uses the observed median")
    recovered_tails = list(journal.parent.glob("polls.jsonl.corrupt-tail-*"))
    if recovered_tails:
        quality_flags.append(f"{feed_name}: recovered poll journal tail; one or more events may be missing")
    failed_responses = attempted - len(responses)
    if failed_responses:
        quality_flags.append(f"{feed_name}: {failed_responses} failed poll(s)")
    parse_failures = len(responses) - len(parse_successes)
    if parse_failures:
        quality_flags.append(f"{feed_name}: {parse_failures} parse failure(s)")
    data_successes = [event for event in responses if data_poll_success(event)]
    if len(data_successes) < len(parse_successes):
        quality_flags.append(f"{feed_name}: {len(parse_successes) - len(data_successes)} parsed poll(s) had storage/tracking/evidence failures")
    gap_stats = _observation_gaps(data_successes, date_str, expected_interval)
    failed_processing_with_raw = sum(bool(event.get("raw_archived") and event.get("raw_ok", True)
                                          and not data_poll_success(event)) for event in responses)
    stale_incidents = sum(1 for event in responses if any(
        not flag.endswith("_warning") for flag in event.get("freshness_flags", [])
    ))
    freshness_warning_polls = sum(1 for event in responses if any(
        flag.endswith("_warning") for flag in event.get("freshness_flags", [])
    ))
    if stale_incidents:
        quality_flags.append(f"{feed_name}: {stale_incidents} freshness incident poll(s)")

    raw_interval = next(
        (float(event.get("raw_archive_interval_seconds")) for event in responses if event.get("raw_archive_interval_seconds") is not None),
        expected_interval,
    )
    effective_raw_interval = expected_interval if raw_interval <= 0 else max(expected_interval, raw_interval)
    expected_raw_snapshots = max(1, round(86400 / effective_raw_interval))

    raw_dir = data_dir / "raw" / feed_name / f"date={date_str}"
    raw_path = raw_dir / f"{feed_name}.rawlog"
    raw_records = 0
    raw_clean = False
    if raw_path.exists() and raw_path.stat().st_size:
        try:
            raw_scan = scan_raw_log(raw_path)
            raw_records = raw_scan.complete_records
            raw_clean = raw_scan.clean
            if not raw_scan.clean:
                errors.append(f"{feed_name}: raw log has an invalid/truncated tail")
            if raw_records < len(raw_events):
                errors.append(f"{feed_name}: raw record count {raw_records} is below journal count {len(raw_events)}")
        except Exception as error:
            errors.append(f"{feed_name}: raw log scan failed: {type(error).__name__}: {error}")
    else:
        errors.append(f"{feed_name}: raw log missing or empty")
    if raw_records < expected_raw_snapshots * 0.9:
        quality_flags.append(
            f"{feed_name}: only {raw_records}/{expected_raw_snapshots} expected raw snapshots present"
        )

    parquet_dir = data_dir / "parquet" / feed_name / f"date={date_str}"
    parquet_files = sorted(parquet_dir.glob("*.parquet")) if parquet_dir.exists() else []
    if not parquet_files and parse_successes and emitted_rows == 0 and create_empty:
        try:
            parquet_files = [ensure_empty_parquet(data_dir, feed_name, date_str)]
        except Exception as error:
            errors.append(f"{feed_name}: failed creating empty Parquet marker: {type(error).__name__}: {error}")
    parquet_rows = 0
    for path in parquet_files:
        try:
            parquet_rows += parquet_row_count(path)
        except Exception as error:
            errors.append(f"{feed_name}: unreadable Parquet {path.name}: {type(error).__name__}: {error}")
    if not parquet_files:
        errors.append(f"{feed_name}: no Parquet artifact")
    elif parquet_rows < emitted_rows:
        errors.append(f"{feed_name}: Parquet has {parquet_rows} rows but journal emitted {emitted_rows}")

    pending_spool = list((data_dir / "spool" / feed_name / f"date={date_str}").glob("batch-*.json.gz"))
    if pending_spool:
        errors.append(f"{feed_name}: {len(pending_spool)} uncommitted spool segment(s)")

    timestamps = [event.get("response_received_at") for event in responses if event.get("response_received_at")]
    stats = {
        "configured_poll_interval_seconds": expected_interval,
        "expected_polls": expected_polls,
        "attempted_polls": attempted,
        "successful_http_polls": len(responses),
        "failed_http_polls": failed_responses,
        "successful_parse_polls": len(parse_successes),
        "parse_failures": parse_failures,
        "successful_data_polls": len(data_successes),
        "failed_processing_polls_with_raw_evidence": failed_processing_with_raw,
        "journalled_poll_count_ratio": attempted / expected_polls,
        "successful_data_poll_count_ratio": len(data_successes) / expected_polls,
        "poll_count_shortfall": max(0, expected_polls - attempted),
        **gap_stats,
        "first_success_at": min(timestamps) if timestamps else None,
        "last_success_at": max(timestamps) if timestamps else None,
        "raw_snapshots": raw_records,
        "expected_raw_snapshots": expected_raw_snapshots,
        "journal_raw_snapshots": len(raw_events),
        "raw_log_clean": raw_clean,
        "parsed_rows_seen": parsed_rows,
        "event_rows_selected": selected_rows,
        "event_rows_emitted": emitted_rows,
        "parquet_rows": parquet_rows,
        "parquet_files": len(parquet_files),
        "stale_feed_polls": stale_incidents,
        "freshness_warning_polls": freshness_warning_polls,
        "corrupt_journal_lines": corrupt_journal_lines,
        "recovered_journal_tail_files": len(recovered_tails),
        "collector_run_ids": sorted({event["run_id"] for event in events if event.get("run_id")}),
        "pending_spool_segments": len(pending_spool),
        "scheduler_missed_deadlines": scheduler_misses,
    }
    if feed_name == "tripupdates":
        presence, presence_errors, presence_quality = _presence_stats(data_dir, date_str, events)
        stats["presence"] = presence
        errors.extend(presence_errors)
        quality_flags.extend(presence_quality)
    return stats, errors, quality_flags


def build_daily_manifest(
    data_dir: Path,
    date_str: str,
    static_store: StaticGtfsStore,
    *,
    create_empty_parquet: bool = True,
) -> dict[str, Any]:
    # Validate date and prohibit a current/future day from being finalized.
    date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if date.date() >= datetime.now(timezone.utc).date():
        raise ValueError("daily manifests can only finalize completed UTC dates")

    errors: list[str] = []
    quality_flags: list[str] = []
    feeds: dict[str, Any] = {}
    for feed_name in FEED_NAMES:
        stats, feed_errors, feed_quality = _feed_stats(
            data_dir, feed_name, date_str, create_empty=create_empty_parquet
        )
        feeds[feed_name] = stats
        errors.extend(feed_errors)
        quality_flags.extend(feed_quality)

    applicable_static = static_store.applicable_version(date_str)
    day_start = date.timestamp()
    static_timeline = static_store.observed_timeline(day_start, day_start + 86400)
    if applicable_static is None:
        errors.append("static_gtfs: no version history applicable to this date")
    else:
        static_path = static_store.archive_path(applicable_static)
        if not static_path.exists():
            errors.append(f"static_gtfs: applicable archive missing: {applicable_static['version_path']}")
        elif sha256_file(static_path) != applicable_static.get("sha256"):
            errors.append("static_gtfs: applicable archive checksum mismatch")
    if not static_timeline:
        quality_flags.append("static_gtfs: no collector-observed version covers this date")
    else:
        if static_timeline[0]["effective_from"] != utc_iso(day_start):
            quality_flags.append("static_gtfs: observed version history does not cover the start of this date")
        if any(item["applicability_confidence"] == "legacy_schedule_uncertain" for item in static_timeline):
            quality_flags.append("static_gtfs: legacy schedule applicability is uncertain")

    artifacts: list[dict[str, Any]] = []
    for feed_name in FEED_NAMES:
        for path in sorted((data_dir / "raw" / feed_name / f"date={date_str}").glob("*")):
            if path.is_file():
                artifacts.append(_artifact(data_dir, path, "raw"))
        for path in sorted((data_dir / "parquet" / feed_name / f"date={date_str}").glob("*.parquet")):
            artifacts.append(_artifact(data_dir, path, "parquet"))
        journal = poll_journal_path(data_dir, feed_name, date_str)
        for path in sorted(journal.parent.glob(journal.name + "*")):
            if path.is_file():
                artifacts.append(_artifact(data_dir, path, "poll_metadata"))
    for path in sorted(presence_path(data_dir, date_str).parent.glob("*")):
        if path.is_file():
            artifacts.append(_artifact(data_dir, path, "tripupdates_presence"))
    run_ids = {run_id for stats in feeds.values() for run_id in stats["collector_run_ids"]}
    for run_id in sorted(run_ids):
        if not re.fullmatch(r"[0-9a-f]{32}", run_id):
            errors.append("collector_runs: invalid run ID in poll journal")
            continue
        path = data_dir / "metadata" / "collector_runs" / f"{run_id}.json"
        if not path.is_file():
            errors.append(f"collector_runs: missing run provenance {run_id}")
        else:
            artifacts.append(_artifact(data_dir, path, "collector_run_metadata"))
    if applicable_static is not None:
        # Include every distinct static version known locally, not just today's
        # applicable one. This also brings preserved v1-era static archives into
        # the independently verified off-server backup without copying them.
        static_events = [applicable_static, *static_store.history()]
        seen_static_hashes: set[str] = set()
        for event in static_events:
            digest = event.get("sha256")
            version_path = event.get("version_path")
            if not digest or not version_path or digest in seen_static_hashes:
                continue
            try:
                static_path = static_store.archive_path(event)
            except ValueError as error:
                errors.append(f"static_gtfs: {error}")
                continue
            if static_path.exists():
                artifact = _artifact(data_dir, static_path, "static_gtfs")
                if artifact["sha256"] != digest:
                    errors.append(f"static_gtfs: archive checksum mismatch: {version_path}")
                    continue
                artifacts.append(artifact)
                seen_static_hashes.add(digest)
        history = data_dir / "static_gtfs" / "history.jsonl"
        if history.exists():
            artifacts.append(_artifact(data_dir, history, "static_gtfs_history"))
        static_state = data_dir / "static_gtfs" / "state.json"
        if static_state.exists():
            artifacts.append(_artifact(data_dir, static_state, "static_gtfs_state"))

    manifest = {
        "manifest_version": 1,
        "date": date_str,
        "generated_at": utc_iso(),
        "collector_schema_version": 2,
        "collector_version": os.environ.get("COLLECTOR_VERSION", "2"),
        "collector_git_commit": os.environ.get("COLLECTOR_GIT_COMMIT", "unknown"),
        "complete": not errors,
        "completeness_errors": errors,
        "quality_ok": not quality_flags,
        "quality_flags": quality_flags,
        "feeds": feeds,
        "static_gtfs": applicable_static,
        "static_gtfs_timeline": static_timeline,
        "artifacts": artifacts,
    }
    path = data_dir / "metadata" / "manifests" / f"date={date_str}.json"
    atomic_write_json(path, manifest)
    return manifest


def discover_completed_dates(data_dir: Path) -> list[str]:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    found: set[str] = set()
    for root in (data_dir / "raw", data_dir / "parquet", data_dir / "metadata" / "polls"):
        if not root.exists():
            continue
        for date_dir in root.glob("*/date=*"):
            date_str = date_dir.name.removeprefix("date=")
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue
            if date_str < today:
                found.add(date_str)
    return sorted(found)
