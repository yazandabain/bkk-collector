"""Evidence-backed daily collection manifests and completeness checks."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from atomic_io import atomic_write_json, sha256_file
from config import FEED_NAMES
from monitoring import iter_jsonl, poll_journal_path, utc_iso
from parquet_store import ensure_empty_parquet, parquet_row_count
from raw_log import scan_raw_log
from static_gtfs import StaticGtfsStore


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

    expected_interval = next((float(event.get("poll_interval_seconds")) for event in events if event.get("poll_interval_seconds")), 30.0)
    expected_polls = max(1, round(86400 / expected_interval))
    if attempted < expected_polls * 0.9:
        quality_flags.append(f"{feed_name}: only {attempted}/{expected_polls} expected polls recorded")
    failed_responses = attempted - len(responses)
    if failed_responses:
        quality_flags.append(f"{feed_name}: {failed_responses} failed poll(s)")
    parse_failures = len(responses) - len(parse_successes)
    if parse_failures:
        quality_flags.append(f"{feed_name}: {parse_failures} parse failure(s)")
    stale_incidents = sum(1 for event in responses if event.get("freshness_flags"))
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
        "expected_polls": expected_polls,
        "attempted_polls": attempted,
        "successful_http_polls": len(responses),
        "failed_http_polls": failed_responses,
        "successful_parse_polls": len(parse_successes),
        "parse_failures": parse_failures,
        "first_success_at": min(timestamps) if timestamps else None,
        "last_success_at": max(timestamps) if timestamps else None,
        "raw_snapshots": raw_records,
        "expected_raw_snapshots": expected_raw_snapshots,
        "journal_raw_snapshots": len(raw_events),
        "raw_log_clean": raw_clean,
        "parsed_rows_seen": parsed_rows,
        "event_rows_emitted": emitted_rows,
        "parquet_rows": parquet_rows,
        "parquet_files": len(parquet_files),
        "stale_feed_polls": stale_incidents,
        "corrupt_journal_lines": corrupt_journal_lines,
        "pending_spool_segments": len(pending_spool),
    }
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
    if applicable_static is None:
        errors.append("static_gtfs: no version history applicable to this date")
    else:
        static_path = data_dir / "static_gtfs" / applicable_static["version_path"]
        if not static_path.exists():
            errors.append(f"static_gtfs: applicable archive missing: {applicable_static['version_path']}")
        elif sha256_file(static_path) != applicable_static.get("sha256"):
            errors.append("static_gtfs: applicable archive checksum mismatch")

    artifacts: list[dict[str, Any]] = []
    for feed_name in FEED_NAMES:
        for path in sorted((data_dir / "raw" / feed_name / f"date={date_str}").glob("*")):
            if path.is_file():
                artifacts.append(_artifact(data_dir, path, "raw"))
        for path in sorted((data_dir / "parquet" / feed_name / f"date={date_str}").glob("*.parquet")):
            artifacts.append(_artifact(data_dir, path, "parquet"))
        journal = poll_journal_path(data_dir, feed_name, date_str)
        if journal.exists():
            artifacts.append(_artifact(data_dir, journal, "poll_metadata"))
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
            static_path = data_dir / "static_gtfs" / version_path
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
