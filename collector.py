"""BKK GTFS-Realtime collector hot path.

This process only fetches and durably stores realtime observations. Remote
backup, static GTFS, compaction, pruning, and external monitoring run in the
separate maintenance process so they can never delay a poll.
"""

from __future__ import annotations

import hashlib
import logging
import logging.handlers
import re
import shutil
import signal
import sys
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import CollectorConfig, FEED_NAMES, feed_urls
from dedup import ChangeTracker, ChangeTrackerSignalError
from gtfs_rt_parse import PARSERS, parse_feed
from monitoring import HealthMonitor, append_poll_event, entity_timestamp_range, utc_iso
from parquet_store import DurableParquetSpool
from raw_log import append_record, repair_truncated_tail
from realtime_scheduler import IndependentFeedScheduler, ScheduledResult
from trip_update_policy import (
    TRIP_UPDATE_DELAY_FIELDS,
    TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
    TRIP_UPDATE_KEY_FIELDS,
    TRIP_UPDATE_PREDICTION_TIME_FIELDS,
    TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
)


LOGGER = logging.getLogger("bkk_collector")
_shutdown_requested = False


def configure_logging(data_dir: Path, process_name: str = "collector") -> logging.Logger:
    log_dir = data_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"bkk_{process_name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if logger.handlers:
        return logger
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(formatter)
    rotating = logging.handlers.RotatingFileHandler(
        log_dir / f"{process_name}.log", maxBytes=10_000_000, backupCount=5
    )
    rotating.setFormatter(formatter)
    logger.addHandler(stream)
    logger.addHandler(rotating)
    return logger


def _handle_signal(signum, _frame) -> None:
    global _shutdown_requested
    LOGGER.info("Received signal %s; finishing durable local writes before exit.", signum)
    _shutdown_requested = True


@dataclass(frozen=True)
class FetchResult:
    feed_name: str
    poll_id: str
    request_started_at: str
    request_started_ts: float
    response_received_at: str
    response_received_ts: float
    latency_ms: float
    http_status: int | None
    payload: bytes | None
    error: str | None


def _redact_error(error: BaseException, api_key: str) -> str:
    message = str(error).replace(api_key, "<redacted>") if api_key else str(error)
    message = re.sub(r"([?&]key=)[^&\s]+", r"\1<redacted>", message, flags=re.IGNORECASE)
    return f"{type(error).__name__}: {message}"[:1000]


def _new_session(config: CollectorConfig) -> requests.Session:
    retry = Retry(
        total=config.http_connect_retries,
        connect=config.http_connect_retries,
        read=config.http_connect_retries,
        status=0,
        redirect=0,
        other=0,
        backoff_factor=config.http_backoff_seconds,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=False,
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update({"User-Agent": "bkk-collector/2 (transit research archival)"})
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_connections=1, pool_maxsize=1))
    return session


class Collector:
    def __init__(self, config: CollectorConfig):
        self.config = config
        self.logger = configure_logging(config.data_dir, "collector")
        global LOGGER
        LOGGER = self.logger
        self.urls = feed_urls(config.api_key)
        self.sessions = {feed_name: _new_session(config) for feed_name in FEED_NAMES}
        self.executor = ThreadPoolExecutor(max_workers=len(FEED_NAMES), thread_name_prefix="bkk-fetch")
        self.spool = DurableParquetSpool(config.data_dir, config.parquet_flush_seconds)
        self.monitor = HealthMonitor(
            config.data_dir,
            stale_seconds=config.feed_stale_seconds,
            absent_seconds=config.feed_absent_seconds,
            frozen_seconds=config.frozen_payload_seconds,
            alerts_frozen_seconds=config.alerts_frozen_payload_seconds,
        )
        # None means this process has never successfully archived the feed.
        # A numeric zero is not a safe sentinel: immediately after host boot,
        # monotonic time can be below TripUpdates' five-minute raw interval.
        self.last_raw_archive_monotonic: dict[str, float | None] = {
            feed_name: None for feed_name in FEED_NAMES
        }
        self.raw_needs_repair: set[Path] = set()
        self.trackers = self._make_trackers()
        self.scheduler: IndependentFeedScheduler | None = None

    def _make_trackers(self) -> dict[str, ChangeTracker]:
        return {
            "tripupdates": ChangeTracker(
                key_fields=TRIP_UPDATE_KEY_FIELDS,
                value_fields=TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
                numeric_tolerance_fields=TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
                tolerance=self.config.delay_change_threshold_seconds,
                numeric_tolerances=self.config.trip_update_numeric_tolerances,
                required_signal_fields=TRIP_UPDATE_DELAY_FIELDS + TRIP_UPDATE_PREDICTION_TIME_FIELDS,
                null_guard_min_rows=self.config.change_tracker_null_guard_rows,
                heartbeat_seconds=self.config.heartbeat_seconds,
            ),
            "alerts": ChangeTracker(
                key_fields=(
                    "entity_id", "affected_agency_id", "affected_route_id", "affected_route_type",
                    "affected_stop_id", "affected_direction_id", "affected_trip_id",
                    "affected_trip_start_date", "affected_trip_start_time",
                ),
                value_fields=(
                    "cause", "effect", "severity_level", "header_text_json", "description_text_json",
                    "url_json", "active_periods_json", "communication_periods_json", "impact_periods_json",
                    "informed_entities_json", "bkk_start_text_json", "bkk_end_text_json",
                    "bkk_modified_time", "bkk_route_details_json",
                ),
                heartbeat_seconds=self.config.heartbeat_seconds,
            ),
        }

    def recover_recent_raw_tails(self) -> None:
        """Validate today's and the latest prior raw log for every feed.

        Limiting startup work to at most two existing logs per feed covers a
        crash across UTC midnight without repeatedly scanning months of data.
        Valid records stay in place; repair_truncated_tail moves only the
        invalid suffix to a forensic sidecar.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for feed_name in FEED_NAMES:
            feed_root = self.config.data_dir / "raw" / feed_name
            candidates: set[Path] = set()
            today_path = feed_root / f"date={today}" / f"{feed_name}.rawlog"
            if today_path.exists():
                candidates.add(today_path)
            prior: list[tuple[str, Path]] = []
            if feed_root.exists():
                for directory in feed_root.glob("date=*"):
                    date_str = directory.name.removeprefix("date=")
                    try:
                        datetime.strptime(date_str, "%Y-%m-%d")
                    except ValueError:
                        continue
                    path = directory / f"{feed_name}.rawlog"
                    if date_str < today and path.exists():
                        prior.append((date_str, path))
            if prior:
                candidates.add(max(prior)[1])
            for path in sorted(candidates):
                try:
                    recovery = repair_truncated_tail(path)
                except Exception:
                    self.raw_needs_repair.add(path)
                    self.logger.exception("CRITICAL: could not validate raw log tail for %s: %s", feed_name, path)
                else:
                    if recovery:
                        self.logger.error("Detached an invalid raw-log tail to %s before resuming appends", recovery)

    # Compatibility for callers of the v2.0 method name.
    recover_current_raw_tails = recover_recent_raw_tails

    def fetch_feed(self, feed_name: str, poll_id: str) -> FetchResult:
        started_ts = time.time()
        started = utc_iso(started_ts)
        monotonic_start = time.monotonic()
        response = None
        try:
            response = self.sessions[feed_name].get(
                self.urls[feed_name],
                timeout=(self.config.connect_timeout_seconds, self.config.read_timeout_seconds),
            )
            received_ts = time.time()
            received = utc_iso(received_ts)
            status = response.status_code
            response.raise_for_status()
            payload = response.content
            error = None
        except Exception as exc:
            received_ts = time.time()
            received = utc_iso(received_ts)
            status = response.status_code if response is not None else None
            payload = None
            error = _redact_error(exc, self.config.api_key)
        return FetchResult(
            feed_name=feed_name,
            poll_id=poll_id,
            request_started_at=started,
            request_started_ts=started_ts,
            response_received_at=received,
            response_received_ts=received_ts,
            latency_ms=(time.monotonic() - monotonic_start) * 1000,
            http_status=status,
            payload=payload,
            error=error,
        )

    def _raw_path(self, feed_name: str, date_str: str) -> Path:
        return self.config.data_dir / "raw" / feed_name / f"date={date_str}" / f"{feed_name}.rawlog"

    def _archive_raw(self, result: FetchResult, date_str: str, *, force: bool = False) -> tuple[bool, bool, str | None]:
        assert result.payload is not None
        interval = self.config.raw_archive_intervals[result.feed_name]
        now_monotonic = time.monotonic()
        last_archived = self.last_raw_archive_monotonic[result.feed_name]
        due = force or last_archived is None or now_monotonic - last_archived >= interval
        if not due:
            return False, True, None
        raw_path = self._raw_path(result.feed_name, date_str)
        try:
            for pending_path in list(self.raw_needs_repair):
                if pending_path.parent.parent.name != result.feed_name:
                    continue
                try:
                    recovery = repair_truncated_tail(pending_path)
                except Exception:
                    if pending_path == raw_path:
                        # Never append behind an unvalidated tail in the same
                        # file; that could hide valid future records behind a
                        # malformed length/header.
                        raise
                    # A damaged/permission-denied historical partition must
                    # remain visible for repair, but must not prevent today's
                    # independent raw log from accepting new observations.
                    self.logger.exception(
                        "CRITICAL: prior raw log still needs repair but current archival will continue: %s",
                        pending_path,
                    )
                    continue
                if recovery:
                    self.logger.error("Detached failed raw append tail to %s before retry", recovery)
                self.raw_needs_repair.discard(pending_path)
            append_record(raw_path, result.response_received_ts, result.payload, fsync=self.config.raw_fsync)
            self.last_raw_archive_monotonic[result.feed_name] = now_monotonic
            return True, True, str(raw_path.relative_to(self.config.data_dir))
        except Exception as error:
            self.raw_needs_repair.add(raw_path)
            self.logger.exception("CRITICAL: raw archive write failed for %s", result.feed_name)
            return False, False, _redact_error(error, self.config.api_key)

    def _schedule_event_fields(self, schedule: ScheduledResult | None) -> dict[str, Any]:
        if schedule is None:
            return {
                "scheduled_for_at": None,
                "scheduler_lag_ms": None,
                "missed_deadlines_before_request": 0,
            }
        return {
            "scheduled_for_at": schedule.scheduled_for_at,
            "scheduler_lag_ms": round(schedule.scheduler_lag_ms, 3),
            "missed_deadlines_before_request": schedule.missed_deadlines_before_request,
        }

    def _failure_event(self, result: FetchResult, schedule: ScheduledResult | None = None) -> dict[str, Any]:
        return {
            "version": 1,
            "poll_id": result.poll_id,
            "feed": result.feed_name,
            "poll_interval_seconds": self.config.feed_intervals[result.feed_name],
            **self._schedule_event_fields(schedule),
            "request_started_at": result.request_started_at,
            "response_received_at": result.response_received_at,
            "http_status": result.http_status,
            "latency_ms": round(result.latency_ms, 3),
            "success": False,
            "error": result.error,
            "payload_size": None,
            "payload_sha256": None,
            "raw_due": False,
            "raw_archived": False,
            "parse_ok": False,
            "parsed_rows": 0,
            "selected_rows": 0,
            "emitted_rows": 0,
        }

    def process_result(
        self,
        result: FetchResult,
        cycle_errors: list[str],
        schedule: ScheduledResult | None = None,
    ) -> None:
        date_str = datetime.fromtimestamp(result.response_received_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        if result.payload is None:
            self.logger.warning("Fetch failed for %s: %s", result.feed_name, result.error)
            self.monitor.record_failure(
                result.feed_name,
                now_ts=result.response_received_ts,
                error=result.error or "unknown fetch failure",
                http_status=result.http_status,
            )
            event = self._failure_event(result, schedule)
            try:
                append_poll_event(self.config.data_dir, result.feed_name, date_str, event)
            except Exception:
                cycle_errors.append(f"{result.feed_name}:poll_journal_failed")
                self.logger.exception("CRITICAL: failed to append poll failure journal for %s", result.feed_name)
            return

        payload_hash = hashlib.sha256(result.payload).hexdigest()
        content_hash = payload_hash
        raw_archived, raw_ok, raw_error = self._archive_raw(result, date_str)
        feed = None
        parsed_rows: list[dict[str, Any]] = []
        emitted_rows: list[dict[str, Any]] = []
        parse_ok = False
        change_tracking_ok = True
        spool_ok = True
        spool_path: str | None = None
        parse_error: str | None = None
        change_tracking_error: str | None = None
        change_tracking_failure_flag: str | None = None
        spool_error: str | None = None
        tracker = None
        try:
            feed = parse_feed(result.payload)
            content_digest = hashlib.sha256()
            for serialized_entity in sorted(entity.SerializeToString() for entity in feed.entity):
                content_digest.update(len(serialized_entity).to_bytes(8, "big"))
                content_digest.update(serialized_entity)
            content_hash = content_digest.hexdigest()
            context = {
                "poll_id": result.poll_id,
                "request_started_at": result.request_started_at,
                "response_received_at": result.response_received_at,
            }
            parsed_rows = PARSERS[result.feed_name](feed, context)
            parse_ok = True
        except Exception as error:
            parse_error = _redact_error(error, self.config.api_key)
            self.logger.exception("Failed parsing %s; preserving a raw fallback", result.feed_name)

        if parse_ok:
            tracker = self.trackers.get(result.feed_name)
            try:
                emitted_rows = (
                    tracker.filter(parsed_rows, date_str, result.response_received_ts, update=False)
                    if tracker
                    else parsed_rows
                )
            except Exception as error:
                change_tracking_ok = False
                change_tracking_error = _redact_error(error, self.config.api_key)
                change_tracking_failure_flag = (
                    "change_tracker_signal_missing"
                    if isinstance(error, ChangeTrackerSignalError)
                    else "change_tracker_failed"
                )
                self.logger.exception(
                    "CRITICAL: change tracking failed for %s; forcing raw fallback",
                    result.feed_name,
                )

        if parse_ok and change_tracking_ok:
            try:
                # The tracker advances only after the atomic spool segment is
                # durable. A failed stage is therefore eligible again later.
                # This block is separate from parsing so health evidence can
                # distinguish corrupt protobuf from storage failure.
                staged = self.spool.stage(result.feed_name, date_str, result.poll_id, emitted_rows)
                spool_path = str(staged.relative_to(self.config.data_dir)) if staged else None
                if tracker:
                    tracker.commit(emitted_rows, date_str, result.response_received_ts)
            except Exception as error:
                spool_ok = False
                spool_error = _redact_error(error, self.config.api_key)
                self.logger.exception("Failed staging %s; preserving a raw fallback", result.feed_name)

        if not parse_ok or not change_tracking_ok or not spool_ok:
            # Force a full snapshot when TripUpdates was between its normal raw
            # intervals, so any parser/spool loss remains reconstructible.
            if not raw_archived:
                fallback_written, fallback_ok, fallback_error = self._archive_raw(result, date_str, force=True)
                raw_archived = fallback_written
                raw_ok = fallback_ok
                raw_error = fallback_error

        header_timestamp = None
        min_entity_timestamp = None
        max_entity_timestamp = None
        entity_count = None
        if feed is not None:
            header_timestamp = feed.header.timestamp if feed.header.HasField("timestamp") else None
            min_entity_timestamp, max_entity_timestamp = entity_timestamp_range(result.feed_name, feed)
            entity_count = len(feed.entity)
        flags = self.monitor.record_success(
            result.feed_name,
            now_ts=result.response_received_ts,
            header_timestamp=header_timestamp,
            content_sha256=content_hash,
            min_entity_timestamp=min_entity_timestamp,
            max_entity_timestamp=max_entity_timestamp,
            parse_ok=parse_ok,
            change_tracking_ok=change_tracking_ok,
            change_tracking_failure_flag=change_tracking_failure_flag,
            raw_ok=raw_ok,
            spool_ok=spool_ok,
            entity_count=entity_count,
            request_started_at=result.request_started_at,
            response_received_at=result.response_received_at,
            http_status=result.http_status,
            latency_ms=result.latency_ms,
            payload_size=len(result.payload),
            payload_sha256=payload_hash,
        )
        event = {
            "version": 1,
            "poll_id": result.poll_id,
            "feed": result.feed_name,
            "poll_interval_seconds": self.config.feed_intervals[result.feed_name],
            **self._schedule_event_fields(schedule),
            "request_started_at": result.request_started_at,
            "response_received_at": result.response_received_at,
            "http_status": result.http_status,
            "latency_ms": round(result.latency_ms, 3),
            "success": True,
            "error": parse_error or change_tracking_error or spool_error,
            "parse_error": parse_error,
            "change_tracking_ok": change_tracking_ok,
            "change_tracking_error": change_tracking_error,
            "change_tracking_failure_flag": change_tracking_failure_flag,
            "spool_error": spool_error,
            "payload_size": len(result.payload),
            "payload_sha256": payload_hash,
            "entity_content_sha256": content_hash,
            "feed_header_timestamp": header_timestamp,
            "min_entity_timestamp": min_entity_timestamp,
            "max_entity_timestamp": max_entity_timestamp,
            "entity_count": entity_count,
            "freshness_flags": flags,
            "raw_due": raw_archived or not raw_ok,
            "raw_archive_interval_seconds": self.config.raw_archive_intervals[result.feed_name],
            "raw_archived": raw_archived,
            "raw_ok": raw_ok,
            "raw_path": str(self._raw_path(result.feed_name, date_str).relative_to(self.config.data_dir)) if raw_archived else None,
            "raw_error": raw_error,
            "parse_ok": parse_ok,
            "parsed_rows": len(parsed_rows),
            # selected_rows describes the change decision. emitted_rows is
            # intentionally narrower: only rows already durable in an atomic
            # spool segment count toward the Parquet completeness invariant.
            "selected_rows": len(emitted_rows) if parse_ok and change_tracking_ok else 0,
            "emitted_rows": len(emitted_rows) if parse_ok and change_tracking_ok and spool_ok else 0,
            "spool_ok": spool_ok,
            "spool_path": spool_path,
        }
        try:
            append_poll_event(self.config.data_dir, result.feed_name, date_str, event)
        except Exception:
            cycle_errors.append(f"{result.feed_name}:poll_journal_failed")
            self.logger.exception("CRITICAL: failed to append poll journal for %s", result.feed_name)

    def _commit_and_write_status(
        self,
        poll_id: str,
        cycle_errors: list[str],
        scheduler_snapshot: dict[str, dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Retry durable commits and atomically publish current collector health."""
        flush = self.spool.flush()
        for error in flush.errors:
            self.logger.error("Parquet flush failed; durable spool retained for retry: %s", error)
        if flush.files_written:
            self.logger.info("Committed %d rows to %d Parquet file(s)", flush.rows_written, len(flush.files_written))
        try:
            _total, _used, free = shutil.disk_usage(self.config.data_dir)
        except FileNotFoundError:
            self.config.data_dir.mkdir(parents=True, exist_ok=True)
            _total, _used, free = shutil.disk_usage(self.config.data_dir)
        try:
            status = self.monitor.write_status(
                now_ts=time.time(),
                poll_id=poll_id,
                disk_free_bytes=free,
                disk_warn_bytes=int(self.config.disk_warn_free_gb * 1_000_000_000),
                disk_critical_bytes=int(self.config.disk_critical_free_gb * 1_000_000_000),
                pending_spool_segments=len(self.spool.pending_segments()),
                parquet_flush_errors=flush.errors,
                cycle_errors=cycle_errors,
                scheduler=scheduler_snapshot,
            )
        except Exception:
            self.logger.exception("CRITICAL: failed to persist collector health status")
            status = {"healthy": False, "reasons": ["health_status_write_failed"]}
        if not status["healthy"]:
            self.logger.error("Collector health is degraded: %s", ", ".join(status["reasons"]))
        return status

    def poll_once(self) -> dict[str, Any]:
        """Synchronous one-shot helper retained for diagnostics/tests only."""
        poll_id = uuid.uuid4().hex
        cycle_errors: list[str] = []
        futures: dict[Future[FetchResult], str] = {
            self.executor.submit(self.fetch_feed, feed_name, poll_id): feed_name for feed_name in FEED_NAMES
        }
        for future in as_completed(futures):
            feed_name = futures[future]
            try:
                result = future.result()
            except Exception as error:
                # Defensive: fetch_feed normally converts all request failures.
                cycle_errors.append(f"{feed_name}:unexpected_fetch_worker_failure")
                self.logger.error("Unexpected fetch worker failure for %s: %s", feed_name, _redact_error(error, self.config.api_key))
                continue
            try:
                self.process_result(result, cycle_errors)
            except Exception as error:
                cycle_errors.append(f"{feed_name}:unexpected_processing_failure")
                self.logger.exception("Unexpected result-processing failure for %s: %s", feed_name, type(error).__name__)

        return self._commit_and_write_status(poll_id, cycle_errors)

    def run(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.recover_recent_raw_tails()
        recovered = self.spool.flush(force=True)
        if recovered.errors:
            self.logger.error("Startup spool recovery is pending: %s", "; ".join(recovered.errors))
        elif recovered.files_written:
            self.logger.info("Recovered %d staged rows from an earlier process", recovered.rows_written)
        self.logger.info(
            "Starting realtime-only collector. data=%s intervals=%s trip_raw=%.1fs",
            self.config.data_dir,
            self.config.feed_intervals,
            self.config.raw_archive_intervals["tripupdates"],
        )
        self.scheduler = IndependentFeedScheduler(
            self.fetch_feed,
            self.config.feed_intervals,
            executor=self.executor,
        )
        self.scheduler.start()
        cycle_errors: list[str] = []
        last_status_monotonic = float("-inf")
        while not _shutdown_requested:
            item = self.scheduler.get(timeout=1.0)
            if item is not None:
                try:
                    if item.worker_error is not None:
                        cycle_errors.append(f"{item.feed_name}:unexpected_fetch_worker_failure")
                        self.logger.error(
                            "Unexpected fetch worker failure for %s: %s",
                            item.feed_name,
                            _redact_error(item.worker_error, self.config.api_key),
                        )
                    else:
                        self.process_result(item.value, cycle_errors, item)
                except Exception as error:
                    cycle_errors.append(f"{item.feed_name}:unexpected_processing_failure")
                    self.logger.exception(
                        "Unexpected result-processing failure for %s: %s",
                        item.feed_name,
                        type(error).__name__,
                    )
                finally:
                    self.scheduler.acknowledge(item)

            now_monotonic = time.monotonic()
            if item is not None or now_monotonic - last_status_monotonic >= 5.0:
                poll_id = item.poll_id if item is not None else uuid.uuid4().hex
                self._commit_and_write_status(poll_id, cycle_errors, self.scheduler.snapshot(now_monotonic))
                cycle_errors = []
                last_status_monotonic = now_monotonic

        self.logger.info("Shutdown requested; stopping new requests and draining active requests.")
        for item in self.scheduler.stop_and_drain():
            if item.worker_error is not None:
                cycle_errors.append(f"{item.feed_name}:unexpected_fetch_worker_failure")
                self.logger.error(
                    "Unexpected fetch worker failure during shutdown for %s: %s",
                    item.feed_name,
                    _redact_error(item.worker_error, self.config.api_key),
                )
                continue
            try:
                self.process_result(item.value, cycle_errors, item)
            except Exception:
                cycle_errors.append(f"{item.feed_name}:unexpected_processing_failure")
                self.logger.exception("Unexpected result-processing failure during shutdown for %s", item.feed_name)
        self.logger.info("Committing all durable spool segments.")
        result = self.spool.flush(force=True)
        if result.errors:
            self.logger.error("Parquet remains pending in durable spool: %s", "; ".join(result.errors))
        for session in self.sessions.values():
            session.close()
        self.logger.info("Clean shutdown complete; no in-memory-only derived rows remain.")


def main() -> None:
    config = CollectorConfig.from_env()
    collector = Collector(config)
    collector.run()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)
    main()
