from __future__ import annotations

import hashlib
import io
import tempfile
import time
import unittest
import zipfile
from concurrent.futures import Future
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from google.transit import gtfs_realtime_pb2 as pb

import realcity
from atomic_io import append_jsonl, read_json, sha256_file
from backup import BackupManager
from collector import Collector, FetchResult
from config import CollectorConfig, DEFAULT_FEED_INTERVALS, FEED_NAMES, MaintenanceConfig
from dedup import ChangeTracker, ChangeTrackerSignalError
from diagnostics import summarize_live_sample
from gtfs_rt_parse import parse_trip_updates, parse_vehicle_positions
from manifests import _feed_stats
from maintenance import MaintenanceWorker
from migrate_legacy import LegacyMigrator
from parquet_store import PARQUET_COLUMNS, write_parquet_atomic
from quality_diagnostics import prediction_revision_report, tripupdate_static_join_report
from raw_log import append_record, scan_raw_log
from realtime_scheduler import IndependentFeedScheduler
from static_gtfs import StaticGtfsStore
from trip_update_policy import (
    TRIP_UPDATE_COLLECTION_METADATA_FIELDS,
    TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
    TRIP_UPDATE_IMMUTABLE_OR_REDUNDANT_FIELDS,
    TRIP_UPDATE_KEY_FIELDS,
    TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
    classified_trip_update_fields,
)


def diagnostic_gtfs_zip() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "agency.txt",
            "agency_id,agency_name,agency_url,agency_timezone\nBKK,BKK,https://bkk.hu,Europe/Budapest\n",
        )
        archive.writestr("routes.txt", "route_id,route_short_name,route_type\nbus-route,5,3\n")
        archive.writestr("trips.txt", "route_id,service_id,trip_id\nbus-route,daily,bus-trip\n")
        archive.writestr(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\nbus-stop,Stop,47.5,19.0\n",
        )
        archive.writestr(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "bus-trip,08:00:00,08:00:00,bus-stop,8\n",
        )
        archive.writestr(
            "feed_info.txt",
            "feed_publisher_name,feed_publisher_url,feed_lang,feed_version\n"
            "BKK,https://bkk.hu,hu,test\n",
        )
    return output.getvalue()


class MemoryRemoteApi:
    def __init__(self):
        self.remote: dict[str, tuple[int, str]] = {}
        self.uploaded: list[str] = []

    def create_repo(self, **_kwargs):
        return None

    def upload_file(self, *, path_or_fileobj, path_in_repo, **_kwargs):
        path = Path(path_or_fileobj)
        self.remote[path_in_repo] = (path.stat().st_size, sha256_file(path))
        self.uploaded.append(path_in_repo)

    def get_paths_info(self, *, paths, **_kwargs):
        return [
            {"path": path, "size": self.remote[path][0], "lfs": {"sha256": self.remote[path][1]}}
            for path in paths
            if path in self.remote
        ]


class ControlledExecutor:
    def __init__(self):
        self.submissions: list[tuple[str, str, Future]] = []

    def submit(self, _function, feed_name, poll_id):
        future = Future()
        self.submissions.append((feed_name, poll_id, future))
        return future

    def count(self, feed_name: str) -> int:
        return sum(name == feed_name for name, _poll, _future in self.submissions)

    def latest(self, feed_name: str) -> Future:
        return next(future for name, _poll, future in reversed(self.submissions) if name == feed_name)

    def shutdown(self, **_kwargs):
        return None


def scheduler(intervals=None):
    executor = ControlledExecutor()
    value = IndependentFeedScheduler(
        lambda feed, poll: (feed, poll),
        intervals or dict(DEFAULT_FEED_INTERVALS),
        executor=executor,
        start_monotonic=0.0,
        monotonic=lambda: 0.0,
        wall_time=lambda: 1_800_000_000.0,
    )
    return value, executor


def finish_and_ack(value: IndependentFeedScheduler, executor: ControlledExecutor, feed_name: str, now: float):
    executor.latest(feed_name).set_result(feed_name)
    value.step(now)
    item = value.get(0)
    assert item is not None
    value.acknowledge(item)
    return item


class SchedulerTests(unittest.TestCase):
    def test_each_feed_runs_at_its_configured_cadence(self):
        value, executor = scheduler({"vehiclepositions": 5, "tripupdates": 10, "alerts": 30})
        value.step(0)
        for feed in FEED_NAMES:
            finish_and_ack(value, executor, feed, 0.1)
        value.step(5)
        self.assertEqual(2, executor.count("vehiclepositions"))
        self.assertEqual(1, executor.count("tripupdates"))
        self.assertEqual(1, executor.count("alerts"))
        finish_and_ack(value, executor, "vehiclepositions", 5.1)
        value.step(10)
        self.assertEqual(3, executor.count("vehiclepositions"))
        self.assertEqual(2, executor.count("tripupdates"))
        self.assertEqual(1, executor.count("alerts"))
        finish_and_ack(value, executor, "vehiclepositions", 10.1)
        finish_and_ack(value, executor, "tripupdates", 10.1)
        value.step(30)
        self.assertEqual(2, executor.count("alerts"))

    def test_slow_alerts_does_not_delay_vehicle_positions(self):
        value, executor = scheduler()
        value.step(0)
        finish_and_ack(value, executor, "vehiclepositions", 0.1)
        # Alerts remains unresolved throughout.
        value.step(10)
        self.assertEqual(2, executor.count("vehiclepositions"))
        self.assertEqual(1, executor.count("alerts"))

    def test_slow_trip_updates_does_not_delay_vehicle_positions(self):
        value, executor = scheduler()
        value.step(0)
        finish_and_ack(value, executor, "vehiclepositions", 0.1)
        # TripUpdates remains unresolved throughout.
        value.step(10)
        self.assertEqual(2, executor.count("vehiclepositions"))
        self.assertEqual(1, executor.count("tripupdates"))

    def test_same_feed_never_overlaps_or_builds_a_backlog(self):
        value, executor = scheduler()
        value.step(0)
        value.step(10)
        value.step(20)
        self.assertEqual(1, executor.count("vehiclepositions"))
        state = value.snapshot(20)["vehiclepositions"]
        self.assertEqual(2, state["total_missed_deadlines"])
        executor.latest("vehiclepositions").set_result("vehiclepositions")
        value.step(25)
        item = value.get(0)
        self.assertEqual("vehiclepositions", item.feed_name)
        # A completed result may coexist with the next request, but two HTTP
        # requests to the feed never do.
        value.step(30)
        self.assertEqual(2, executor.count("vehiclepositions"))

    def test_request_latency_does_not_shift_anchored_deadlines(self):
        value, executor = scheduler()
        value.step(0)
        finish_and_ack(value, executor, "vehiclepositions", 7)
        value.step(10)
        self.assertEqual(2, executor.count("vehiclepositions"))
        state = value.schedules["vehiclepositions"]
        self.assertEqual(20, state.next_deadline)
        self.assertEqual(0, state.active_scheduler_lag_ms)

    def test_changing_one_interval_does_not_change_other_feeds(self):
        value, executor = scheduler({"vehiclepositions": 5, "tripupdates": 20, "alerts": 30})
        value.step(0)
        for feed in FEED_NAMES:
            finish_and_ack(value, executor, feed, 0.1)
        value.step(10)
        self.assertEqual(2, executor.count("vehiclepositions"))
        self.assertEqual(1, executor.count("tripupdates"))
        self.assertEqual(1, executor.count("alerts"))


class FinalReadinessTests(unittest.TestCase):
    def test_default_and_legacy_interval_configuration(self):
        default = CollectorConfig(api_key="x", data_dir=Path("/tmp/not-used"))
        self.assertEqual(DEFAULT_FEED_INTERVALS, default.feed_intervals)
        legacy = CollectorConfig(api_key="x", data_dir=Path("/tmp/not-used"), poll_interval_seconds=20)
        self.assertEqual({feed: 20.0 for feed in FEED_NAMES}, legacy.feed_intervals)
        overridden = CollectorConfig(
            api_key="x",
            data_dir=Path("/tmp/not-used"),
            poll_interval_seconds=20,
            alerts_interval_seconds=35,
        )
        self.assertEqual(20, overridden.feed_intervals["tripupdates"])
        self.assertEqual(35, overridden.feed_intervals["alerts"])

    def test_realtime_intervals_below_five_seconds_are_rejected(self):
        for kwargs in (
            {"poll_interval_seconds": 4.9},
            {"vehicle_positions_interval_seconds": 4.9},
            {"trip_updates_interval_seconds": 0},
            {"alerts_interval_seconds": -1},
            {"alerts_interval_seconds": float("nan")},
            {"vehicle_positions_interval_seconds": float("inf")},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaisesRegex(ValueError, "at least 5 seconds"):
                CollectorConfig(api_key="x", data_dir=Path("/tmp/not-used"), **kwargs)

    def test_first_trip_update_raw_is_due_even_when_monotonic_is_below_interval_and_after_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            config = CollectorConfig(api_key="x", data_dir=data_dir, tripupdates_raw_archive_seconds=300)
            result = FetchResult(
                feed_name="tripupdates",
                poll_id="first",
                request_started_at="2026-08-23T00:00:00+00:00",
                request_started_ts=1,
                response_received_at="2026-08-23T00:00:01+00:00",
                response_received_ts=1,
                latency_ms=1,
                http_status=200,
                payload=b"first",
                error=None,
            )
            first = Collector(config)
            with patch("collector.time.monotonic", return_value=42):
                archived, ok, _path = first._archive_raw(result, "2026-08-23")
            self.assertTrue(archived and ok)
            first.executor.shutdown(wait=True)
            for session in first.sessions.values():
                session.close()

            restarted = Collector(config)
            restarted_result = FetchResult(**{**result.__dict__, "poll_id": "restart", "payload": b"restart"})
            with patch("collector.time.monotonic", return_value=3):
                archived, ok, _path = restarted._archive_raw(restarted_result, "2026-08-23")
            self.assertTrue(archived and ok)
            restarted.executor.shutdown(wait=True)
            for session in restarted.sessions.values():
                session.close()
            self.assertEqual(2, scan_raw_log(data_dir / "raw/tripupdates/date=2026-08-23/tripupdates.rawlog").complete_records)

    def test_restart_after_midnight_repairs_latest_prior_raw_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
            path = data_dir / "raw" / "tripupdates" / f"date={yesterday}" / "tripupdates.rawlog"
            append_record(path, time.time() - 30, b"valid")
            with open(path, "ab") as handle:
                handle.write(b"truncated-after-midnight-crash")
            collector = Collector(CollectorConfig(api_key="x", data_dir=data_dir))
            collector.recover_recent_raw_tails()
            collector.executor.shutdown(wait=True)
            for session in collector.sessions.values():
                session.close()
            self.assertTrue(scan_raw_log(path).clean)
            self.assertEqual(1, scan_raw_log(path).complete_records)
            tails = list(path.parent.glob("tripupdates.rawlog.corrupt-tail-*"))
            self.assertEqual(1, len(tails))
            self.assertEqual(b"truncated-after-midnight-crash", tails[0].read_bytes())

    def test_unrepairable_prior_partition_does_not_block_current_raw_archival(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            collector = Collector(CollectorConfig(api_key="x", data_dir=data_dir))
            prior = data_dir / "raw/tripupdates/date=2026-08-20/tripupdates.rawlog"
            collector.raw_needs_repair.add(prior)
            result = FetchResult(
                feed_name="tripupdates",
                poll_id="current",
                request_started_at="2026-08-23T00:00:00+00:00",
                request_started_ts=1,
                response_received_at="2026-08-23T00:00:01+00:00",
                response_received_ts=1,
                latency_ms=1,
                http_status=200,
                payload=b"current-raw-remains-collectable",
                error=None,
            )
            with patch("collector.repair_truncated_tail", side_effect=PermissionError("legacy read-only")):
                archived, ok, _path = collector._archive_raw(result, "2026-08-23")
            self.assertTrue(archived and ok)
            self.assertIn(prior, collector.raw_needs_repair)
            self.assertEqual(
                1,
                scan_raw_log(data_dir / "raw/tripupdates/date=2026-08-23/tripupdates.rawlog").complete_records,
            )
            collector.executor.shutdown(wait=True)
            for session in collector.sessions.values():
                session.close()

    def test_trip_update_policy_covers_every_column_exactly_once(self):
        groups = [
            set(TRIP_UPDATE_KEY_FIELDS),
            set(TRIP_UPDATE_EXACT_MUTABLE_FIELDS),
            set(TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS),
            set(TRIP_UPDATE_IMMUTABLE_OR_REDUNDANT_FIELDS),
            set(TRIP_UPDATE_COLLECTION_METADATA_FIELDS),
        ]
        self.assertEqual(set(PARQUET_COLUMNS["tripupdates"]), classified_trip_update_fields())
        for left in range(len(groups)):
            for right in range(left + 1, len(groups)):
                self.assertFalse(groups[left] & groups[right])

    def test_every_meaningful_non_delay_trip_update_change_emits(self):
        base = {field: "key" for field in TRIP_UPDATE_KEY_FIELDS}
        for field in TRIP_UPDATE_EXACT_MUTABLE_FIELDS:
            with self.subTest(field=field):
                tracker = ChangeTracker(
                    key_fields=TRIP_UPDATE_KEY_FIELDS,
                    value_fields=TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
                    numeric_tolerance_fields=TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
                    tolerance=15,
                    heartbeat_seconds=1800,
                )
                tracker.filter([base], "2026-08-23", 100)
                changed = {**base, field: "meaningful-change"}
                self.assertEqual([changed], tracker.filter([changed], "2026-08-23", 101))

    def test_trip_update_collection_metadata_alone_does_not_emit(self):
        tracker = ChangeTracker(
            key_fields=TRIP_UPDATE_KEY_FIELDS,
            value_fields=TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
            numeric_tolerance_fields=TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
            tolerance=15,
            heartbeat_seconds=1800,
        )
        base = {field: "key" for field in TRIP_UPDATE_KEY_FIELDS}
        first = {**base, **{field: "old" for field in TRIP_UPDATE_COLLECTION_METADATA_FIELDS}}
        second = {**base, **{field: "new" for field in TRIP_UPDATE_COLLECTION_METADATA_FIELDS}}
        self.assertEqual([first], tracker.filter([first], "2026-08-23", 100))
        self.assertEqual([], tracker.filter([second], "2026-08-23", 101))

    def test_absolute_prediction_time_jitter_is_suppressed_but_metro_revision_emits(self):
        config = CollectorConfig(
            api_key="x",
            data_dir=Path("/tmp/not-used"),
            prediction_time_change_threshold_seconds=5,
        )
        tracker = ChangeTracker(
            key_fields=TRIP_UPDATE_KEY_FIELDS,
            value_fields=TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
            numeric_tolerance_fields=TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
            tolerance=config.delay_change_threshold_seconds,
            numeric_tolerances=config.trip_update_numeric_tolerances,
            heartbeat_seconds=1800,
        )

        def metro_row(revision: int, poll: str):
            feed = pb.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            update = feed.entity.add(id="metro-5100").trip_update
            update.trip.trip_id = "metro-trip"
            update.trip.route_id = "5100"
            update.trip.start_date = "20260823"
            stop = update.stop_time_update.add(stop_id="metro-stop", stop_sequence=4)
            stop.arrival.time = 1_800_000_000 + revision
            stop.departure.time = 1_800_000_030 + revision
            row = parse_trip_updates(
                feed,
                {
                    "poll_id": poll,
                    "request_started_at": f"2026-08-23T00:00:{poll}+00:00",
                    "response_received_at": f"2026-08-23T00:00:{poll}+00:00",
                },
            )[0]
            self.assertIsNone(row["arrival_delay"])
            self.assertIsNone(row["departure_delay"])
            return row

        first = metro_row(0, "00")
        self.assertEqual([first], tracker.filter([first], "2026-08-23", 0))
        for seconds, jitter in ((10, 2), (20, 4), (30, 5)):
            self.assertEqual([], tracker.filter([metro_row(jitter, f"{seconds:02d}")], "2026-08-23", seconds))
        revised = metro_row(6, "40")
        self.assertEqual([revised], tracker.filter([revised], "2026-08-23", 40))
        # Identical predictions still receive the normal long-term heartbeat.
        heartbeat = metro_row(6, "50")
        self.assertEqual([heartbeat], tracker.filter([heartbeat], "2026-08-23", 1840))

    def test_surface_mode_null_delays_use_the_same_absolute_prediction_policy(self):
        config = CollectorConfig(api_key="x", data_dir=Path("/tmp/not-used"))
        tracker = ChangeTracker(
            key_fields=TRIP_UPDATE_KEY_FIELDS,
            value_fields=TRIP_UPDATE_EXACT_MUTABLE_FIELDS,
            numeric_tolerance_fields=TRIP_UPDATE_TOLERANT_NUMERIC_FIELDS,
            tolerance=config.delay_change_threshold_seconds,
            numeric_tolerances=config.trip_update_numeric_tolerances,
            heartbeat_seconds=1800,
        )

        def bus_row(revision: int):
            feed = pb.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            update = feed.entity.add(id="bus").trip_update
            update.trip.trip_id = "bus-trip"
            update.trip.route_id = "5"
            stop = update.stop_time_update.add(stop_id="bus-stop", stop_sequence=8)
            stop.arrival.time = 1_800_001_000 + revision
            stop.departure.time = 1_800_001_030 + revision
            return parse_trip_updates(feed, "2026-08-23T01:00:00+00:00")[0]

        first = bus_row(0)
        self.assertIsNone(first["arrival_delay"])
        self.assertEqual([first], tracker.filter([first], "2026-08-23", 0))
        self.assertEqual([], tracker.filter([bus_row(5)], "2026-08-23", 10))
        changed = bus_row(6)
        self.assertEqual([changed], tracker.filter([changed], "2026-08-23", 20))

    def test_all_null_primary_signals_fail_loudly_even_when_other_fields_are_populated(self):
        signal_fields = ("arrival_delay", "departure_delay", "arrival_time", "departure_time")
        tracker = ChangeTracker(
            key_fields=("entity_id",),
            value_fields=("route_id",),
            numeric_tolerance_fields=signal_fields,
            required_signal_fields=signal_fields,
            null_guard_min_rows=2,
            tolerance=15,
        )
        rows = [
            {"entity_id": "one", "route_id": "5"},
            {"entity_id": "two", "route_id": "7"},
        ]
        with self.assertRaisesRegex(ChangeTrackerSignalError, "all required change-tracking signals are null"):
            tracker.filter(rows, "2026-08-23", 0)

    def test_change_tracker_without_mutable_fields_is_rejected_at_startup(self):
        with self.assertRaisesRegex(ValueError, "at least one mutable tracked field"):
            ChangeTracker(key_fields=("entity_id",), value_fields=(), numeric_tolerance_fields=())

    def test_manifest_uses_each_feeds_own_cadence(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            date = "2026-08-20"
            raw_feed = pb.FeedMessage()
            raw_feed.header.gtfs_realtime_version = "2.0"
            raw = raw_feed.SerializeToString()
            intervals = {"vehiclepositions": 10, "tripupdates": 15, "alerts": 30}
            expected = {"vehiclepositions": 8640, "tripupdates": 5760, "alerts": 2880}
            from monitoring import append_poll_event

            for feed, interval in intervals.items():
                append_record(data_dir / "raw" / feed / f"date={date}" / f"{feed}.rawlog", 1, raw)
                write_parquet_atomic(feed, [], data_dir / "parquet" / feed / f"date={date}" / "empty.parquet")
                append_poll_event(
                    data_dir,
                    feed,
                    date,
                    {
                        "success": True,
                        "parse_ok": True,
                        "raw_archived": True,
                        "poll_interval_seconds": interval,
                        "raw_archive_interval_seconds": interval,
                        "parsed_rows": 0,
                        "emitted_rows": 0,
                        "freshness_flags": [],
                        "response_received_at": f"{date}T00:00:01+00:00",
                    },
                )
                stats, _errors, _quality = _feed_stats(data_dir, feed, date, create_empty=False)
                self.assertEqual(interval, stats["configured_poll_interval_seconds"])
                self.assertEqual(expected[feed], stats["expected_polls"])

    def test_live_schema_summary_counts_realcity_without_writing_data(self):
        feed = pb.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        vehicle = feed.entity.add(id="v").vehicle
        vehicle.vehicle.id = "vehicle"
        extension = vehicle.vehicle.Extensions[realcity.vehicle]
        extension.vehicle_model = "model"
        rows = parse_vehicle_positions(feed, "2026-08-23T00:00:00+00:00")
        summary = summarize_live_sample("vehiclepositions", feed, rows)
        self.assertEqual(1, summary["entities"])
        self.assertEqual(1, summary["realcity_vehicle_extension_present"])
        self.assertEqual(1, summary["bkk_non_null_parsed_rows"]["bkk_vehicle_model"])

    def test_scheduler_health_is_cadence_aware(self):
        from monitoring import HealthMonitor

        with tempfile.TemporaryDirectory() as temporary:
            monitor = HealthMonitor(
                Path(temporary),
                stale_seconds=180,
                absent_seconds=30,
                frozen_seconds=300,
                alerts_frozen_seconds=86400,
            )
            for feed in FEED_NAMES:
                monitor.record_success(
                    feed,
                    now_ts=100,
                    header_timestamp=100,
                    content_sha256=feed,
                    min_entity_timestamp=100 if feed != "alerts" else None,
                    max_entity_timestamp=100 if feed != "alerts" else None,
                    parse_ok=True,
                    raw_ok=True,
                    spool_ok=True,
                    entity_count=1,
                )
            schedules = {
                feed: {
                    "interval_seconds": interval,
                    "in_flight_seconds": 12 if feed == "vehiclepositions" else None,
                    "missed_since_last_request": 1 if feed == "vehiclepositions" else 0,
                    "next_deadline_in_seconds": 1,
                }
                for feed, interval in DEFAULT_FEED_INTERVALS.items()
            }
            status = monitor.write_status(
                now_ts=101,
                poll_id="health",
                disk_free_bytes=10_000,
                disk_warn_bytes=100,
                disk_critical_bytes=50,
                pending_spool_segments=0,
                parquet_flush_errors=[],
                cycle_errors=[],
                scheduler=schedules,
            )
            self.assertTrue(status["healthy"], status["reasons"])
            self.assertIn("vehiclepositions:request_exceeds_cadence", status["warnings"])
            self.assertIn("vehiclepositions:scheduler_missed_deadline", status["warnings"])
            self.assertEqual(90, status["feeds"]["alerts"]["absence_threshold_seconds"])

    def test_all_null_change_tracker_signal_is_persisted_as_unhealthy_data(self):
        from monitoring import HealthMonitor

        with tempfile.TemporaryDirectory() as temporary:
            monitor = HealthMonitor(
                Path(temporary),
                stale_seconds=180,
                absent_seconds=180,
                frozen_seconds=300,
                alerts_frozen_seconds=86400,
            )
            flags = monitor.record_success(
                "tripupdates",
                now_ts=100,
                header_timestamp=100,
                content_sha256="payload",
                min_entity_timestamp=None,
                max_entity_timestamp=None,
                parse_ok=True,
                change_tracking_ok=False,
                change_tracking_failure_flag="change_tracker_signal_missing",
                raw_ok=True,
                spool_ok=True,
                entity_count=1000,
            )
            self.assertIn("change_tracker_signal_missing", flags)
            self.assertFalse(monitor.feeds["tripupdates"]["change_tracking_ok"])

    def test_trip_update_spool_failure_forces_raw_fallback_between_normal_snapshots(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            collector = Collector(
                CollectorConfig(
                    api_key="x",
                    data_dir=data_dir,
                    tripupdates_raw_archive_seconds=300,
                    feed_stale_seconds=1000,
                    feed_absent_seconds=1000,
                    frozen_payload_seconds=1000,
                    alerts_frozen_payload_seconds=1000,
                )
            )
            feed = pb.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            update = feed.entity.add(id="tu").trip_update
            update.trip.trip_id = "trip"
            update.stop_time_update.add(stop_id="stop", stop_sequence=1).arrival.delay = 1
            raw = feed.SerializeToString()

            def result(poll_id, timestamp, payload=raw):
                received = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                return FetchResult(
                    feed_name="tripupdates",
                    poll_id=poll_id,
                    request_started_at=received,
                    request_started_ts=timestamp,
                    response_received_at=received,
                    response_received_ts=timestamp,
                    latency_ms=1,
                    http_status=200,
                    payload=payload,
                    error=None,
                )

            timestamp = datetime.now(timezone.utc).timestamp()
            date = datetime.now(timezone.utc).date().isoformat()
            with patch("collector.time.monotonic", return_value=100):
                collector.process_result(result("first", timestamp), [])
            feed.entity[0].trip_update.stop_time_update[0].arrival.delay = 100
            changed_raw = feed.SerializeToString()
            with patch("collector.time.monotonic", return_value=110), patch.object(
                collector.spool, "stage", side_effect=OSError("disk write failed")
            ):
                collector.process_result(result("fallback", timestamp + 10, changed_raw), [])
            path = data_dir / "raw/tripupdates" / f"date={date}" / "tripupdates.rawlog"
            self.assertEqual(2, scan_raw_log(path).complete_records)
            from monitoring import iter_jsonl, poll_journal_path

            events = [event for _line, event in iter_jsonl(poll_journal_path(data_dir, "tripupdates", date))]
            self.assertGreater(events[-1]["selected_rows"], 0)
            self.assertEqual(0, events[-1]["emitted_rows"])
            self.assertFalse(events[-1]["spool_ok"])
            collector.executor.shutdown(wait=True)
            for session in collector.sessions.values():
                session.close()

    def test_prediction_revision_diagnostic_preserves_large_values_and_uses_raw_cadence(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_path = Path(temporary) / "tripupdates.rawlog"

            def payload(arrival: int, departure: int) -> bytes:
                feed = pb.FeedMessage()
                feed.header.gtfs_realtime_version = "2.0"
                update = feed.entity.add(id="bus").trip_update
                update.trip.trip_id = "bus-trip"
                update.trip.start_date = "20260823"
                stop = update.stop_time_update.add(stop_id="bus-stop", stop_sequence=8)
                stop.arrival.time = arrival
                stop.departure.time = departure
                return feed.SerializeToString()

            append_record(raw_path, 1_800_000_000, payload(1_800_000_100, 1_800_000_130))
            append_record(raw_path, 1_800_000_300, payload(1_800_000_104, 1_800_012_620))
            report = prediction_revision_report(raw_path)
            self.assertEqual(2, report["snapshots_analyzed"])
            self.assertEqual(4, report["arrival"]["max_seconds"])
            self.assertEqual(12_490, report["departure"]["max_seconds"])
            self.assertEqual(1, report["departure"]["above_threshold"]["60"]["count"])
            self.assertIn("raw archive cadence", report["snapshot_interval_note"])

    def test_static_observation_timeline_does_not_hide_intraday_changes_or_legacy_uncertainty(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            store = StaticGtfsStore(data_dir, check_interval_seconds=1, retry_seconds=1)
            day = datetime(2026, 8, 20, tzinfo=timezone.utc)
            for hour, digest, source in (
                (0, "old", "legacy_archive_migration"),
                (12, "new", "https://go.bkk.hu/static.zip"),
            ):
                append_jsonl(
                    store.history_path,
                    {
                        "checked_at": (day + timedelta(hours=hour)).isoformat(),
                        "sha256": digest,
                        "size": 1,
                        "version_path": f"versions/{digest}.zip",
                        "source": source,
                    },
                )
            morning = store.observed_version_at((day + timedelta(hours=8)).timestamp())
            afternoon = store.observed_version_at((day + timedelta(hours=13)).timestamp())
            self.assertEqual("old", morning["sha256"])
            self.assertEqual("legacy_schedule_uncertain", morning["applicability_confidence"])
            self.assertEqual("new", afternoon["sha256"])
            timeline = store.observed_timeline(day.timestamp(), (day + timedelta(days=1)).timestamp())
            self.assertEqual(["old", "new"], [entry["sha256"] for entry in timeline])
            self.assertEqual((day + timedelta(hours=12)).isoformat(), timeline[0]["effective_until"])

    def test_static_join_report_preserves_and_labels_external_trip_updates(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            date = "2026-08-20"
            timestamp = datetime(2026, 8, 20, 8, tzinfo=timezone.utc).timestamp()
            payload = diagnostic_gtfs_zip()
            digest = hashlib.sha256(payload).hexdigest()
            version_path = data_dir / "static_gtfs" / "versions" / f"{digest}.zip"
            version_path.parent.mkdir(parents=True)
            version_path.write_bytes(payload)
            append_jsonl(
                data_dir / "static_gtfs/history.jsonl",
                {
                    "checked_at": datetime(2026, 8, 20, 0, tzinfo=timezone.utc).isoformat(),
                    "sha256": digest,
                    "size": len(payload),
                    "version_path": f"versions/{digest}.zip",
                    "feed_version": "test",
                    "source": "daily-test-observation",
                },
            )
            feed = pb.FeedMessage()
            feed.header.gtfs_realtime_version = "2.0"
            matched = feed.entity.add(id="bus").trip_update
            matched.trip.trip_id = "bus-trip"
            matched.trip.route_id = "bus-route"
            matched.stop_time_update.add(stop_id="bus-stop", stop_sequence=8).arrival.time = int(timestamp + 60)
            external = feed.entity.add(id="external").trip_update
            external.trip.trip_id = "external-trip"
            external.trip.route_id = "IC"
            external.stop_time_update.add(stop_id="external-stop", stop_sequence=1).arrival.time = int(timestamp + 120)
            append_record(
                data_dir / f"raw/tripupdates/date={date}/tripupdates.rawlog",
                timestamp,
                feed.SerializeToString(),
            )
            report = tripupdate_static_join_report(data_dir, date)
            self.assertEqual(2, report["overall"]["stop_updates"])
            self.assertEqual(1, report["by_mode"]["bus"]["exact_stop_matched"])
            external_mode = report["by_mode"]["external_or_unmatched_valid_realtime"]
            self.assertEqual(1, external_mode["stop_updates"])
            self.assertEqual({"IC": 1}, report["unmatched_realtime_route_ids"])
            self.assertEqual("none; join quality is diagnostic evidence, not collector liveness", report["collector_health_impact"])

    def test_legacy_inventory_is_non_destructive_idempotent_and_unblocks_v2_backlog(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            date = "2026-08-20"
            feed_message = pb.FeedMessage()
            feed_message.header.gtfs_realtime_version = "2.0"
            raw = feed_message.SerializeToString()
            originals = []
            for feed in FEED_NAMES:
                raw_path = data_dir / "raw" / feed / f"date={date}" / f"{feed}.rawlog"
                append_record(raw_path, 1, raw)
                parquet_path = data_dir / "parquet" / feed / f"date={date}" / "legacy.parquet"
                write_parquet_atomic(feed, [], parquet_path)
                originals.extend((raw_path, parquet_path))
            before = {path: (path.stat().st_size, path.stat().st_mtime_ns, sha256_file(path)) for path in originals}
            migrator = LegacyMigrator(data_dir, "repo", "token")
            remote = MemoryRemoteApi()
            migrator.manager._api = remote
            already_remote = originals[0]
            remote.remote[str(already_remote.relative_to(data_dir))] = (
                already_remote.stat().st_size,
                sha256_file(already_remote),
            )
            dry_run = migrator.inventory()
            self.assertFalse((data_dir / "metadata/legacy_inventory.json").exists())
            self.assertEqual("legacy_present_but_completeness_unverifiable", dry_run["dates"][0]["classification"])
            self.assertEqual(
                "sha256_match",
                dry_run["dates"][0]["remote_status"][str(already_remote.relative_to(data_dir))],
            )
            self.assertNotIn(
                str(already_remote.relative_to(data_dir)),
                dry_run["dates"][0]["files_that_would_be_uploaded"],
            )
            first = migrator.apply(dry_run)
            second = migrator.apply(migrator.inventory())
            self.assertEqual(first["dates"][0]["classification"], second["dates"][0]["classification"])
            self.assertTrue(second["dates"][0]["remote_copy_confirmed"])
            self.assertTrue(second["dates"][0]["legacy_receipt_remote_verified"])
            for path, evidence in before.items():
                self.assertEqual(evidence, (path.stat().st_size, path.stat().st_mtime_ns, sha256_file(path)))
            self.assertFalse(read_json(data_dir / f"backup_receipts/legacy/date={date}.json", {})["scientific_complete"])
            store = StaticGtfsStore(data_dir, check_interval_seconds=1, retry_seconds=1)
            manager = BackupManager(data_dir, "repo", "token", store, api=object())
            self.assertNotIn(date, manager.pending_dates())

    def test_legacy_inventory_marks_missing_artifacts_corrupt(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            date = "2026-08-20"
            append_record(
                data_dir / "raw/vehiclepositions" / f"date={date}" / "vehiclepositions.rawlog",
                1,
                b"only-one-feed",
            )
            report = LegacyMigrator(data_dir, "", "").inventory()
            self.assertEqual("corrupt_or_missing", report["dates"][0]["classification"])
            self.assertTrue(report["dates"][0]["problems"])

    def test_maintenance_never_compacts_legacy_dates_without_v2_journals(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            date = "2026-08-20"
            path = data_dir / "parquet/tripupdates" / f"date={date}" / "legacy.parquet"
            write_parquet_atomic("tripupdates", [], path)
            before = (path.stat().st_size, sha256_file(path))
            worker = MaintenanceWorker(
                MaintenanceConfig(data_dir=data_dir, hf_token="", hf_repo_id="")
            )
            with patch("maintenance.compact_partition") as compact:
                worker.compact_pending_dates()
            worker.session.close()
            compact.assert_not_called()
            self.assertEqual(before, (path.stat().st_size, sha256_file(path)))

    def test_external_deadman_ping_is_suppressed_when_offsite_backup_is_disabled(self):
        with tempfile.TemporaryDirectory() as temporary:
            worker = MaintenanceWorker(
                MaintenanceConfig(
                    data_dir=Path(temporary),
                    hf_token="",
                    hf_repo_id="",
                    healthcheck_url="https://example.invalid/secret-ping",
                )
            )
            with patch.object(worker.session, "get") as request:
                worker.maybe_ping_healthcheck({"healthy": True})
            request.assert_not_called()
            status = worker.write_status()
            self.assertIn("offsite_backup_disabled", status["warnings"])
            worker.session.close()


if __name__ == "__main__":
    unittest.main()
