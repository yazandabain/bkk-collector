from __future__ import annotations

import io
import json
import tempfile
import time
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from google.transit import gtfs_realtime_pb2 as pb

import realcity
from atomic_io import atomic_write_json, sha256_file
from backup import BackupManager
from collector import Collector, FetchResult
from config import CollectorConfig, FEED_NAMES
from dedup import ChangeTracker
from gtfs_rt_parse import parse_alerts, parse_trip_updates, parse_vehicle_positions
from manifests import build_daily_manifest
from monitoring import HealthMonitor, append_poll_event, entity_timestamp_range, evaluate_freshness
from parquet_compact import compact_partition
from parquet_store import DurableParquetSpool, parquet_row_count, write_parquet_atomic
from raw_log import append_record, iter_records, repair_truncated_tail
from static_gtfs import StaticGtfsStore


def feed_message(timestamp: int = 100) -> pb.FeedMessage:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = timestamp
    return feed


def gtfs_zip_bytes(marker: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in ("agency.txt", "routes.txt", "trips.txt", "stops.txt", "stop_times.txt"):
            archive.writestr(name, f"marker\n{marker}\n")
        archive.writestr("feed_info.txt", f"feed_version\n{marker}\n")
    return output.getvalue()


class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.status_code = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size: int):
        for start in range(0, len(self.payload), chunk_size):
            yield self.payload[start : start + chunk_size]


class StaticSession:
    def __init__(self, payloads: list[bytes]):
        self.payloads = iter(payloads)

    def get(self, *_args, **_kwargs):
        return FakeResponse(next(self.payloads))


class ReliabilityTests(unittest.TestCase):
    def test_synthetic_poll_cycle_archives_journals_and_commits(self):
        now = time.time()
        feeds = {}
        vehicle_feed = feed_message(int(now))
        vehicle = vehicle_feed.entity.add(id="vehicle").vehicle
        vehicle.position.latitude = 47.5
        vehicle.position.longitude = 19.0
        feeds["vehiclepositions"] = vehicle_feed.SerializeToString()
        trip_feed = feed_message(int(now))
        update = trip_feed.entity.add(id="trip-update").trip_update
        update.trip.trip_id = "trip"
        stop = update.stop_time_update.add(stop_id="stop", stop_sequence=1)
        stop.arrival.delay = 10
        feeds["tripupdates"] = trip_feed.SerializeToString()
        alert_feed = feed_message(int(now))
        alert_feed.entity.add(id="alert").alert.header_text.translation.add(text="Notice", language="en")
        feeds["alerts"] = alert_feed.SerializeToString()

        with tempfile.TemporaryDirectory() as temporary:
            config = CollectorConfig(
                api_key="secret",
                data_dir=Path(temporary),
                parquet_flush_seconds=0,
                feed_stale_seconds=1000,
                feed_absent_seconds=1000,
                frozen_payload_seconds=1000,
                alerts_frozen_payload_seconds=1000,
            )
            collector = Collector(config)

            def fake_fetch(feed_name, poll_id):
                started = now
                received = now + 0.01
                return FetchResult(
                    feed_name=feed_name,
                    poll_id=poll_id,
                    request_started_at="2026-08-23T00:00:00+00:00",
                    request_started_ts=started,
                    response_received_at="2026-08-23T00:00:00.010000+00:00",
                    response_received_ts=received,
                    latency_ms=10,
                    http_status=200,
                    payload=feeds[feed_name],
                    error=None,
                )

            with patch.object(collector, "fetch_feed", side_effect=fake_fetch):
                status = collector.poll_once()
            collector.executor.shutdown(wait=True)
            for session in collector.sessions.values():
                session.close()
            self.assertTrue(status["healthy"], status["reasons"])
            for feed_name in FEED_NAMES:
                self.assertTrue(list((Path(temporary) / "raw" / feed_name).glob("date=*/*.rawlog")))
                self.assertTrue(list((Path(temporary) / "metadata" / "polls" / feed_name).glob("date=*/polls.jsonl")))
                self.assertTrue(list((Path(temporary) / "parquet" / feed_name).glob("date=*/*.parquet")))

    def test_repeated_same_stop_with_different_sequence_is_not_collapsed(self):
        feed = feed_message()
        entity = feed.entity.add()
        entity.id = "trip-update-1"
        entity.trip_update.trip.trip_id = "trip"
        entity.trip_update.trip.start_date = "20260823"
        entity.trip_update.trip.start_time = "12:00:00"
        for sequence in (4, 9):
            stop = entity.trip_update.stop_time_update.add()
            stop.stop_id = "SAME_STOP"
            stop.stop_sequence = sequence
            stop.arrival.delay = 30
        rows = parse_trip_updates(feed, "2026-08-23T12:00:00+00:00")
        tracker = ChangeTracker(
            key_fields=(
                "entity_id", "trip_id", "start_date", "start_time", "stop_sequence",
                "stop_visit_fallback_index", "stop_id",
            ),
            numeric_tolerance_fields=("arrival_delay",),
            tolerance=15,
        )
        selected = tracker.filter(rows, "2026-08-23", 1.0)
        self.assertEqual([4, 9], [row["stop_sequence"] for row in selected])

    def test_deleted_entities_are_preserved_in_derived_rows(self):
        parsers = (parse_vehicle_positions, parse_trip_updates, parse_alerts)
        for parser in parsers:
            feed = feed_message()
            entity = feed.entity.add(id="deleted")
            entity.is_deleted = True
            rows = parser(feed, "2026-08-23T00:00:00+00:00")
            self.assertEqual(1, len(rows))
            self.assertEqual("deleted", rows[0]["entity_id"])
            self.assertTrue(rows[0]["entity_is_deleted"])

    def test_missing_stop_sequence_uses_ordinal_fallback_identity(self):
        feed = feed_message()
        update = feed.entity.add(id="trip-update").trip_update
        update.trip.trip_id = "trip"
        for _ in range(2):
            stop = update.stop_time_update.add(stop_id="REPEATED")
            stop.arrival.delay = 10
        rows = parse_trip_updates(feed, "2026-08-23T00:00:00+00:00")
        self.assertEqual([0, 1], [row["stop_visit_fallback_index"] for row in rows])

    def test_failed_parquet_write_retains_durable_buffer(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            spool = DurableParquetSpool(data_dir, flush_seconds=0)
            segment = spool.stage("alerts", "2026-08-22", "poll", [{"schema_version": 2, "entity_id": "a"}])
            with patch("parquet_store.write_parquet_atomic", side_effect=OSError("disk full")):
                result = spool.flush(force=True)
            self.assertFalse(result.ok)
            self.assertTrue(segment.exists())
            self.assertEqual([segment], spool.pending_segments())

    def test_midnight_rows_are_partitioned_by_collection_date(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            spool = DurableParquetSpool(data_dir, flush_seconds=0)
            spool.stage("vehiclepositions", "2026-08-21", "before-midnight", [{"schema_version": 2, "entity_id": "a"}])
            spool.stage("vehiclepositions", "2026-08-22", "after-midnight", [{"schema_version": 2, "entity_id": "b"}])
            row_counts = {}

            def fake_write(_feed, rows, path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"valid-for-mocked-counter")
                row_counts[path] = len(rows)

            with patch("parquet_store.write_parquet_atomic", side_effect=fake_write), patch(
                "parquet_store.parquet_row_count", side_effect=lambda path: row_counts[path]
            ):
                result = spool.flush(force=True)
            self.assertTrue(result.ok)
            self.assertEqual({"date=2026-08-21", "date=2026-08-22"}, {path.parent.name for path in result.files_written})

    def test_spool_recovers_after_process_restart(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            first = DurableParquetSpool(data_dir, flush_seconds=300)
            first.stage("alerts", "2026-08-22", "poll", [{"schema_version": 2, "entity_id": "recover"}])
            second = DurableParquetSpool(data_dir, flush_seconds=300)
            row_counts = {}

            def fake_write(_feed, rows, path):
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"valid-for-mocked-counter")
                row_counts[path] = len(rows)

            with patch("parquet_store.write_parquet_atomic", side_effect=fake_write), patch(
                "parquet_store.parquet_row_count", side_effect=lambda path: row_counts[path]
            ):
                result = second.flush(force=True)
            self.assertEqual(1, result.rows_written)
            self.assertEqual([], second.pending_segments())

    def test_spool_commit_cleanup_is_idempotent_after_interruption(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            spool = DurableParquetSpool(data_dir, flush_seconds=0)
            segment = spool.stage("alerts", "2026-08-22", "poll", [{"schema_version": 2, "entity_id": "once"}])
            original_unlink = Path.unlink
            failed_once = False

            def interrupted_unlink(path, *args, **kwargs):
                nonlocal failed_once
                if path == segment and not failed_once:
                    failed_once = True
                    raise OSError("simulated interruption before spool cleanup")
                return original_unlink(path, *args, **kwargs)

            with patch.object(Path, "unlink", new=interrupted_unlink):
                first = spool.flush(force=True)
            self.assertFalse(first.ok)
            self.assertTrue(segment.exists())
            self.assertEqual(1, len(list((data_dir / "parquet" / "alerts" / "date=2026-08-22").glob("*.parquet"))))

            recovered = DurableParquetSpool(data_dir, flush_seconds=0).flush(force=True)
            self.assertTrue(recovered.ok, recovered.errors)
            self.assertEqual([], spool.pending_segments())
            files = list((data_dir / "parquet" / "alerts" / "date=2026-08-22").glob("*.parquet"))
            self.assertEqual(1, len(files))
            self.assertEqual(1, parquet_row_count(files[0]))

    def test_long_spool_backlog_is_flushed_in_bounded_commits(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            spool = DurableParquetSpool(data_dir, flush_seconds=0)
            for index in range(21):
                spool.stage(
                    "alerts",
                    "2026-08-22",
                    f"poll-{index}",
                    [{"schema_version": 2, "entity_id": f"alert-{index}"}],
                )
            result = spool.flush(force=True)
            self.assertTrue(result.ok, result.errors)
            self.assertEqual(21, result.rows_written)
            self.assertEqual(2, len(result.files_written))
            self.assertEqual([], spool.pending_segments())

    def test_stale_and_frozen_feed_detection(self):
        flags = evaluate_freshness(
            now_ts=1000,
            header_timestamp=700,
            header_unchanged_since=600,
            payload_unchanged_since=650,
            stale_seconds=180,
            frozen_seconds=300,
        )
        self.assertEqual(
            {"source_timestamp_stale", "source_timestamp_frozen", "payload_frozen"},
            set(flags),
        )

    def test_entity_content_freeze_is_detected_when_header_keeps_advancing(self):
        with tempfile.TemporaryDirectory() as temporary:
            monitor = HealthMonitor(
                Path(temporary),
                stale_seconds=180,
                absent_seconds=180,
                frozen_seconds=10,
                alerts_frozen_seconds=10,
            )
            monitor.record_success(
                "vehiclepositions",
                now_ts=100,
                header_timestamp=100,
                content_sha256="same-entities",
                min_entity_timestamp=100,
                max_entity_timestamp=100,
                parse_ok=True,
                raw_ok=True,
                spool_ok=True,
            )
            flags = monitor.record_success(
                "vehiclepositions",
                now_ts=120,
                header_timestamp=120,
                content_sha256="same-entities",
                min_entity_timestamp=120,
                max_entity_timestamp=120,
                parse_ok=True,
                raw_ok=True,
                spool_ok=True,
            )
            self.assertIn("payload_frozen", flags)
            self.assertNotIn("source_timestamp_frozen", flags)

    def test_empty_alert_feed_is_not_reported_as_payload_frozen(self):
        with tempfile.TemporaryDirectory() as temporary:
            monitor = HealthMonitor(
                Path(temporary),
                stale_seconds=180,
                absent_seconds=180,
                frozen_seconds=10,
                alerts_frozen_seconds=10,
            )
            monitor.record_success(
                "alerts",
                now_ts=100,
                header_timestamp=100,
                content_sha256="empty",
                min_entity_timestamp=None,
                max_entity_timestamp=None,
                parse_ok=True,
                raw_ok=True,
                spool_ok=True,
                entity_count=0,
            )
            flags = monitor.record_success(
                "alerts",
                now_ts=120,
                header_timestamp=120,
                content_sha256="empty",
                min_entity_timestamp=None,
                max_entity_timestamp=None,
                parse_ok=True,
                raw_ok=True,
                spool_ok=True,
                entity_count=0,
            )
            self.assertNotIn("payload_frozen", flags)
            self.assertNotIn("source_timestamp_frozen", flags)

    def test_unchanged_nonempty_alert_is_a_warning_not_a_liveness_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            monitor = HealthMonitor(
                Path(temporary),
                stale_seconds=180,
                absent_seconds=180,
                frozen_seconds=10,
                alerts_frozen_seconds=10,
            )
            for now in (100, 120):
                flags = monitor.record_success(
                    "alerts",
                    now_ts=now,
                    header_timestamp=now,
                    content_sha256="same-alert",
                    min_entity_timestamp=None,
                    max_entity_timestamp=None,
                    parse_ok=True,
                    raw_ok=True,
                    spool_ok=True,
                    entity_count=1,
                )
            self.assertIn("payload_unchanged_warning", flags)
            status = monitor.write_status(
                now_ts=120,
                poll_id="poll",
                disk_free_bytes=10_000,
                disk_warn_bytes=100,
                disk_critical_bytes=10,
                pending_spool_segments=0,
                parquet_flush_errors=[],
                cycle_errors=[],
            )
            self.assertIn("alerts:payload_unchanged_warning", status["warnings"])
            self.assertNotIn("alerts:payload_unchanged_warning", status["reasons"])

    def test_parsers_tolerate_missing_optional_fields(self):
        vehicle_feed = feed_message()
        vehicle_feed.entity.add(id="v").vehicle.trip.trip_id = "trip"
        trip_feed = feed_message()
        trip_feed.entity.add(id="t").trip_update.trip.trip_id = "trip"
        alert_feed = feed_message()
        alert_feed.entity.add(id="a").alert.SetInParent()
        vehicle = parse_vehicle_positions(vehicle_feed, "2026-08-23T00:00:00+00:00")[0]
        trip = parse_trip_updates(trip_feed, "2026-08-23T00:00:00+00:00")[0]
        alert = parse_alerts(alert_feed, "2026-08-23T00:00:00+00:00")[0]
        self.assertIsNone(vehicle["latitude"])
        self.assertIsNone(trip["stop_sequence"])
        self.assertEqual("[]", alert["active_periods_json"])

    def test_alert_preserves_all_active_periods_and_translations(self):
        feed = feed_message()
        alert = feed.entity.add(id="alert").alert
        alert.active_period.add(start=1, end=2)
        alert.active_period.add(start=3, end=4)
        alert.header_text.translation.add(text="English", language="en")
        alert.header_text.translation.add(text="Magyar", language="hu")
        alert.Extensions[realcity.alert].modifiedTime = 99
        row = parse_alerts(feed, "2026-08-23T00:00:00+00:00")[0]
        self.assertEqual([{"end": 2, "start": 1}, {"end": 4, "start": 3}], json.loads(row["active_periods_json"]))
        self.assertEqual(2, len(json.loads(row["header_text_json"])))
        self.assertEqual(99, row["bkk_modified_time"])
        self.assertEqual((99, 99), entity_timestamp_range("alerts", feed))

    def test_bkk_vehicle_and_scheduled_stop_extensions_are_preserved(self):
        vehicle_feed = feed_message()
        descriptor = vehicle_feed.entity.add(id="vehicle").vehicle.vehicle
        vehicle_extension = descriptor.Extensions[realcity.vehicle]
        vehicle_extension.vehicle_model = "Solaris"
        vehicle_extension.deviated = True
        vehicle_extension.vehicle_type = 7
        vehicle_extension.door_open = True
        vehicle_extension.stop_distance = 42
        vehicle_row = parse_vehicle_positions(
            vehicle_feed, "2026-08-23T00:00:00+00:00"
        )[0]
        self.assertEqual("Solaris", vehicle_row["bkk_vehicle_model"])
        self.assertTrue(vehicle_row["bkk_deviated"])
        self.assertEqual(42, vehicle_row["bkk_stop_distance"])

        trip_feed = feed_message()
        stop = trip_feed.entity.add(id="trip").trip_update.stop_time_update.add(
            stop_id="stop", stop_sequence=3
        )
        stop_extension = stop.Extensions[realcity.stop_time_update]
        stop_extension.scheduled_arrival.time = 1_777_000_000
        stop_extension.scheduled_departure.time = 1_777_000_060
        trip_row = parse_trip_updates(trip_feed, "2026-08-23T00:00:00+00:00")[0]
        self.assertEqual(1_777_000_000, trip_row["bkk_scheduled_arrival_time"])
        self.assertEqual(1_777_000_060, trip_row["bkk_scheduled_departure_time"])

    def test_raw_tail_recovery_preserves_valid_records_and_forensic_tail(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "feed.rawlog"
            append_record(path, 1.0, b"first")
            with open(path, "ab") as handle:
                handle.write(b"partial-header")
            recovery = repair_truncated_tail(path)
            self.assertIsNotNone(recovery)
            self.assertTrue(recovery.exists())
            append_record(path, 2.0, b"second")
            with open(path, "rb") as handle:
                self.assertEqual([(1.0, b"first"), (2.0, b"second")], list(iter_records(handle)))

    def test_static_unchanged_hash_does_not_duplicate_version(self):
        payload = gtfs_zip_bytes("v1")
        with tempfile.TemporaryDirectory() as temporary:
            store = StaticGtfsStore(Path(temporary), check_interval_seconds=86400, retry_seconds=1)
            session = StaticSession([payload, payload])
            first = store.check(session, now_ts=1)
            second = store.check(session, now_ts=2)
            self.assertTrue(first["changed"])
            self.assertFalse(second["changed"])
            self.assertEqual(1, len(list(store.versions.glob("*.zip"))))

    def test_static_changed_hash_creates_a_new_version(self):
        with tempfile.TemporaryDirectory() as temporary:
            store = StaticGtfsStore(Path(temporary), check_interval_seconds=86400, retry_seconds=1)
            session = StaticSession([gtfs_zip_bytes("v1"), gtfs_zip_bytes("v2")])
            first = store.check(session, now_ts=1)
            second = store.check(session, now_ts=2)
            self.assertNotEqual(first["sha256"], second["sha256"])
            self.assertTrue(second["changed"])
            self.assertEqual(2, len(list(store.versions.glob("*.zip"))))

    def test_real_parquet_schema_and_atomic_writer(self):
        feed = feed_message()
        entity = feed.entity.add(id="vehicle")
        entity.vehicle.position.latitude = 47.5
        entity.vehicle.position.longitude = 19.0
        rows = parse_vehicle_positions(feed, "2026-08-23T00:00:00+00:00")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "part.parquet"
            write_parquet_atomic("vehiclepositions", rows, path)
            self.assertEqual(1, parquet_row_count(path))
            import pyarrow.parquet as pq

            table = pq.read_table(path)
            self.assertEqual(2, table.column("schema_version")[0].as_py())
            self.assertEqual("vehicle", table.column("entity_id")[0].as_py())

    def test_streaming_compaction_preserves_every_row(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            directory = data_dir / "parquet" / "alerts" / "date=2026-08-20"
            for index in range(3):
                write_parquet_atomic(
                    "alerts",
                    [{"schema_version": 2, "entity_id": f"alert-{index}"}],
                    directory / f"part-{index}.parquet",
                )
            output = compact_partition(data_dir, "alerts", "2026-08-20", min_files=2)
            self.assertIsNotNone(output)
            self.assertEqual(3, parquet_row_count(output))
            self.assertEqual([output], list(directory.glob("*.parquet")))
            self.assertFalse((directory / ".compaction-transaction.json").exists())

    def test_daily_manifest_distinguishes_legitimate_empty_feeds_from_missing_collection(self):
        with tempfile.TemporaryDirectory() as temporary:
            data_dir = Path(temporary)
            date = "2026-08-20"
            static_store = StaticGtfsStore(data_dir, check_interval_seconds=86400, retry_seconds=1)
            static_store.check(StaticSession([gtfs_zip_bytes("version")]), now_ts=1_776_924_000)
            empty_feed = feed_message(timestamp=1_776_924_000)
            raw = empty_feed.SerializeToString()
            for feed_name in FEED_NAMES:
                raw_path = data_dir / "raw" / feed_name / f"date={date}" / f"{feed_name}.rawlog"
                append_record(raw_path, 1_776_924_000, raw)
                append_poll_event(
                    data_dir,
                    feed_name,
                    date,
                    {
                        "poll_id": "poll",
                        "feed": feed_name,
                        "poll_interval_seconds": 30,
                        "request_started_at": f"{date}T12:00:00+00:00",
                        "response_received_at": f"{date}T12:00:01+00:00",
                        "success": True,
                        "parse_ok": True,
                        "raw_archived": True,
                        "parsed_rows": 0,
                        "emitted_rows": 0,
                        "freshness_flags": [],
                    },
                )
            manifest = build_daily_manifest(data_dir, date, static_store)
            self.assertTrue(manifest["complete"], manifest["completeness_errors"])
            self.assertFalse(manifest["quality_ok"])
            for feed_name in FEED_NAMES:
                self.assertEqual(0, manifest["feeds"][feed_name]["parquet_rows"])
                self.assertEqual(1, manifest["feeds"][feed_name]["parquet_files"])

            (data_dir / "raw" / "tripupdates" / f"date={date}" / "tripupdates.rawlog").unlink()
            incomplete = build_daily_manifest(data_dir, date, static_store)
            self.assertFalse(incomplete["complete"])
            self.assertTrue(any("tripupdates: raw log missing" in error for error in incomplete["completeness_errors"]))


class FakeBackupApi:
    def __init__(self, root: Path, *, fail_upload: bool = False, wrong_size: bool = False):
        self.root = root
        self.fail_upload = fail_upload
        self.wrong_size = wrong_size
        self.uploaded: list[str] = []
        self.remote: dict[str, tuple[int, str]] = {}

    def create_repo(self, **_kwargs):
        return None

    def upload_file(self, *, path_in_repo, path_or_fileobj, **_kwargs):
        if self.fail_upload:
            raise ConnectionError("offline")
        self.uploaded.append(path_in_repo)
        source = Path(path_or_fileobj)
        self.remote[path_in_repo] = (source.stat().st_size, sha256_file(source))

    def get_paths_info(self, *, paths, **_kwargs):
        infos = []
        for path in paths:
            if path not in self.remote:
                continue
            stored_size, stored_sha = self.remote[path]
            size = stored_size + (1 if self.wrong_size else 0)
            infos.append(SimpleNamespace(path=path, size=size, lfs=SimpleNamespace(sha256=stored_sha)))
        return infos


class BackupTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.date = "2026-08-20"
        artifacts = []
        feeds = {}
        for feed_name in FEED_NAMES:
            paths = (
                (self.root / "raw" / feed_name / f"date={self.date}" / f"{feed_name}.rawlog", "raw"),
                (self.root / "parquet" / feed_name / f"date={self.date}" / "part.parquet", "parquet"),
                (self.root / "metadata" / "polls" / feed_name / f"date={self.date}" / "polls.jsonl", "poll_metadata"),
            )
            for path, kind in paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(f"{feed_name}-{kind}".encode())
                artifacts.append(
                    {
                        "path": str(path.relative_to(self.root)),
                        "kind": kind,
                        "size": path.stat().st_size,
                        "sha256": sha256_file(path),
                    }
                )
            feeds[feed_name] = {
                "successful_http_polls": 1,
                "successful_parse_polls": 1,
                "raw_snapshots": 1,
                "parquet_files": 1,
                "pending_spool_segments": 0,
                "raw_log_clean": True,
            }
        static = self.root / "static_gtfs" / "versions" / "hash.zip"
        static.parent.mkdir(parents=True, exist_ok=True)
        static.write_bytes(b"static")
        artifacts.append(
            {
                "path": str(static.relative_to(self.root)),
                "kind": "static_gtfs",
                "size": static.stat().st_size,
                "sha256": sha256_file(static),
            }
        )
        self.manifest_path = self.root / "metadata" / "manifests" / f"date={self.date}.json"
        self.complete_manifest = {
            "manifest_version": 1,
            "date": self.date,
            "complete": True,
            "completeness_errors": [],
            "feeds": feeds,
            "static_gtfs": {"version_path": "versions/hash.zip", "sha256": sha256_file(static)},
            "static_gtfs_timeline": [
                {
                    "effective_from": f"{self.date}T00:00:00+00:00",
                    "effective_until": f"{self.date}T23:59:59+00:00",
                    "version_path": "versions/hash.zip",
                    "sha256": sha256_file(static),
                    "applicability_confidence": "collector_observed",
                }
            ],
            "artifacts": artifacts,
        }
        atomic_write_json(self.manifest_path, self.complete_manifest)
        self.static = Mock()

    def tearDown(self):
        self.temporary.cleanup()

    def manager(self, api):
        return BackupManager(self.root, "owner/repo", "token", self.static, api=api, logger=Mock())

    def test_backup_failure_remains_pending(self):
        manager = self.manager(FakeBackupApi(self.root, fail_upload=True))
        with patch("backup.build_daily_manifest", return_value=self.complete_manifest):
            result = manager.backup_date(self.date)
        self.assertFalse(result.success)
        self.assertIn(self.date, manager.pending_dates())
        self.assertEqual(set(), manager.confirmed_dates())

    def test_backup_does_not_start_when_completeness_validation_fails(self):
        api = FakeBackupApi(self.root)
        manager = self.manager(api)
        manifest = {"complete": False, "completeness_errors": ["missing tripupdates raw"], "artifacts": []}
        with patch("backup.build_daily_manifest", return_value=manifest):
            result = manager.backup_date(self.date)
        self.assertFalse(result.success)
        self.assertEqual([], api.uploaded)
        self.assertEqual(set(), manager.confirmed_dates())

    def test_complete_flag_without_expected_artifacts_is_not_accepted(self):
        api = FakeBackupApi(self.root)
        manager = self.manager(api)
        malformed = dict(self.complete_manifest)
        malformed["artifacts"] = []
        with patch("backup.build_daily_manifest", return_value=malformed):
            result = manager.backup_date(self.date)
        self.assertFalse(result.success)
        self.assertEqual([], api.uploaded)
        self.assertEqual(set(), manager.confirmed_dates())

    def test_remote_mismatch_is_not_marked_successful(self):
        manager = self.manager(FakeBackupApi(self.root, wrong_size=True))
        with patch("backup.build_daily_manifest", return_value=self.complete_manifest):
            result = manager.backup_date(self.date)
        self.assertFalse(result.success)
        self.assertEqual(set(), manager.confirmed_dates())

    def test_receipt_is_created_only_after_remote_verification(self):
        api = FakeBackupApi(self.root)
        manager = self.manager(api)
        with patch("backup.build_daily_manifest", return_value=self.complete_manifest):
            result = manager.backup_date(self.date)
        self.assertTrue(result.success)
        self.assertEqual({self.date}, manager.confirmed_dates())
        self.assertIn(f"backup_receipts/date={self.date}.json", api.uploaded)


if __name__ == "__main__":
    unittest.main()
