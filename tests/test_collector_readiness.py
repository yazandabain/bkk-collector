from __future__ import annotations

import io
import json
import logging
import struct
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from google.transit import gtfs_realtime_pb2 as pb
from urllib3.exceptions import NewConnectionError

import collector as collector_module
import rebuild_parquet
from atomic_io import atomic_write_json, fsync_directory, read_json, sha256_file
from backup import BackupManager
from collector import Collector, FetchResult, _new_session
from config import CollectorConfig, FEED_NAMES
from diagnostics import collection_window_report, main as diagnostics_main
from gtfs_rt_parse import parse_trip_updates
from manifests import _feed_stats, _presence_stats, build_daily_manifest
from monitoring import HealthMonitor, append_poll_event, poll_journal_path, utc_iso
from parquet_store import DurableParquetSpool, MAX_SEGMENTS_PER_COMMIT, parquet_row_count, write_parquet_atomic
from parquet_worker import ParquetCommitWorker
from poll_journal import append_poll_jsonl, recent_partition_files, repair_jsonl_tail
from raw_log import MAX_COMPRESSED_RECORD_BYTES, append_record, iter_records, repair_truncated_tail, scan_raw_log
from realtime_scheduler import IndependentFeedScheduler
from static_gtfs import StaticGtfsStore
from tests.test_reliability import FakeBackupApi, StaticSession, gtfs_zip_bytes
from tripupdate_presence import TripUpdatePresence, apply_presence_record, iter_presence, presence_path
from verify_backup import verify_day


def sample(feed_name: str, timestamp: int, sequences=(1, 2)) -> pb.FeedMessage:
    feed = pb.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = timestamp
    if feed_name == "vehiclepositions":
        vehicle = feed.entity.add(id="vehicle").vehicle
        vehicle.position.latitude, vehicle.position.longitude = 47.5, 19.0
        vehicle.timestamp = timestamp
    elif feed_name == "tripupdates":
        trip = feed.entity.add(id="update").trip_update
        trip.trip.trip_id, trip.trip.start_date, trip.trip.start_time = "trip", "20261001", "08:00:00"
        for sequence in sequences:
            stop = trip.stop_time_update.add(stop_sequence=sequence, stop_id="REPEATED")
            stop.arrival.time = timestamp + 300
    elif feed_name == "alerts":
        feed.entity.add(id="alert").alert.header_text.translation.add(text="Notice", language="en")
    return feed


def response(feed_name: str, feed: pb.FeedMessage | None, poll_id: str, timestamp: float) -> FetchResult:
    return FetchResult(feed_name, poll_id, utc_iso(timestamp - .01), timestamp - .01,
                       utc_iso(timestamp), timestamp, 10.0, 200 if feed is not None else None,
                       feed.SerializeToString() if feed is not None else None,
                       None if feed is not None else "ConnectionError: unavailable")


def wait_until(predicate, timeout=5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return
        threading.Event().wait(.01)
    raise AssertionError("condition was not satisfied before timeout")


class ReadinessFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data = Path(self.temporary.name)
        self.now = time.time()
        self.date = datetime.fromtimestamp(self.now, timezone.utc).date().isoformat()

    def collector(self, **kwargs):
        value = Collector(CollectorConfig(api_key="synthetic-key", data_dir=self.data, **kwargs))
        self.addCleanup(lambda: [session.close() for session in value.sessions.values()])
        self.addCleanup(lambda: value.executor.shutdown(wait=True))
        return value

    def context(self, poll: str):
        return {"poll_id": poll, "request_started_at": utc_iso(self.now - .01),
                "response_received_at": utc_iso(self.now), "feed_header_timestamp": int(self.now),
                "feed_incrementality": 0}

    def rows(self, sequences=(1, 2)):
        return parse_trip_updates(sample("tripupdates", int(self.now), sequences), self.context("source"))

    def records(self, date=None):
        return [record for _timestamp, record in iter_presence(presence_path(self.data, date or self.date))]


class ParquetIsolationTests(ReadinessFixture):
    def test_fatal_main_loop_error_stops_threads_and_drains_in_flight_results(self):
        value = self.collector()
        with patch.object(collector_module, "_shutdown_requested", False), \
                patch.object(value, "fetch_feed", side_effect=lambda feed, poll:
                             response(feed, sample(feed, int(self.now)), poll, self.now)), \
                patch.object(value, "_commit_and_write_status", side_effect=OSError("status disk unavailable")):
            with self.assertRaisesRegex(OSError, "status disk unavailable"):
                value.run()
        self.assertFalse(value.scheduler._thread.is_alive())
        self.assertFalse(value.commit_worker.snapshot()["alive"])
        for feed in FEED_NAMES:
            self.assertEqual(1, len(poll_journal_path(self.data, feed, self.date).read_text().splitlines()))
        recovered = DurableParquetSpool(self.data, flush_seconds=0).flush(force=True)
        self.assertTrue(recovered.ok, recovered.errors)
        self.assertEqual(4, sum(map(parquet_row_count, (self.data / "parquet").glob("*/date=*/*.parquet"))))

    def test_zero_flush_interval_has_bounded_idle_wait(self):
        spool = DurableParquetSpool(self.data, flush_seconds=0)
        worker = ParquetCommitWorker(spool, logging.getLogger("test-parquet-idle"))
        worker._stop = Mock()
        worker._stop.is_set.side_effect = [False, True]
        worker._run()
        worker._stop.wait.assert_called_once_with(1.0)

    def test_startup_and_ten_second_polls_continue_during_real_parquet_write(self):
        value = self.collector()
        old = value.spool.stage("vehiclepositions", self.date, "old", [{"entity_id": "old"}])
        writing, release = threading.Event(), threading.Event()
        real_write = write_parquet_atomic
        writes = []

        def gated_write(feed_name, rows, path):
            if not writes:
                writes.append(path)
                writing.set()
                if not release.wait(10):
                    raise TimeoutError("test did not release Parquet writer")
            return real_write(feed_name, rows, path)

        start = time.monotonic()
        clock = [start]
        schedules = []

        def scheduler_factory(fetch, intervals, **kwargs):
            scheduler = IndependentFeedScheduler(fetch, intervals, **kwargs, monotonic=lambda: clock[0],
                                                 wall_time=lambda: self.now + clock[0] - start)
            schedules.append(scheduler)
            return scheduler

        def fetch(feed, poll):
            return response(feed, sample(feed, int(self.now)), poll, self.now)

        def processed(feed, count):
            state = value.scheduler.snapshot(clock[0])[feed]
            return state["total_completed"] >= count and not state["result_pending_processing"]

        failures = []

        def run():
            try:
                value.run()
            except BaseException as error:
                failures.append(error)

        with patch.object(collector_module, "_shutdown_requested", False), \
                patch("collector.IndependentFeedScheduler", side_effect=scheduler_factory), \
                patch.object(value, "fetch_feed", side_effect=fetch), \
                patch("parquet_store.write_parquet_atomic", side_effect=gated_write):
            thread = threading.Thread(target=run, name="test-ingestion")
            thread.start()
            try:
                self.assertTrue(writing.wait(3), "old spool must actually enter the writer")
                wait_until(lambda: schedules and all(processed(feed, 1) for feed in FEED_NAMES))
                for elapsed in (10, 20, 30):
                    clock[0] = start + elapsed
                    value.scheduler.step(clock[0])
                    wait_until(lambda: all(processed(feed, elapsed // 10 + 1) for feed in FEED_NAMES[:2]))
                wait_until(lambda: processed("alerts", 2))
                state = value.scheduler.snapshot(clock[0])
                self.assertEqual([4, 4, 2], [state[feed]["total_submitted"] for feed in FEED_NAMES])
                self.assertEqual([0, 0, 0], [state[feed]["total_missed_deadlines"] for feed in FEED_NAMES])
                self.assertTrue(old.exists(), "writer has not consumed its durable source")
                self.assertFalse(release.is_set())
                self.assertEqual(4, len(self.records()), "membership evidence persists while commits are blocked")
                for feed in FEED_NAMES:
                    events = [json.loads(line) for line in poll_journal_path(self.data, feed, self.date).read_text().splitlines()]
                    self.assertEqual(4 if feed != "alerts" else 2, len(events))
                    self.assertTrue(all(event["spool_ok"] for event in events))
            finally:
                collector_module._shutdown_requested = True
                release.set()
                thread.join(10)
                if value.commit_worker:
                    value.commit_worker.stop(3)
            self.assertFalse(thread.is_alive())
            self.assertEqual([], failures)
        self.assertTrue(writes[0].is_file())
        self.assertGreaterEqual(parquet_row_count(writes[0]), 1)
        self.assertEqual(2, value.config.prediction_time_change_threshold_seconds)
        self.assertEqual({"vehiclepositions": 10., "tripupdates": 10., "alerts": 30.}, value.config.feed_intervals)

    def test_failed_background_commit_keeps_segments_and_recovers_after_restart(self):
        spool = DurableParquetSpool(self.data, flush_seconds=300)
        source = spool.stage("alerts", self.date, "poll", [{"entity_id": "recover"}])
        worker = ParquetCommitWorker(spool, logging.getLogger("test-parquet"))
        with patch("parquet_store.write_parquet_atomic", side_effect=OSError("disk full")):
            worker.start()
            try:
                wait_until(lambda: bool(worker.snapshot()["errors"]))
                self.assertTrue(source.exists())
                self.assertTrue(worker.snapshot()["alive"])
            finally:
                self.assertTrue(worker.stop(2))
        restarted = DurableParquetSpool(self.data, flush_seconds=300)
        result = restarted.flush(force=True)
        self.assertTrue(result.ok, result.errors)
        self.assertEqual(1, sum(parquet_row_count(path) for path in result.files_written))
        self.assertEqual([], restarted.pending_segments())

    def test_shutdown_stops_between_batches_not_before_durable_publication(self):
        spool = DurableParquetSpool(self.data, flush_seconds=0)
        for index in range(MAX_SEGMENTS_PER_COMMIT + 1):
            spool.stage("alerts", self.date, str(index), [{"entity_id": str(index)}])
        checks = iter((False, True))
        result = spool.flush(force=True, should_stop=lambda: next(checks))
        self.assertEqual(MAX_SEGMENTS_PER_COMMIT, result.rows_written)
        self.assertEqual(1, len(spool.pending_segments()))
        restarted = DurableParquetSpool(self.data, flush_seconds=0)
        remaining = restarted.flush(force=True)
        self.assertEqual(1, remaining.rows_written)
        files = list((self.data / "parquet" / "alerts" / f"date={self.date}").glob("*.parquet"))
        self.assertEqual(MAX_SEGMENTS_PER_COMMIT + 1, sum(map(parquet_row_count, files)))

    def test_worker_health_reports_failed_dead_and_stuck_writes(self):
        value = self.collector()
        for feed in FEED_NAMES:
            value.process_result(response(feed, sample(feed, int(self.now)), feed, self.now), [])
        for alive, duration, errors, reason in ((False, 0, [], "parquet_worker_stopped"),
                                                (True, 601, [], "parquet_worker_stuck"),
                                                (True, 0, ["failed write"], "parquet_flush_failed")):
            with self.subTest(reason=reason):
                worker = Mock(snapshot=lambda: {"alive": alive, "active_seconds": duration, "errors": errors})
                value.commit_worker = worker
                status = value._commit_and_write_status("status", [])
                self.assertFalse(status["healthy"])
                self.assertIn(reason, status["reasons"])


class PresenceTests(ReadinessFixture):
    def test_changed_shapes_preserve_raw_without_changing_rows_presence_or_normal_cadence(self):
        value = self.collector()
        feed = sample("tripupdates", int(self.now))
        for index in range(3):
            shape = feed.entity.add(id=f"shape-{index}").shape
            shape.shape_id = f"detour-{index}"
            shape.encoded_polyline = "_p~iF~ps|U_ulLnnqC_mqNvxq`@"
        value.process_result(response("tripupdates", feed, "first", self.now), [])
        feed.entity[-1].shape.encoded_polyline += "??"
        value.process_result(response("tripupdates", feed, "second", self.now + 10), [])

        events = [json.loads(line) for line in poll_journal_path(self.data, "tripupdates", self.date).read_text().splitlines()]
        self.assertTrue(all(event["parse_ok"] and event["presence_ok"] and event["spool_ok"] for event in events))
        self.assertEqual([2, 2], [event["parsed_rows"] for event in events])
        self.assertEqual([2, 0], [event["emitted_rows"] for event in events])
        records = self.records()
        self.assertEqual([1, 1], [record["trip_count"] for record in records])
        self.assertEqual([2, 2], [record["stop_count"] for record in records])
        self.assertEqual([], records[-1]["changes"])
        raw = self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog"
        with raw.open("rb") as handle:
            snapshots = list(iter_records(handle))
        self.assertEqual(2, len(snapshots))
        archived = pb.FeedMessage.FromString(snapshots[0][1])
        self.assertEqual(3, sum(entity.HasField("shape") for entity in archived.entity))
        self.assertEqual(feed.SerializeToString(), snapshots[-1][1])
        self.assertEqual([], value.monitor.feeds["tripupdates"]["freshness_flags"])
        result = value.spool.flush(force=True)
        self.assertTrue(result.ok, result.errors)
        self.assertEqual(2, sum(parquet_row_count(path) for path in result.files_written))
        self.assertEqual(300, value.config.tripupdates_raw_archive_seconds)
        self.assertEqual(2, value.config.prediction_time_change_threshold_seconds)

    def test_auxiliary_entities_are_supported_across_all_primary_feeds(self):
        for feed_name in FEED_NAMES:
            with self.subTest(feed=feed_name):
                value = self.collector()
                feed = sample(feed_name, int(self.now))
                shape = feed.entity.add(id="shape").shape
                shape.shape_id, shape.encoded_polyline = "detour", "??"
                stop = feed.entity.add(id="dynamic-stop").stop
                stop.stop_id = "new-stop"
                feed.entity.add(id="modifications").trip_modifications.SetInParent()
                value.process_result(response(feed_name, feed, feed_name, self.now), [])
                event = json.loads(poll_journal_path(self.data, feed_name, self.date).read_text().splitlines()[-1])
                self.assertTrue(event["parse_ok"], event["parse_error"])
                self.assertTrue(event["spool_ok"])
                self.assertEqual(2 if feed_name == "tripupdates" else 1, event["parsed_rows"])
                if feed_name == "tripupdates":
                    self.assertTrue(event["presence_ok"])
                    self.assertEqual(1, self.records()[-1]["trip_count"])

    def test_auxiliary_only_full_feed_confirms_no_trip_updates(self):
        value = self.collector()
        value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "first", self.now), [])
        empty = pb.FeedMessage()
        empty.header.gtfs_realtime_version = "2.0"
        empty.header.timestamp = int(self.now + 10)
        shape = empty.entity.add(id="shape").shape
        shape.shape_id, shape.encoded_polyline = "detour", "??"
        value.process_result(response("tripupdates", empty, "second", self.now + 10), [])
        record = self.records()[-1]
        self.assertEqual(0, record["trip_count"])
        self.assertEqual(0, record["stop_count"])
        self.assertTrue(record["changes"][0]["trip_withdrawn"])
        event = json.loads(poll_journal_path(self.data, "tripupdates", self.date).read_text().splitlines()[-1])
        self.assertTrue(event["parse_ok"] and event["presence_ok"])
        self.assertEqual(0, event["parsed_rows"])

    def test_invalid_primary_entities_force_raw_without_false_withdrawals(self):
        for invalid in ("empty", "wrong-primary", "ambiguous"):
            with self.subTest(payload=invalid):
                value = self.collector()
                value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), invalid + "-first", self.now), [])
                records_before = len(self.records())
                segments_before = len(value.spool.pending_segments())
                bad = sample("tripupdates", int(self.now))
                if invalid == "empty":
                    bad.entity.add(id="empty")
                elif invalid == "wrong-primary":
                    wrong = bad.entity.add(id="wrong").vehicle
                    wrong.position.latitude, wrong.position.longitude = 47.5, 19.0
                else:
                    shape = bad.entity[0].shape
                    shape.shape_id, shape.encoded_polyline = "detour", "??"
                value.process_result(response("tripupdates", bad, invalid + "-bad", self.now + 10), [])
                event = json.loads(poll_journal_path(self.data, "tripupdates", self.date).read_text().splitlines()[-1])
                self.assertFalse(event["parse_ok"])
                self.assertFalse(event["presence_ok"])
                self.assertTrue(event["raw_archived"] and event["raw_ok"])
                self.assertEqual(0, event["emitted_rows"])
                self.assertEqual(records_before, len(self.records()))
                self.assertEqual(segments_before, len(value.spool.pending_segments()))
                self.assertIn("protobuf_parse_failed", value.monitor.feeds["tripupdates"]["freshness_flags"])
                value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), invalid + "-recovered", self.now + 20), [])
                self.assertEqual("baseline", self.records()[-1]["kind"])
                self.assertEqual("parse_observation_gap", self.records()[-1]["baseline_reason"])

    def test_withdrawal_and_reappearance_survive_even_when_prediction_rows_are_suppressed(self):
        value = self.collector()
        for index, sequences in enumerate(((1, 2), (1,), (1, 2))):
            value.process_result(response("tripupdates", sample("tripupdates", int(self.now), sequences),
                                          str(index), self.now + index * 10), [])
        events = [json.loads(line) for line in poll_journal_path(self.data, "tripupdates", self.date).read_text().splitlines()]
        self.assertEqual([2, 0, 0], [event["emitted_rows"] for event in events])
        records = self.records()
        self.assertEqual(1, len(records[1]["changes"][0]["withdrawn_stops"]))
        self.assertEqual(1, len(records[2]["changes"][0]["entered_stops"]))
        self.assertEqual(1, scan_raw_log(self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog").complete_records)

    def test_baseline_empty_delta_withdrawal_and_unchanged_reappearance_replay(self):
        value = TripUpdatePresence(self.data, "run")
        rows = self.rows()
        value.observe(rows, self.date, self.now, self.context("one"))
        value.observe(rows, self.date, self.now + 10, self.context("two"))
        value.observe(rows[:1], self.date, self.now + 20, self.context("three"))
        value.observe(rows, self.date, self.now + 30, self.context("four"))
        value.observe([], self.date, self.now + 40, self.context("five"))
        records = self.records()
        self.assertEqual(["baseline", "delta", "delta", "delta", "delta"], [r["kind"] for r in records])
        self.assertEqual([], records[1]["changes"])
        self.assertEqual(2, records[0]["stop_count"], "repeated stop IDs remain distinct visits")
        self.assertEqual(1, len(records[2]["changes"][0]["withdrawn_stops"]))
        self.assertEqual(1, len(records[3]["changes"][0]["entered_stops"]))
        self.assertTrue(records[4]["changes"][0]["trip_withdrawn"])
        members = {}
        for record in records:
            members = apply_presence_record(members, record)
        self.assertEqual({}, members)

    def test_trip_only_entity_is_present_without_invented_stop(self):
        value = TripUpdatePresence(self.data, "run")
        value.observe(self.rows(()), self.date, self.now, self.context("trip-only"))
        record = self.records()[0]
        self.assertEqual((1, 0), (record["trip_count"], record["stop_count"]))
        self.assertEqual([], record["changes"][0]["entered_stops"])
        self.assertEqual(1, len(apply_presence_record({}, record)))

    def test_failure_midnight_and_process_restart_are_baselines_not_withdrawals(self):
        value = TripUpdatePresence(self.data, "run-one")
        value.observe(self.rows(), self.date, self.now, self.context("one"))
        value.invalidate("http_observation_gap")
        value.observe([], self.date, self.now + 10, self.context("after-outage"))
        self.assertEqual("http_observation_gap", self.records()[-1]["baseline_reason"])
        self.assertEqual([], self.records()[-1]["changes"], "unobserved trips must not be called withdrawals")
        tomorrow = (datetime.fromtimestamp(self.now, timezone.utc).date() + timedelta(days=1)).isoformat()
        value.observe(self.rows(), tomorrow, self.now + 86400, self.context("midnight"))
        self.assertEqual("utc_date_boundary", self.records(tomorrow)[0]["baseline_reason"])
        restarted = TripUpdatePresence(self.data, "run-two")
        restarted.observe(self.rows(), tomorrow, self.now + 86410, self.context("restart"))
        records = self.records(tomorrow)
        self.assertEqual("process_start", records[-1]["baseline_reason"])
        self.assertEqual(1, records[-1]["sequence"])
        self.assertNotEqual(records[0]["stream_id"], records[-1]["stream_id"])

    def test_failed_presence_append_never_advances_delta_state(self):
        value = TripUpdatePresence(self.data, "run")
        value.observe(self.rows(), self.date, self.now, self.context("one"))
        with patch("tripupdate_presence.append_record", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                value.observe([], self.date, self.now + 10, self.context("lost"))
        value.observe(self.rows(), self.date, self.now + 20, self.context("recovered"))
        self.assertEqual(2, len(self.records()))
        self.assertEqual("baseline", self.records()[-1]["kind"])
        self.assertEqual("presence_write_gap", self.records()[-1]["baseline_reason"])

    def test_presence_failure_forces_raw_between_normal_snapshots_and_unhealthy(self):
        value = self.collector()
        value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "first", self.now), [])
        with patch("tripupdate_presence.append_record", side_effect=OSError("disk full")):
            value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "second", self.now + 10), [])
        raw = self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog"
        self.assertEqual(2, scan_raw_log(raw).complete_records)
        self.assertIn("tripupdates_presence_failed", value.monitor.feeds["tripupdates"]["freshness_flags"])
        events = [json.loads(line) for line in poll_journal_path(self.data, "tripupdates", self.date).read_text().splitlines()]
        self.assertFalse(events[-1]["presence_ok"])
        self.assertTrue(events[-1]["raw_archived"])
        self.assertEqual(300, events[-1]["raw_archive_interval_seconds"])
        stats, _errors, _quality = _feed_stats(self.data, "tripupdates", self.date, create_empty=False)
        self.assertEqual(1, stats["failed_processing_polls_with_raw_evidence"])
        value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "third", self.now + 20), [])
        self.assertEqual("presence_write_gap", self.records()[-1]["baseline_reason"])

    def test_http_and_parse_failures_invalidate_without_false_withdrawals(self):
        for error in ("http", "parse"):
            with self.subTest(error=error):
                value = self.collector()
                value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), error + "-one", self.now), [])
                bad = response("tripupdates", None, error + "-bad", self.now + 10)
                if error == "parse":
                    bad = response("tripupdates", sample("vehiclepositions", int(self.now)), error + "-bad", self.now + 10)
                value.process_result(bad, [])
                value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), error + "-three", self.now + 20), [])
                self.assertEqual("baseline", self.records()[-1]["kind"])
                self.assertEqual(error + "_observation_gap", self.records()[-1]["baseline_reason"])

    def test_differential_feed_is_not_interpreted_as_full_membership(self):
        value = self.collector()
        value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "full", self.now), [])
        differential = sample("tripupdates", int(self.now), ())
        differential.header.incrementality = pb.FeedHeader.DIFFERENTIAL
        value.process_result(response("tripupdates", differential, "partial", self.now + 10), [])
        self.assertEqual(1, len(self.records()))
        raw = self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog"
        self.assertEqual(2, scan_raw_log(raw).complete_records)

    def test_manifest_refuses_missing_or_corrupt_promised_presence(self):
        value = TripUpdatePresence(self.data, "run")
        details = value.observe(self.rows(), self.date, self.now, self.context("one"))
        event = {"poll_id": "one", "presence_required": True, "presence_ok": True, "parse_ok": True, **details}
        stats, errors, quality = _presence_stats(self.data, self.date, [event])
        self.assertEqual([], errors)
        self.assertEqual([], quality)
        self.assertEqual(1, stats["journal_confirmed_observations"])
        path = presence_path(self.data, self.date)
        original = path.read_bytes()
        path.write_bytes(original[:-1])
        self.assertTrue(_presence_stats(self.data, self.date, [event])[1])
        path.unlink()
        self.assertTrue(_presence_stats(self.data, self.date, [event])[1])

    def test_manifest_reports_legacy_without_inventing_presence(self):
        stats, errors, quality = _presence_stats(self.data, self.date, [{"poll_id": "old", "parse_ok": True}])
        self.assertEqual([], errors)
        self.assertEqual([], quality)
        self.assertEqual(1, stats["legacy_pre_presence_polls"])
        self.assertEqual(0, stats["records"])

    def test_manifest_flags_mixed_version_presence_without_calling_http_failures_legacy(self):
        value = TripUpdatePresence(self.data, "run")
        details = value.observe(self.rows(), self.date, self.now, self.context("new"))
        events = [{"poll_id": "old", "parse_ok": True},
                  {"poll_id": "http-failed", "parse_ok": False},
                  {"poll_id": "new", "parse_ok": True, "presence_required": True, "presence_ok": True, **details}]
        stats, errors, quality = _presence_stats(self.data, self.date, events)
        self.assertEqual([], errors)
        self.assertEqual(1, stats["legacy_pre_presence_polls"])
        self.assertTrue(any("mixed-version" in flag for flag in quality))


class ForensicDurabilityTests(ReadinessFixture):
    def raw_fixture(self, name):
        path = self.data / name
        append_record(path, self.now, b'{"poll_id":"first"}')
        prefix = path.read_bytes()
        later = self.data / ("later-" + name)
        append_record(later, self.now + 10, b'{"poll_id":"later-valid-record"}')
        # Preserve valid later bytes even when an invalid header precedes them.
        tail = struct.pack(">dI", self.now, MAX_COMPRESSED_RECORD_BYTES + 1) + later.read_bytes()
        with path.open("ab") as handle:
            handle.write(tail)
        return path, prefix, tail

    def test_raw_and_presence_tail_entry_is_synced_before_source_truncation(self):
        for name in ("feed.rawlog", "presence.jsonlog"):
            with self.subTest(name=name):
                path, prefix, tail = self.raw_fixture(name)

                def sync(directory, *, strict=False):
                    self.assertTrue(strict)
                    self.assertEqual(prefix + tail, path.read_bytes())
                    copies = list(path.parent.glob(path.name + ".corrupt-tail-*"))
                    self.assertEqual(1, len(copies))
                    self.assertEqual(tail, copies[0].read_bytes())
                    fsync_directory(directory, strict=strict)

                with patch("raw_log.fsync_directory", side_effect=sync) as directory_sync:
                    recovery = repair_truncated_tail(path)
                directory_sync.assert_called_once_with(path.parent, strict=True)
                self.assertEqual(prefix, path.read_bytes())
                self.assertEqual(tail, recovery.read_bytes())
                self.assertTrue(scan_raw_log(path).clean)

    def test_raw_and_presence_sync_failure_leaves_original_bytes_and_checkpoint(self):
        for name in ("feed.rawlog", "presence.jsonlog"):
            with self.subTest(name=name):
                path, prefix, tail = self.raw_fixture(name)
                checkpoint = path.with_name(path.name + ".checkpoint.json")
                original_checkpoint = checkpoint.read_bytes()
                with patch("raw_log.fsync_directory", side_effect=OSError("directory sync failed")):
                    with self.assertRaisesRegex(OSError, "directory sync failed"):
                        repair_truncated_tail(path)
                self.assertEqual(prefix + tail, path.read_bytes())
                self.assertEqual(original_checkpoint, checkpoint.read_bytes())
                copies = list(path.parent.glob(path.name + ".corrupt-tail-*"))
                self.assertEqual(1, len(copies))
                self.assertEqual(tail, copies[0].read_bytes())
                # A later restart/retry can finish safely without removing the
                # first forensic copy or changing any valid record's bytes.
                recovery = repair_truncated_tail(path)
                self.assertEqual(prefix, path.read_bytes())
                self.assertEqual(tail, recovery.read_bytes())
                self.assertEqual(2, len(list(path.parent.glob(path.name + ".corrupt-tail-*"))))

    def test_poll_tail_entry_is_synced_before_source_truncation(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "first"})
        prefix = path.read_bytes()
        tail = b'{"poll_id":"unfinished"'
        with path.open("ab") as handle:
            handle.write(tail)

        def sync(directory, *, strict=False):
            self.assertTrue(strict)
            self.assertEqual(prefix + tail, path.read_bytes())
            copies = list(path.parent.glob(path.name + ".corrupt-tail-*"))
            self.assertEqual(1, len(copies))
            self.assertEqual(tail, copies[0].read_bytes())
            fsync_directory(directory, strict=strict)

        with patch("poll_journal.fsync_directory", side_effect=sync) as directory_sync:
            recovery = repair_jsonl_tail(path)
        directory_sync.assert_called_once_with(path.parent, strict=True)
        self.assertEqual(prefix, path.read_bytes())
        self.assertEqual(tail, recovery.read_bytes())

    def test_poll_tail_sync_failure_leaves_original_bytes_and_checkpoint(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "first"})
        prefix = path.read_bytes()
        checkpoint = path.with_name(path.name + ".checkpoint.json")
        original_checkpoint = checkpoint.read_bytes()
        tail = b'{"poll_id":"unfinished"'
        with path.open("ab") as handle:
            handle.write(tail)
        with patch("poll_journal.fsync_directory", side_effect=OSError("directory sync failed")):
            with self.assertRaisesRegex(OSError, "directory sync failed"):
                repair_jsonl_tail(path)
        self.assertEqual(prefix + tail, path.read_bytes())
        self.assertEqual(original_checkpoint, checkpoint.read_bytes())
        copies = list(path.parent.glob(path.name + ".corrupt-tail-*"))
        self.assertEqual(1, len(copies))
        self.assertEqual(tail, copies[0].read_bytes())
        recovery = repair_jsonl_tail(path)
        self.assertEqual(prefix, path.read_bytes())
        self.assertEqual(tail, recovery.read_bytes())
        self.assertEqual(2, len(list(path.parent.glob(path.name + ".corrupt-tail-*"))))

    def test_directory_sync_strict_failures_propagate_and_close_descriptor(self):
        with patch("atomic_io.os.open", side_effect=PermissionError("directory unavailable")):
            self.assertIsNone(fsync_directory(self.data))
            with self.assertRaises(PermissionError):
                fsync_directory(self.data, strict=True)
        with patch("atomic_io.os.open", return_value=123), \
                patch("atomic_io.os.fsync", side_effect=OSError("sync unsupported")), \
                patch("atomic_io.os.close") as close:
            self.assertIsNone(fsync_directory(self.data))
            with self.assertRaisesRegex(OSError, "sync unsupported"):
                fsync_directory(self.data, strict=True)
            self.assertEqual(2, close.call_count)
            close.assert_called_with(123)


class JournalRecoveryTests(ReadinessFixture):
    def test_journal_failure_stays_unhealthy_until_same_feed_append_recovers(self):
        value = self.collector()
        for feed in FEED_NAMES:
            value.process_result(response(feed, sample(feed, int(self.now)), feed, self.now), [])
        with patch("collector.append_poll_event", side_effect=OSError("journal unavailable")):
            value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "missing", self.now + 10), [])
        value.process_result(response("vehiclepositions", sample("vehiclepositions", int(self.now)), "other-feed", self.now + 11), [])
        status = value._commit_and_write_status("status", [])
        self.assertFalse(status["healthy"])
        self.assertIn("tripupdates:poll_journal_failed", status["reasons"])
        raw = self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog"
        self.assertEqual(2, scan_raw_log(raw).complete_records, "journal loss also forces raw fallback")
        value.process_result(response("tripupdates", sample("tripupdates", int(self.now)), "recovered", self.now + 20), [])
        status = value._commit_and_write_status("recovered-status", [])
        self.assertTrue(status["healthy"], status["reasons"])

    def test_torn_final_line_is_detached_and_all_valid_records_survive(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "one"})
        original = path.read_bytes()
        torn = b'{"poll_id":"two","text":"\xf0\x9f'
        with path.open("ab") as handle:
            handle.write(torn)
        recovered = repair_jsonl_tail(path)
        self.assertEqual(torn, recovered.read_bytes())
        self.assertEqual(original, path.read_bytes())
        append_poll_jsonl(path, {"poll_id": "three"})
        self.assertEqual(["one", "three"], [json.loads(line)["poll_id"] for line in path.read_text().splitlines()])
        self.assertIsNone(repair_jsonl_tail(path))

    def test_legacy_complete_json_without_separator_is_preserved(self):
        path = self.data / "polls.jsonl"
        path.write_bytes(b'{"poll_id":"one"}\n{"poll_id":"two"}')
        self.assertIsNone(repair_jsonl_tail(path))
        append_poll_jsonl(path, {"poll_id": "three"})
        self.assertEqual(["one", "two", "three"], [json.loads(line)["poll_id"] for line in path.read_text().splitlines()])

    def test_checkpoint_failure_preserves_complete_append(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "one"})
        with patch("poll_journal.atomic_write_json", side_effect=OSError("checkpoint full")):
            with self.assertRaises(OSError):
                append_poll_jsonl(path, {"poll_id": "two"})
        append_poll_jsonl(path, {"poll_id": "three"})
        self.assertEqual(["one", "two", "three"], [json.loads(line)["poll_id"] for line in path.read_text().splitlines()])

    def test_interior_corruption_is_not_destructively_repaired(self):
        path = self.data / "polls.jsonl"
        append_poll_jsonl(path, {"poll_id": "one"})
        with path.open("ab") as handle:
            handle.write(b'bad interior line\n{"poll_id":"three"}\n')
        original = path.read_bytes()
        with self.assertRaisesRegex(ValueError, "interior corruption"):
            repair_jsonl_tail(path)
        self.assertEqual(original, path.read_bytes())
        self.assertEqual([], list(self.data.glob("*.corrupt-tail-*")))

    def test_restart_after_midnight_repairs_today_and_latest_prior_only(self):
        value = self.collector()
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        older = (datetime.now(timezone.utc).date() - timedelta(days=2)).isoformat()
        for feed in FEED_NAMES:
            for date in (older, yesterday, self.date):
                path = poll_journal_path(self.data, feed, date)
                path.parent.mkdir(parents=True)
                path.write_bytes(b'{"poll_id":"valid"}\n{"torn":')
        value.recover_recent_journal_tails()
        for feed in FEED_NAMES:
            root = self.data / "metadata" / "polls" / feed
            self.assertEqual([yesterday, self.date], [p.parent.name[5:] for p in recent_partition_files(root, "polls.jsonl", self.date)])
            for date in (yesterday, self.date):
                path = poll_journal_path(self.data, feed, date)
                self.assertEqual(b'{"poll_id":"valid"}\n', path.read_bytes())
                self.assertEqual(1, len(list(path.parent.glob("*.corrupt-tail-*"))))
            self.assertTrue(poll_journal_path(self.data, feed, older).read_bytes().endswith(b'{"torn":'))
        self.assertEqual([], recent_partition_files(self.data / "missing", "polls.jsonl", self.date))


class RetryRedactionTests(ReadinessFixture):
    def test_actual_urllib3_connection_retry_warning_does_not_leak_key(self):
        canary = "not-a-real-BKK-key-CANARY"
        session = _new_session(CollectorConfig(api_key=canary, data_dir=self.data, http_connect_retries=1))
        session.trust_env = False
        self.addCleanup(session.close)
        captured = io.StringIO()
        handler = logging.StreamHandler(captured)
        logger = logging.getLogger("urllib3.connectionpool")
        logger.addHandler(handler)
        self.addCleanup(lambda: logger.removeHandler(handler))
        with patch("urllib3.connectionpool.HTTPConnectionPool._make_request", side_effect=NewConnectionError(None, "offline")):
            with self.assertRaises(Exception):
                session.get("https://example.invalid/feed?key=" + canary, timeout=(.1, .1))
        text = captured.getvalue()
        self.assertIn("Retrying", text, "exercise the library's real warning path")
        self.assertIn("key=<redacted>", text)
        self.assertNotIn(canary, text)

    def test_debug_and_exception_logging_are_also_redacted(self):
        canary = "not-a-real-key-DEBUG-CANARY"
        session = _new_session(CollectorConfig(api_key=canary, data_dir=self.data))
        self.addCleanup(session.close)
        logger = logging.getLogger("urllib3.connectionpool")
        previous_level = logger.level
        logger.setLevel(logging.DEBUG)
        self.addCleanup(lambda: logger.setLevel(previous_level))
        with self.assertLogs(logger, logging.DEBUG) as captured:
            logger.debug("request %s", "/feed?key=" + canary)
            try:
                raise IOError("failed /feed?key=" + canary)
            except IOError:
                logger.exception("failed request")
        self.assertNotIn(canary, "\n".join(captured.output))
        self.assertIn("<redacted>", "\n".join(captured.output))


class RebuildSafetyTests(ReadinessFixture):
    def setUp(self):
        super().setUp()
        self.date = "2026-09-24"
        for name, value in (("DATA_DIR", self.data), ("RAW_DIR", self.data / "raw"), ("PARQUET_DIR", self.data / "parquet")):
            patcher = patch.object(rebuild_parquet, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.raw = self.data / "raw" / "vehiclepositions" / f"date={self.date}" / "vehiclepositions.rawlog"
        for index in range(3):
            append_record(self.raw, self.now + index * 10, sample("vehiclepositions", int(self.now)).SerializeToString())
        self.original = self.data / "parquet" / "vehiclepositions" / f"date={self.date}" / "part-original.parquet"
        write_parquet_atomic("vehiclepositions", [{"entity_id": "original"}], self.original)

    def test_write_error_cannot_be_dismissed_as_parse_error_or_replace_original(self):
        raw_hash, original_hash = sha256_file(self.raw), sha256_file(self.original)
        real_write = write_parquet_atomic
        for allow in (False, True):
            calls = []

            def fail_late(feed, rows, path):
                calls.append(path)
                if len(calls) == 2:
                    raise OSError("disk full on second output")
                return real_write(feed, rows, path)

            with self.subTest(allow_parse_errors=allow), patch("rebuild_parquet.write_parquet_atomic", side_effect=fail_late):
                with self.assertRaisesRegex(OSError, "second output"):
                    rebuild_parquet.rebuild_one("vehiclepositions", self.date, chunk_rows=1, allow_parse_errors=allow)
            self.assertEqual(raw_hash, sha256_file(self.raw))
            self.assertEqual(original_hash, sha256_file(self.original))
            self.assertEqual([], list((self.data / "parquet" / ".rebuild-staging").glob("*/date=*")))

    def test_explicitly_allowed_genuine_parse_error_still_preserves_previous_partition(self):
        append_record(self.raw, self.now + 30, b"not valid protobuf")
        with self.assertRaises(RuntimeError):
            rebuild_parquet.rebuild_one("vehiclepositions", self.date, chunk_rows=1)
        result = rebuild_parquet.rebuild_one("vehiclepositions", self.date, chunk_rows=1, allow_parse_errors=True)
        self.assertEqual(1, result["parse_failures"])
        self.assertEqual(3, result["rows"])
        previous = Path(result["previous_partition"])
        self.assertTrue((previous / "part-original.parquet").is_file())
        self.assertEqual(3, sum(map(parquet_row_count, self.original.parent.glob("*.parquet"))))

    def test_cli_returns_failure_and_continues_remaining_dates(self):
        result = {"raw_records": 1, "rows": 1, "previous_partition": None}
        with patch("sys.argv", ["rebuild_parquet.py", "--feed", "vehiclepositions"]), \
                patch("rebuild_parquet.all_dates_for", return_value=["2026-09-23", "2026-09-24"]), \
                patch("rebuild_parquet.rebuild_one", side_effect=[OSError("disk full"), result]) as run, \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(1, rebuild_parquet.main())
            self.assertEqual(2, run.call_count)

    def test_cli_default_skips_current_and_future_dates(self):
        tomorrow = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
        with patch("sys.argv", ["rebuild_parquet.py", "--feed", "vehiclepositions"]), \
                patch("rebuild_parquet.all_dates_for", return_value=[self.date, utc_iso()[:10], tomorrow]), \
                patch("rebuild_parquet.rebuild_one", return_value={"raw_records": 1, "rows": 1, "previous_partition": None}) as run, \
                patch("sys.stdout", io.StringIO()):
            self.assertEqual(0, rebuild_parquet.main())
            self.assertEqual(1, run.call_count)
            self.assertEqual(self.date, run.call_args.args[1])


class CoverageAndRestoreTests(ReadinessFixture):
    def test_new_presence_and_run_metadata_are_in_verified_backup_receipt(self):
        self.date = "2026-09-24"
        self.now = datetime.strptime(self.date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() + 3600
        value = self.collector()
        for feed in FEED_NAMES:
            value.process_result(response(feed, sample(feed, int(self.now)), feed, self.now), [])
        self.assertTrue(value.spool.flush(force=True).ok)
        static = StaticGtfsStore(self.data, check_interval_seconds=86400, retry_seconds=1)
        static.check(StaticSession([gtfs_zip_bytes("readiness")]), now_ts=self.now - 7200)
        manifest = build_daily_manifest(self.data, self.date, static)
        self.assertTrue(manifest["complete"], manifest["completeness_errors"])
        api = FakeBackupApi(self.data)
        manager = BackupManager(self.data, "owner/archive", "fake-token", static, api=api, logger=Mock())
        result = manager.backup_date(self.date)
        self.assertTrue(result.success, result.reason)
        receipt = read_json(self.data / "backup_receipts" / f"date={self.date}.json", {})
        self.assertTrue(receipt["remote_verified"])
        manifest_path = self.data / "metadata" / "manifests" / f"date={self.date}.json"
        self.assertEqual(receipt["manifest_sha256"], sha256_file(manifest_path))
        self.assertEqual(1, read_json(manifest_path, {})["feeds"]["tripupdates"]["presence"]["records"])
        names = {artifact["path"] for artifact in receipt["artifacts"]}
        self.assertIn(str(presence_path(self.data, self.date).relative_to(self.data)), names)
        self.assertIn(f"metadata/collector_runs/{value.run_id}.json", names)
        self.assertTrue(any(path.endswith("polls.jsonl.checkpoint.json") for path in names))
        for artifact in receipt["artifacts"]:
            self.assertEqual((artifact["size"], artifact["sha256"]), api.remote[artifact["path"]])
        run_metadata = read_json(self.data / "metadata" / "collector_runs" / f"{value.run_id}.json", {})
        self.assertEqual(value.config.feed_intervals, run_metadata["poll_intervals_seconds"])
        self.assertEqual(value.config.trip_update_numeric_tolerances, run_metadata["numeric_tolerances"])
        self.assertNotIn(value.config.api_key, json.dumps(run_metadata))
        missing_presence = dict(manifest, artifacts=[artifact for artifact in manifest["artifacts"]
                                                     if artifact["kind"] != "tripupdates_presence"])
        with self.assertRaisesRegex(ValueError, "presence evidence"):
            BackupManager._validate_manifest(self.date, missing_presence)

    def test_collection_window_reads_across_midnight_and_separates_failures(self):
        midnight = datetime(2026, 9, 25, tzinfo=timezone.utc).timestamp()
        for index, (success, parse_ok, spool_ok) in enumerate(((True, True, True), (False, False, False),
                                                              (True, False, False), (True, True, False),
                                                              (True, True, True))):
            timestamp = midnight - 10 + index * 10
            date = utc_iso(timestamp)[:10]
            append_poll_event(self.data, "vehiclepositions", date, {
                "poll_id": str(index), "response_received_at": utc_iso(timestamp),
                "poll_interval_seconds": 10, "success": success, "parse_ok": parse_ok, "spool_ok": spool_ok,
                "missed_deadlines_before_request": 2 if index == 4 else 0, "processing_latency_ms": index,
            })
        before = {str(path.relative_to(self.data)): sha256_file(path) for path in self.data.rglob("*") if path.is_file()}
        with patch("requests.Session.get", side_effect=AssertionError("offline diagnostic must not contact BKK")):
            report = collection_window_report(self.data, midnight - 10, midnight + 40)
        stats = report["feeds"]["vehiclepositions"]
        self.assertEqual((5, 2, 1, 1, 1), tuple(stats[key] for key in (
            "journalled_polls", "successful_data_polls", "http_failures", "parse_failures", "storage_tracking_evidence_failures")))
        self.assertEqual(2, stats["scheduler_missed_deadlines_reported"])
        self.assertEqual(40, stats["max_successful_observation_gap_seconds"])
        self.assertEqual(.4, stats["successful_data_poll_count_ratio"])
        self.assertEqual(0, report["feeds"]["tripupdates"]["journalled_polls"])
        self.assertEqual(before, {str(path.relative_to(self.data)): sha256_file(path) for path in self.data.rglob("*") if path.is_file()})

    def test_collection_window_requires_explicit_timezone(self):
        with patch("sys.stderr", io.StringIO()):
            self.assertEqual(1, diagnostics_main(["collection-window", "--since", "2026-09-24T08:00:00",
                                                 "--data-dir", str(self.data)]))

    def test_small_poll_loss_is_visible_without_inventing_http_failures(self):
        date = "2026-09-24"
        start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
        # An otherwise successful day missing just one internal deadline.
        events = []
        for index in range(2880):
            if index == 100:
                continue
            events.append({
                "poll_id": str(index), "success": True, "parse_ok": True,
                "poll_interval_seconds": 30, "response_received_at": utc_iso(start + index * 30),
                "missed_deadlines_before_request": 1 if index == 101 else 0,
            })
        path = poll_journal_path(self.data, "alerts", date)
        path.parent.mkdir(parents=True)
        path.write_text("".join(json.dumps(event) + "\n" for event in events))
        stats, _errors, quality = _feed_stats(self.data, "alerts", date, create_empty=False)
        self.assertEqual(0, stats["failed_http_polls"])
        self.assertEqual(1, stats["poll_count_shortfall"])
        self.assertEqual(1, stats["scheduler_missed_deadlines"])
        self.assertEqual(1, stats["successful_observation_gap_count"])
        self.assertEqual(60, stats["max_successful_observation_gap_seconds"])
        self.assertAlmostEqual(2879 / 2880, stats["successful_data_poll_count_ratio"])
        self.assertTrue(any("scheduler deadlines missed" in reason for reason in quality))

    def test_storage_failure_is_not_reported_as_successful_data_coverage(self):
        date = "2026-09-24"
        for index, stored in enumerate((True, False, True)):
            append_poll_event(self.data, "vehiclepositions", date, {
                "success": True, "parse_ok": True, "spool_ok": stored, "poll_id": str(index),
                "poll_interval_seconds": 10, "response_received_at": f"{date}T00:00:{index * 10:02d}+00:00",
            })
        stats, _errors, quality = _feed_stats(self.data, "vehiclepositions", date, create_empty=False)
        self.assertEqual(3, stats["successful_parse_polls"])
        self.assertEqual(2, stats["successful_data_polls"])
        self.assertEqual(0, stats["failed_http_polls"])
        self.assertEqual(20, stats["max_successful_observation_gap_seconds"])
        self.assertTrue(any("storage/tracking/evidence failures" in flag for flag in quality))

    def test_restore_samples_new_evidence_and_journal_not_just_checkpoints(self):
        date = "2026-09-24"
        kinds = ("raw", "parquet", "daily_manifest", "poll_metadata", "static_gtfs",
                 "static_gtfs_history", "static_gtfs_state", "tripupdates_presence", "collector_run_metadata")
        names = {"raw": "raw/f.rawlog", "poll_metadata": "metadata/polls/f/polls.jsonl",
                 "tripupdates_presence": "metadata/tripupdates_presence/presence.jsonlog"}
        artifacts = [{"path": names.get(kind, kind + ".json"), "kind": kind, "size": 100, "sha256": "a" * 64}
                     for kind in kinds]
        artifacts += [{"path": names[kind] + ".checkpoint.json", "kind": kind, "size": 1, "sha256": "a" * 64}
                      for kind in ("raw", "poll_metadata", "tripupdates_presence")]
        receipt_path = self.data / "backup_receipts" / f"date={date}.json"
        atomic_write_json(receipt_path, {"version": 2, "date": date, "repo_id": "owner/archive",
                                       "remote_verified": True, "artifacts": artifacts})
        manager = Mock(data_dir=self.data, repo_id="owner/archive")
        with patch("verify_backup.receipt_revision", return_value="b" * 40), \
                patch("verify_backup.restore_artifact", side_effect=lambda _m, artifact, _r, path:
                      (path.write_bytes(b"temporary"), {"path": artifact["path"], "result": "PASS"})[1]):
            result = verify_day(manager, date)
        restored = [entry["path"] for entry in result["restored"]]
        self.assertEqual(9, len(restored))
        self.assertIn(names["tripupdates_presence"], restored)
        self.assertIn(names["poll_metadata"], restored)
        self.assertFalse(any(path.endswith(".checkpoint.json") for path in restored))
        self.assertTrue(result["temporary_directory_removed"])


if __name__ == "__main__":
    unittest.main()
