from __future__ import annotations
import io
import json
import logging
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
from atomic_io import sha256_file
from collector import Collector, FetchResult, _new_session
from config import CollectorConfig, FEED_NAMES
from gtfs_rt_parse import parse_trip_updates
from monitoring import poll_journal_path, utc_iso
from parquet_store import DurableParquetSpool, MAX_SEGMENTS_PER_COMMIT, parquet_row_count, write_parquet_atomic
from parquet_worker import ParquetCommitWorker
from poll_journal import append_poll_jsonl, recent_partition_files, repair_jsonl_tail
from raw_log import append_record, scan_raw_log
from realtime_scheduler import IndependentFeedScheduler


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



if __name__ == "__main__":
    unittest.main()
