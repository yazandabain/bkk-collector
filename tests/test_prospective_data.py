from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import replace
from unittest.mock import patch

from bkk_collector.realtime.gtfs_rt_parse import parse_feed
from bkk_collector.storage.raw_log import iter_records
from tests.test_collector_readiness import ReadinessFixture, response, sample
from bkk_collector.realtime.tripupdate_presence import TripUpdatePresence, apply_presence_record, apply_source_timestamp_record, iter_presence, presence_path


class AuxiliaryPreservationTests(ReadinessFixture):
    def feed(self, shapes):
        feed = sample("tripupdates", int(self.now))
        for name, geometry in shapes:
            shape = feed.entity.add(id="shape-" + name).shape
            shape.shape_id, shape.encoded_polyline = name, geometry
        return feed

    def observe(self, collector, feed, index):
        with patch("bkk_collector.collector.time.monotonic", return_value=40 + index * 10):
            collector.process_result(response("tripupdates", feed, f"poll-{index}", self.now + index * 10), [])

    def raw(self):
        path = self.data / "raw" / "tripupdates" / f"date={self.date}" / "tripupdates.rawlog"
        with path.open("rb") as stream:
            return [payload for _, payload in iter_records(stream)]

    def events(self):
        path = self.data / "metadata" / "polls" / "tripupdates" / f"date={self.date}" / "polls.jsonl"
        return [json.loads(line) for line in path.read_text().splitlines()]

    def test_shape_addition_revision_and_withdrawal_force_exact_raw_between_snapshots(self):
        collector = self.collector()
        feeds = [self.feed([]), self.feed([("new", "geometry-1")]),
                 self.feed([("new", "geometry-2")]), self.feed([])]
        for i, feed in enumerate(feeds):
            self.observe(collector, feed, i)
        self.assertEqual([feed.SerializeToString() for feed in feeds], self.raw())
        self.assertTrue(all(e["trip_auxiliary_state_changed"] and e["raw_archived"] for e in self.events()))

    def test_unchanged_auxiliary_state_and_entity_order_do_not_force_raw(self):
        collector = self.collector()
        a = self.feed([("first", "one"), ("second", "two")])
        b = self.feed([("second", "two"), ("first", "one")])
        self.observe(collector, a, 0)
        self.observe(collector, b, 1)
        self.assertEqual([a.SerializeToString()], self.raw())
        self.assertFalse(self.events()[-1]["trip_auxiliary_state_changed"])
        self.assertFalse(self.events()[-1]["raw_archived"])

    def test_failed_auxiliary_archive_keeps_change_due_for_retry(self):
        collector = self.collector()
        old, new = self.feed([]), self.feed([("new", "geometry")])
        self.observe(collector, old, 0)
        signature = collector._archived_trip_auxiliary_sha256
        with patch("bkk_collector.collector.append_record", side_effect=OSError("storage unavailable")):
            self.observe(collector, new, 1)
        self.assertEqual(signature, collector._archived_trip_auxiliary_sha256)
        self.assertFalse(self.events()[-1]["raw_ok"])
        self.observe(collector, new, 2)
        self.assertEqual([old.SerializeToString(), new.SerializeToString()], self.raw())
        self.assertTrue(self.events()[-1]["trip_auxiliary_state_changed"])

    def test_restart_preserves_auxiliary_baseline_without_host_uptime_assumptions(self):
        feed = self.feed([("shape", "geometry")])
        self.observe(self.collector(), feed, 0)
        self.observe(self.collector(), feed, 1)
        self.assertEqual([feed.SerializeToString()] * 2, self.raw())

    def test_dynamic_stops_and_trip_modifications_are_not_silently_skipped(self):
        for kind in ("stop", "trip_modifications"):
            with self.subTest(kind=kind):
                collector = self.collector()
                initial = self.feed([])
                self.observe(collector, initial, 0)
                changed = self.feed([])
                entity = changed.entity.add(id="auxiliary-" + kind)
                getattr(entity, kind).SetInParent()
                self.observe(collector, changed, 1)
                self.assertEqual(changed.SerializeToString(), self.raw()[-1])
                self.assertEqual(1, self.events()[-1]["trip_auxiliary_entities"])

    def test_poll_header_provenance_survives_prediction_suppression(self):
        collector = self.collector()
        feed = self.feed([])
        feed.header.feed_version = "schedule-one"
        self.observe(collector, feed, 0)
        feed.header.feed_version = "schedule-two"
        self.observe(collector, feed, 1)
        event = self.events()[-1]
        self.assertEqual(0, event["emitted_rows"])
        self.assertEqual("schedule-two", event["feed_header_version"])
        self.assertEqual("2.0", event["gtfs_realtime_version"])
        self.assertEqual(0, event["feed_incrementality"])
        self.assertEqual(1, len(self.raw()))
        self.assertEqual("schedule-one", parse_feed(self.raw()[0]).header.feed_version)


class SourceTimestampTests(ReadinessFixture):
    def rows(self, timestamp=100):
        from bkk_collector.realtime.gtfs_rt_parse import parse_trip_updates
        feed = sample("tripupdates", int(self.now))
        feed.entity[0].trip_update.timestamp = timestamp
        return parse_trip_updates(feed, {})

    def replay(self, path):
        members, timestamps = {}, None
        for _, record in iter_presence(path):
            members = apply_presence_record(members, record)
            timestamps = apply_source_timestamp_record(timestamps, record, members)
        return members, timestamps

    def test_source_timestamp_revisions_survive_without_eta_emission(self):
        collector = self.collector()
        feed = sample("tripupdates", int(self.now))
        for index, timestamp in enumerate((100, 101)):
            feed.entity[0].trip_update.timestamp = timestamp
            with patch("bkk_collector.collector.time.monotonic", return_value=40 + index * 10):
                collector.process_result(response("tripupdates", feed, f"poll-{index}", self.now + index * 10), [])
        path = presence_path(self.data, self.date)
        records = [record for _, record in iter_presence(path)]
        self.assertEqual([], records[-1]["changes"])
        self.assertEqual(1, len(records[-1]["source_timestamp_updates"]))
        members, timestamps = self.replay(path)
        self.assertEqual(members.keys(), timestamps.keys())
        self.assertEqual([101], list(timestamps.values()))
        journal = self.data / "metadata" / "polls" / "tripupdates" / f"date={self.date}" / "polls.jsonl"
        self.assertEqual(0, json.loads(journal.read_text().splitlines()[-1])["emitted_rows"])

    def test_unchanged_values_are_compact_and_null_transitions_are_explicit(self):
        tracker = TripUpdatePresence(self.data, "run")
        for index, timestamp in enumerate((100, 100, None, 101)):
            rows = self.rows()
            for row in rows:
                row["trip_update_timestamp"] = timestamp
            tracker.observe(rows, self.date, self.now + index, {})
        records = [r for _, r in iter_presence(presence_path(self.data, self.date))]
        self.assertEqual([], records[1]["source_timestamp_updates"])
        self.assertIsNone(records[2]["source_timestamp_updates"][0]["timestamp"])
        self.assertEqual(101, records[3]["source_timestamp_updates"][0]["timestamp"])

    def test_failed_append_never_advances_source_timestamp_state(self):
        tracker = TripUpdatePresence(self.data, "run")
        tracker.observe(self.rows(100), self.date, self.now, {})
        with patch("bkk_collector.realtime.tripupdate_presence.append_record", side_effect=OSError("unavailable")):
            with self.assertRaises(OSError):
                tracker.observe(self.rows(101), self.date, self.now + 1, {})
        self.assertEqual([100], list(tracker._source_timestamps.values()))
        tracker.observe(self.rows(101), self.date, self.now + 2, {})
        records = [r for _, r in iter_presence(presence_path(self.data, self.date))]
        self.assertEqual("baseline", records[-1]["kind"])
        self.assertEqual([101], list(self.replay(presence_path(self.data, self.date))[1].values()))

    def test_withdrawal_reappearance_restart_and_midnight_reset_provenance(self):
        tracker = TripUpdatePresence(self.data, "run")
        for index, rows in enumerate((self.rows(100), [], self.rows(101))):
            tracker.observe(rows, self.date, self.now + index, {})
        path = presence_path(self.data, self.date)
        self.assertEqual([101], list(self.replay(path)[1].values()))
        restarted = TripUpdatePresence(self.data, "restart")
        restarted.observe(self.rows(102), self.date, self.now + 3, {})
        self.assertEqual([102], list(self.replay(path)[1].values()))
        from datetime import datetime, timedelta
        next_date = (datetime.fromisoformat(self.date) + timedelta(days=1)).date().isoformat()
        restarted.observe(self.rows(103), next_date, self.now + 4, {})
        self.assertEqual([103], list(self.replay(presence_path(self.data, next_date))[1].values()))

    def test_old_membership_has_unknown_source_provenance_not_fabricated_values(self):
        tracker = TripUpdatePresence(self.data, "run")
        tracker.observe(self.rows(100), self.date, self.now, {})
        record = next(iter_presence(presence_path(self.data, self.date)))[1]
        del record["source_timestamps_version"], record["source_timestamp_updates"]
        members = apply_presence_record({}, record)
        self.assertEqual(1, len(members))
        self.assertIsNone(apply_source_timestamp_record({}, record, members))

    def test_missing_duplicate_or_invalid_provenance_is_rejected(self):
        tracker = TripUpdatePresence(self.data, "run")
        tracker.observe(self.rows(100), self.date, self.now, {})
        original = next(iter_presence(presence_path(self.data, self.date)))[1]
        members = apply_presence_record({}, original)
        for kind in ("missing", "duplicate", "negative", "boolean", "unknown_trip"):
            record = deepcopy(original)
            if kind == "missing":
                record["source_timestamp_updates"] = []
            elif kind == "duplicate":
                record["source_timestamp_updates"] *= 2
            elif kind in ("negative", "boolean"):
                record["source_timestamp_updates"][0]["timestamp"] = -1 if kind == "negative" else True
            else:
                record["source_timestamp_updates"][0]["trip"] = ["unknown"] * 4
            with self.subTest(kind=kind), self.assertRaises(ValueError):
                apply_source_timestamp_record(None, record, members)

    def test_manifest_refuses_missing_journal_promised_source_evidence(self):
        from bkk_collector.archive.manifests import _presence_stats
        from bkk_collector.storage.raw_log import append_record
        tracker = TripUpdatePresence(self.data, "run")
        evidence = tracker.observe(self.rows(), self.date, self.now, {"poll_id": "poll"})
        path = presence_path(self.data, self.date)
        record = next(iter_presence(path))[1]
        del record["source_timestamps_version"], record["source_timestamp_updates"]
        # Recreate this synthetic fixture as a readable legacy-format record,
        # not corrupt bytes. The v1 membership alone cannot satisfy a new promise.
        path.unlink()
        append_record(path, self.now, json.dumps(record).encode())
        stats, errors, _ = _presence_stats(self.data, self.date, [{
            "poll_id": "poll", "parse_ok": True, "presence_required": True, "presence_ok": True, **evidence}])
        self.assertEqual(0, stats["source_timestamp_observations"])
        self.assertTrue(any("source timestamp evidence" in error for error in errors), errors)

    def test_incomplete_http_200_is_a_gap_not_false_withdrawals(self):
        collector = self.collector()
        feed = sample("tripupdates", int(self.now))
        collector.process_result(response("tripupdates", feed, "before", self.now), [])
        collector.process_result(replace(response("tripupdates", feed, "broken", self.now + 10), payload=b""), [])
        collector.process_result(response("tripupdates", feed, "after", self.now + 20), [])
        records = [r for _, r in iter_presence(presence_path(self.data, self.date))]
        self.assertEqual(["baseline", "baseline"], [r["kind"] for r in records])
        self.assertTrue(all(not change["trip_withdrawn"] for record in records for change in record["changes"]))
        journal = self.data / "metadata" / "polls" / "tripupdates" / f"date={self.date}" / "polls.jsonl"
        failure = json.loads(journal.read_text().splitlines()[1])
        self.assertFalse(failure["parse_ok"])
        self.assertFalse(failure["presence_ok"])
        self.assertTrue(failure["raw_archived"] and failure["raw_ok"])
