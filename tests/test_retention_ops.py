from __future__ import annotations

import hashlib
import random
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow.parquet as pq

from bkk_collector.archive.retention import prune_confirmed
from bkk_collector.archive.static_gtfs import StaticGtfsStore
from bkk_collector.archive.static_retention import prune_static_versions
from bkk_collector.cli.diagnostics import collection_window_report
from bkk_collector.storage.atomic_io import atomic_write_json, read_json, sha256_file
from bkk_collector.config import FEED_NAMES, MaintenanceConfig
from bkk_collector.realtime.dedup import ChangeTracker
from bkk_collector.maintenance import MaintenanceWorker
from bkk_collector.archive.manifests import _feed_stats
from bkk_collector.storage.atomic_io import append_jsonl
from bkk_collector.monitoring import HealthMonitor
from bkk_collector.storage.parquet_store import DurableParquetSpool, PARQUET_WRITE_BATCH_ROWS, write_parquet_atomic
from tests import test_reliability as reliability
from bkk_collector.cli.verify_backup import restore_artifact


class RetentionTests(unittest.TestCase):
    setUp = reliability.BackupTests.setUp
    tearDown = reliability.BackupTests.tearDown
    manager = reliability.BackupTests.manager

    def ready(self):
        self.api = reliability.FakeBackupApi(self.root)
        self.backup = self.manager(self.api)
        result = self.backup.backup_date(self.date, manifest=self.complete_manifest)
        self.assertTrue(result.success, result.reason)
        self.receipt = self.root / f"backup_receipts/date={self.date}.json"
        self.files = sorted((self.root / "parquet").glob("*/date=*/*.parquet"))
        return self.backup

    def hashes(self):
        return {str(p.relative_to(self.root)): sha256_file(p) for p in self.root.rglob('*')
                if p.is_file() and 'maintenance' not in p.parts and p.name != 'prune_history.jsonl'}

    def refuse(self):
        before = self.hashes()
        self.assertEqual([], self.backup.prune_confirmed_parquet(7))
        self.assertEqual(before, self.hashes())

    def test_verified_old_date_prunes_only_parquet_and_is_idempotent(self):
        self.ready()
        before = self.hashes()
        self.assertEqual(set(self.files), set(self.backup.prune_confirmed_parquet(7)))
        after = self.hashes()
        self.assertEqual({k:v for k,v in before.items() if not k.startswith('parquet/')}, after)
        self.assertEqual([], self.backup.prune_confirmed_parquet(7))
        event = read_json(self.root / f'maintenance/prune-parquet-{self.date}.json', {})
        self.assertEqual('parquet_prune', event['operation'])
        self.assertEqual('complete', event['status'])
        self.assertEqual(3, len(event['files_deleted']))
        self.assertEqual(sum(a['size'] for a in self.complete_manifest['artifacts'] if a['kind']=='parquet'), event['bytes_deleted'])

    def add_artifact(self, relative, kind, payload):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        self.complete_manifest["artifacts"].append({
            "path": relative, "kind": kind, "size": len(payload), "sha256": sha256_file(path),
        })
        return path

    def prepare_evidence(self):
        self.add_artifact(f"metadata/tripupdates_presence/date={self.date}/presence.jsonlog",
                          "tripupdates_presence", b"presence")
        self.add_artifact(f"metadata/polls/alerts/date={self.date}/polls.jsonl.checkpoint.json",
                          "poll_metadata", b"checkpoint")
        self.complete_manifest["feeds"]["tripupdates"]["presence"] = {
            "records": 1, "journal_confirmed_observations": 1,
        }
        atomic_write_json(self.manifest_path, self.complete_manifest)
        self.ready()

    def test_verified_evidence_eviction_preserves_all_other_artifacts_and_reports_absence(self):
        self.prepare_evidence()
        before = self.hashes()
        files = {a["path"] for a in self.complete_manifest["artifacts"]
                 if a["kind"] in {"poll_metadata", "tripupdates_presence"}}
        self.assertEqual(files, {str(p.relative_to(self.root)) for p in
                                 prune_confirmed(self.backup, "evidence", 30)})
        self.assertEqual({k: v for k, v in before.items() if k not in files}, self.hashes())
        self.assertEqual([], prune_confirmed(self.backup, "evidence", 30))
        start = datetime.fromisoformat(self.date).replace(tzinfo=timezone.utc).timestamp()
        report = collection_window_report(self.root, start, start + 86400)
        for feed in report["feeds"].values():
            self.assertEqual([self.date], feed["evicted_journal_dates"])
            self.assertIsNone(feed["successful_data_poll_count_ratio"])

    def test_evidence_prune_refuses_entire_date_on_unbacked_tail_or_remote_mismatch(self):
        self.prepare_evidence()
        extra = self.root / f"metadata/polls/alerts/date={self.date}/polls.jsonl.corrupt-tail-test"
        extra.write_bytes(b"forensic evidence")
        before = self.hashes()
        self.assertEqual([], prune_confirmed(self.backup, "evidence", 30))
        self.assertEqual(before, self.hashes())
        extra.unlink()
        revision = read_json(self.receipt, {})["remote_revision"]
        target = f"metadata/tripupdates_presence/date={self.date}/presence.jsonlog"
        del self.api.revisions[revision][target]
        before = self.hashes()
        self.assertEqual([], prune_confirmed(self.backup, "evidence", 30))
        self.assertEqual(before, self.hashes())

    def test_evidence_prune_refuses_unpinned_receipt(self):
        self.prepare_evidence()
        receipt = read_json(self.receipt, {})
        receipt.pop("remote_revision")
        atomic_write_json(self.receipt, receipt)
        self.api.upload_file(path_in_repo=str(self.receipt.relative_to(self.root)),
                             path_or_fileobj=self.receipt)
        before = self.hashes()
        self.assertEqual([], prune_confirmed(self.backup, "evidence", 30))
        self.assertEqual(before, self.hashes())

    def prepare_static_versions(self):
        today = datetime.now(timezone.utc).date()
        old_date = self.date
        versions = {}
        for role, age in (("evict", 80), ("pending", 70), ("boundary", 35), ("active", 2)):
            payload = role.encode()
            digest = hashlib.sha256(payload).hexdigest()
            relative = f"versions/{digest}.zip"
            versions[role] = self.add_artifact(f"static_gtfs/{relative}", "static_gtfs", payload)
            append_jsonl(self.root / "static_gtfs/history.jsonl", {
                "checked_at": f"{today - timedelta(days=age)}T00:00:00+00:00",
                "version_path": relative, "sha256": digest,
            })
        self.date = (today - timedelta(days=1)).isoformat()
        self.complete_manifest["date"] = self.date
        # Create a complete receipt dated just before today with the test
        # versions as extra static artifacts and today's observed timeline.
        for artifact in self.complete_manifest["artifacts"]:
            if f"date={old_date}/" in artifact["path"]:
                old = self.root / artifact["path"]
                artifact["path"] = artifact["path"].replace(f"date={old_date}/", f"date={self.date}/")
                target = self.root / artifact["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                old.rename(target)
                old.parent.rmdir()
        for item in [self.complete_manifest["static_gtfs"], *self.complete_manifest["static_gtfs_timeline"]]:
            item.update(version_path=f"versions/{versions['active'].name}",
                        sha256=versions["active"].stem)
        self.manifest_path = self.root / f"metadata/manifests/date={self.date}.json"
        atomic_write_json(self.manifest_path, self.complete_manifest)
        atomic_write_json(self.root / "static_gtfs/state.json", {
            "latest_version_path": f"versions/{versions['active'].name}",
            "latest_sha256": versions["active"].stem,
        })
        self.ready()
        self.backup.static_store = StaticGtfsStore(self.root, check_interval_seconds=86400, retry_seconds=60)
        pending = today - timedelta(days=65)
        journal = self.root / f"metadata/polls/alerts/date={pending}/polls.jsonl"
        journal.parent.mkdir(parents=True)
        journal.write_bytes(b"unconfirmed date")
        return versions

    def test_static_eviction_keeps_active_boundary_pending_and_legacy_archives(self):
        versions = self.prepare_static_versions()
        before = self.hashes()
        self.assertEqual([versions["evict"]], prune_static_versions(self.backup, 30))
        del before[str(versions["evict"].relative_to(self.root))]
        self.assertEqual(before, self.hashes())
        self.assertEqual([], prune_static_versions(self.backup, 30))

    def test_static_eviction_refuses_when_pinned_archive_is_missing(self):
        versions = self.prepare_static_versions()
        revision = read_json(self.receipt, {})["remote_revision"]
        del self.api.revisions[revision][str(versions["evict"].relative_to(self.root))]
        before = self.hashes()
        self.assertEqual([], prune_static_versions(self.backup, 30))
        self.assertEqual(before, self.hashes())

    def test_current_and_inside_retention_dates_never_prune(self):
        self.ready()
        for offset in (0, 1, 7):
            # Freeze only the retention calendar; file hashes and remote checks
            # remain real. Existing fixture date becomes today or recent.
            now = datetime.strptime(self.date, '%Y-%m-%d').replace(tzinfo=timezone.utc) + timedelta(days=offset)
            with patch('bkk_collector.archive.retention.datetime') as clock:
                clock.now.return_value = now
                clock.strptime.side_effect = datetime.strptime
                self.refuse()

    def test_missing_receipt_and_legacy_unverified_stay(self):
        self.ready()
        self.receipt.unlink()
        self.refuse()

    def test_unverified_receipt_stays(self):
        self.ready()
        value = read_json(self.receipt, {}); value['remote_verified'] = False
        atomic_write_json(self.receipt, value)
        self.refuse()

    def test_missing_feed_in_receipt_refuses_before_remote_requests(self):
        self.ready()
        value=read_json(self.receipt,{})
        value['artifacts']=[a for a in value['artifacts'] if not a['path'].startswith('parquet/alerts/')]
        atomic_write_json(self.receipt,value)
        with patch.object(self.backup,'remote_revision') as remote:
            self.refuse()
        remote.assert_not_called()

    def test_modified_manifest_cannot_authorize_pruning(self):
        self.ready()
        value=read_json(self.manifest_path,{});value['complete']=False
        atomic_write_json(self.manifest_path,value)
        self.refuse()

    def test_receipt_date_or_repo_mismatch_stays(self):
        self.ready()
        original = read_json(self.receipt, {})
        for key,value in [('date','1999-01-01'),('repo_id','other/repo')]:
            atomic_write_json(self.receipt, {**original,key:value})
            self.refuse()

    def test_size_mismatch_on_last_feed_deletes_nothing(self):
        self.ready(); self.files[-1].write_bytes(b'changed-size')
        self.refuse()

    def test_same_size_hash_mismatch_on_last_feed_deletes_nothing(self):
        self.ready(); self.files[-1].write_bytes(b'x' * self.files[-1].stat().st_size)
        self.refuse()

    def test_extra_parquet_or_hidden_transaction_refuses_entire_date(self):
        self.ready()
        for name in ['extra.parquet','.compaction-transaction.json']:
            extra = self.files[-1].with_name(name);extra.write_bytes(b'extra')
            self.refuse();extra.unlink()

    def test_nested_directory_and_symlink_are_never_deleted(self):
        self.ready()
        directory = self.files[-1].parent / 'nested';directory.mkdir()
        (directory/'irreplaceable').write_bytes(b'keep')
        self.refuse()
        (directory/'irreplaceable').unlink();directory.rmdir()
        self.files[-1].with_name('link.parquet').symlink_to(self.files[0])
        self.refuse()

    def test_parent_symlink_refuses(self):
        self.ready()
        directory = self.files[-1].parent
        saved = directory.with_name('saved');directory.rename(saved);directory.symlink_to(saved, target_is_directory=True)
        self.refuse()
        self.assertTrue((saved / self.files[-1].name).exists())

    def test_pending_spool_refuses(self):
        self.ready()
        spool = self.root / f'spool/alerts/date={self.date}/commit-pending.json'
        atomic_write_json(spool, {'pending':True})
        self.refuse()

    def test_remote_missing_or_mismatched_content_refuses(self):
        self.ready()
        # Pin a remote revision whose candidate object is corrupt.
        revision=read_json(self.receipt,{})['remote_revision']
        key=str(self.files[-1].relative_to(self.root)); old=self.api.revisions[revision][key]
        self.api.revisions[revision][key]=(old[0],'0'*64)
        self.refuse()
        del self.api.revisions[revision][key]
        self.refuse()

    def test_offline_remote_refuses_without_changing_health(self):
        self.ready()
        with patch.object(self.api,'repo_info',side_effect=ConnectionError('offline')):
            self.refuse()
        worker=MaintenanceWorker(MaintenanceConfig(self.root,'token','owner/repo'))
        worker.backup=self.backup
        atomic_write_json(self.root/'static_gtfs/state.json',{'last_success_timestamp':time.time()})
        self.assertTrue(worker.write_status()['healthy'])
        worker.session.close()

    def test_unpinned_v2_receipt_is_reverified_before_pruning(self):
        self.ready()
        value=read_json(self.receipt,{});value.pop('remote_revision')
        atomic_write_json(self.receipt,value)
        self.api.upload_file(path_in_repo=str(self.receipt.relative_to(self.root)),path_or_fileobj=self.receipt)
        self.assertEqual(3,len(self.backup.prune_confirmed_parquet(7)))

    def test_mutation_during_remote_check_deletes_nothing(self):
        self.ready()
        verify=self.backup._verify_remote
        def racing(*args,**kwargs):
            verify(*args,**kwargs)
            self.files[-1].write_bytes(b'changed')
        with patch.object(self.backup,'_verify_remote',side_effect=racing):
            self.assertEqual([],self.backup.prune_confirmed_parquet(7))
        self.assertTrue(all(p.exists() for p in self.files))

    def test_prune_intent_write_failure_deletes_nothing(self):
        self.ready()
        with patch('bkk_collector.archive.retention.atomic_write_json',side_effect=OSError('full')):
            self.refuse()

    def test_interrupted_unlink_resumes_with_verified_intent(self):
        self.ready()
        original=Path.unlink
        def fail(path,*args,**kwargs):
            if path==self.files[1]:raise OSError('interrupted')
            return original(path,*args,**kwargs)
        with patch.object(Path,'unlink',new=fail):
            self.backup.prune_confirmed_parquet(7)
        state=read_json(self.root/f'maintenance/prune-parquet-{self.date}.json',{})
        self.assertEqual('interrupted',state['status'])
        self.assertEqual(2,len(self.backup.prune_confirmed_parquet(7)))
        self.assertTrue(all(not p.exists() for p in self.files))

    def test_raw_pruner_refuses_unexpected_nested_files_and_preserves_parquet(self):
        self.ready()
        extra=self.root/f'raw/alerts/date={self.date}/nested';extra.mkdir()
        (extra/'unverified').write_bytes(b'keep')
        before=self.hashes()
        self.assertEqual([],self.backup.prune_confirmed_raw(3))
        self.assertEqual(before,self.hashes())
        (extra/'unverified').unlink();extra.rmdir()
        self.assertEqual(3,len(self.backup.prune_confirmed_raw(3)))
        self.assertTrue(all(p.exists() for p in self.files))

    def test_non_lfs_same_size_wrong_hash_cannot_be_verified(self):
        self.ready()
        artifact={'path':'state.json','size':3,'sha256':hashlib.sha256(b'one').hexdigest()}
        response=Mock();response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        response.iter_content.return_value=iter([b'two'])
        with patch.object(self.api,'get_paths_info',return_value=[SimpleNamespace(path='state.json',size=3,lfs=None)]), patch('requests.get',return_value=response):
            with self.assertRaisesRegex(OSError,'checksum mismatch'):
                self.backup._verify_remote([artifact])

    def test_receipt_pins_metadata_and_is_not_overwritten_by_retry(self):
        self.ready()
        receipt=read_json(self.receipt,{})
        self.assertEqual(40,len(receipt['remote_revision']))
        before=sha256_file(self.receipt);uploads=len(self.api.uploaded)
        self.assertTrue(self.backup.backup_date(self.date).success)
        self.assertEqual(before,sha256_file(self.receipt));self.assertEqual(uploads,len(self.api.uploaded))

    def test_restore_hashes_actual_downloaded_bytes(self):
        self.ready()
        data=b'actual raw bytes';artifact={'path':'raw/example','size':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        def response(payload):
            r=Mock();r.__enter__=Mock(return_value=r);r.__exit__=Mock(return_value=False)
            r.iter_content.return_value=iter([payload]);return r
        with patch('bkk_collector.cli.verify_backup.requests.get',return_value=response(data)):
            result=restore_artifact(self.backup,artifact,'a'*40,self.root/'restore-good')
        self.assertEqual('PASS',result['result'])
        with patch('bkk_collector.cli.verify_backup.requests.get',return_value=response(b'x'*len(data))):
            with self.assertRaises(OSError):restore_artifact(self.backup,artifact,'a'*40,self.root/'restore-bad')


class MemoryAndHealthTests(unittest.TestCase):
    def test_retention_configuration_defaults_disable_and_validation(self):
        with patch.dict('os.environ',{},clear=True):
            config=MaintenanceConfig.from_env()
            self.assertEqual(3,config.prune_local_raw_after_days)
            self.assertEqual(7,config.prune_local_parquet_after_days)
            self.assertEqual(0, config.prune_local_evidence_after_days)
            self.assertEqual(0, config.prune_local_static_gtfs_after_days)
        with patch.dict('os.environ', {'PRUNE_LOCAL_EVIDENCE_AFTER_DAYS': '30',
                                      'PRUNE_LOCAL_STATIC_GTFS_AFTER_DAYS': '45'}):
            config = MaintenanceConfig.from_env()
            self.assertEqual(30, config.prune_local_evidence_after_days)
            self.assertEqual(45, config.prune_local_static_gtfs_after_days)
        with patch.dict('os.environ',{'PRUNE_LOCAL_PARQUET_AFTER_DAYS':'0'}):
            self.assertEqual(0,MaintenanceConfig.from_env().prune_local_parquet_after_days)
        with self.assertRaisesRegex(ValueError,'PRUNE_LOCAL_PARQUET'):
            MaintenanceConfig(Path('/tmp/not-used'),'','',prune_local_parquet_after_days=-1)

    def test_manifest_separates_alert_advisories_from_stale_data(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            append_jsonl(root/'metadata/polls/alerts/date=2026-08-25/polls.jsonl',{
                'success':True,'parse_ok':True,'freshness_flags':['source_timestamp_unchanged_warning'],
                'poll_interval_seconds':30,'raw_archived':True,
            })
            stats,_,quality=_feed_stats(root,'alerts','2026-08-25',create_empty=False)
            self.assertEqual(0,stats['stale_feed_polls'])
            self.assertEqual(1,stats['freshness_warning_polls'])
            self.assertFalse(any('freshness incident' in item for item in quality))

    def test_recovery_cli_handles_missing_credentials_without_creating_data(self):
        from bkk_collector.cli import verify_backup
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'absent-data'
            with patch.dict('os.environ',{'DATA_DIR':str(path),'HF_TOKEN':'','HF_REPO_ID':''}), patch('sys.argv',['verify_backup.py','2026-09-03']), patch('builtins.print'):
                self.assertEqual(1,verify_backup.main())
            self.assertFalse(path.exists())

    def test_expiring_tracker_matches_unbounded_reference_with_failed_commits(self):
        bounded=ChangeTracker(('id',), numeric_tolerance_fields=('time',),tolerance=2,heartbeat_seconds=30)
        reference=ChangeTracker(('id',), numeric_tolerance_fields=('time',),tolerance=2,heartbeat_seconds=30)
        reference._expire_heartbeat_entries=lambda now:None
        rng=random.Random(17)
        for now in range(1000):
            rows=[{'id':rng.randrange(100), 'time':rng.choice([None,100,101,102,103,130])} for _ in range(7)]
            a=bounded.filter(rows,'day',now,update=False);b=reference.filter(rows,'day',now,update=False)
            self.assertEqual(b,a)
            if now%13:
                bounded.commit(a,'day',now);reference.commit(b,'day',now)

    def test_departed_trip_memory_is_bounded_without_waiting_for_midnight(self):
        tracker=ChangeTracker(('id',),numeric_tolerance_fields=('time',),tolerance=2,heartbeat_seconds=30)
        for now in range(600):
            tracker.filter([{'id':f'{now}-{i}','time':100} for i in range(100)],'day',now)
        self.assertLessEqual(len(tracker._last),6000)

    def test_parquet_writer_bounds_arrow_batch_size_and_preserves_all_rows(self):
        from bkk_collector.storage import parquet_store
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'data.parquet';seen=[];original=parquet_store.rows_to_table
            def table(feed,rows,**options):
                seen.append(len(rows));return original(feed,rows,**options)
            with patch('bkk_collector.storage.parquet_store.rows_to_table',side_effect=table):
                write_parquet_atomic('alerts',({'entity_id':str(i)} for i in range(10001)),path)
            self.assertLessEqual(max(seen),PARQUET_WRITE_BATCH_ROWS)
            self.assertEqual(10001,pq.ParquetFile(path).metadata.num_rows)

    def test_stream_failure_keeps_all_spool_sources_then_restart_recovers(self):
        with tempfile.TemporaryDirectory() as temp:
            spool=DurableParquetSpool(Path(temp),0)
            for i in range(3):spool.stage('alerts','2026-08-25',str(i),[{'entity_id':str(i)}])
            sources=spool.pending_segments();reader=spool._read_segment;calls=0
            def interrupted(path):
                nonlocal calls
                calls+=1
                if calls==5:raise OSError('mid-stream failure')
                return reader(path)
            with patch.object(spool,'_read_segment',side_effect=interrupted):
                self.assertFalse(spool.flush(force=True).ok)
            self.assertEqual(sources,spool.pending_segments())
            result=DurableParquetSpool(Path(temp),0).flush(force=True)
            self.assertTrue(result.ok,result.errors);self.assertEqual(3,result.rows_written)

    def test_alert_timestamp_warning_but_http_parse_and_storage_failures_unhealthy(self):
        with tempfile.TemporaryDirectory() as temp:
            monitor=HealthMonitor(Path(temp),stale_seconds=180,absent_seconds=180,frozen_seconds=300,alerts_frozen_seconds=300)
            def success(feed,**overrides):
                args=dict(now_ts=1000,header_timestamp=1 if feed=='alerts' else 1000,content_sha256='x',min_entity_timestamp=1,max_entity_timestamp=1000,parse_ok=True,raw_ok=True,spool_ok=True)
                monitor.record_success(feed,**{**args,**overrides})
            def status():return monitor.write_status(now_ts=1000,poll_id='p',disk_free_bytes=100,disk_warn_bytes=2,disk_critical_bytes=1,pending_spool_segments=0,parquet_flush_errors=[],cycle_errors=[])
            for feed in FEED_NAMES:success(feed)
            self.assertTrue(status()['healthy']);self.assertIn('alerts:source_timestamp_unchanged_warning',status()['warnings'])
            for field in ['parse_ok','raw_ok','spool_ok']:
                success('alerts',**{field:False});self.assertFalse(status()['healthy']);success('alerts')
            monitor.record_failure('alerts',now_ts=1000,error='offline',http_status=502)
            self.assertIn('alerts:http_failed',status()['reasons'])
            success('alerts');self.assertTrue(status()['healthy'])
            success('vehiclepositions',header_timestamp=1)
            self.assertIn('vehiclepositions:source_timestamp_stale',status()['reasons'])
