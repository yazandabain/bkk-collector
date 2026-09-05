# Production retention and recovery audit — 2026-09-05

## Verdict

**WARN pending deployment and capacity planning.** Current ingestion and backup
content are working, but the audit found five collector OOM kills, unbounded
Parquet, and incomplete backup verification/provenance. The repository fixes
those paths. No production files/configuration were changed, containers
restarted, migration triggered, or HF objects written/deleted during this audit.

Seven-day raw/derived coverage is high, but not 100%. No evidence was found of
loss of already staged derived rows. Pre-durability responses and missed polls
cannot be reconstructed from five-minute TU raw snapshots.

Use a 3-day verified raw cache and 7-day verified Parquet cache. **For a full
nine-month run, increase usable disk capacity to at least 64 GB** before leaving
the installation with weekly-only attention. Protected journals and static
versions still grow; this pass does not delete historical provenance to meet a
disk target. On the existing disk, the projected warning remains about seven
months away after the fix. HF outages or refused pruning shorten that runway.

## Evidence and subsystem results

Read-only production inspection began around **12:33 UTC**, with final inventory
checks before 13:00 UTC, on
`root@46.225.61.62`, repository `/root/bkk-collector-new`, Git commit
`3d16058fbf79f9b5ae2edc870ff04ab2d9e50af9`. The `data` symlink resolves to
`/root/bkk_collector/data`; preserve it during deployment.

Evidence: `git status/diff`, `docker compose ps`, selective `docker inspect`,
`docker stats --no-stream`, `df -B1`, `du -x -B1`, allowlisted configuration,
health/status JSON, streamed daily journals, manifests/receipts, Parquet footer
schemas, `journalctl -k --since 2026-08-29`, HF path metadata, and streamed
downloads. Tokens, authenticated URLs, and full environment dumps were excluded.

| Subsystem | Observed result | Evidence / resolution |
|---|---|---|
| Ingestion | WARN | 10s VP, 10s TU, 30s Alerts confirmed; five OOM interruptions and scheduler skips |
| Raw durability | PASS with crash boundary | fsynced records/checkpoints; first sample and previous-day tail recovery tested; no recorded write failures |
| Derived durability | PASS with memory fix | Every seven-day journal-emitted row accounted for in Parquet; failed writes/restarts tested |
| Parquet retention | FAIL → fixed locally | 9.15GB retained without limit; new receipt/hash-gated 7-day eviction |
| Static GTFS | PASS / provenance warning | Daily checks, 15 history observations including legacy, content-addressed ZIPs; pinned receipt revision fixes restoration |
| Backup verification | WARN → fixed locally | 13 v2 receipts; old ordinary Git files were checked by size only; all new files require SHA-256 verification |
| Recovery | PASS at historical revision | Raw, Parquet, static ZIP, journal, manifest and static metadata downloaded/authenticated; details below |
| Monitoring | WARN → fixed locally | Production-only Alerts patch integrated; advisory counts separated; HTTP failures immediately unhealthy |
| Maintenance | PASS / capacity warning | Healthy, zero restarts, no current v2 backlog; legacy warnings retained |
| Long-term disk | WARN | Large caches bounded by fix; protected provenance continues growing, requiring capacity for nine months |

No Docker build was possible locally: the Windows Docker launcher reports that
WSL integration is unavailable. Production was not used as a build/test target.

## Seven completed UTC dates: August 29–September 4

Counts below are durable **completed request journal events**, not an inferred
count of requests lost before journaling. Expected counts use 86,400 seconds/day
and the confirmed per-feed cadence. There is no persisted seven-day submitted
counter independent of the journals; startup resets runtime scheduler counters.

| UTC date | VP success / 8,640 | VP % | TU success / 8,640 | TU % | Alerts success / 2,880 | Alerts % |
|---|---:|---:|---:|---:|---:|---:|
| Aug 29 | 8,640 | 100.0000 | 8,636 | 99.9537 | 2,880 | 100.0000 |
| Aug 30 | 8,640 | 100.0000 | 8,639 | 99.9884 | 2,880 | 100.0000 |
| Aug 31 | 8,632 | 99.9074 | 8,517 | 98.5764 | 2,880 | 100.0000 |
| Sep 1 | 8,592 | 99.4444 | 8,490 | 98.2639 | 2,879 | 99.9653 |
| Sep 2 | 8,589 | 99.4097 | 8,483 | 98.1829 | 2,880 | 100.0000 |
| Sep 3 | 8,589 | 99.4097 | 8,483 | 98.1829 | 2,879 | 99.9653 |
| Sep 4 | 8,574 | 99.2361 | 8,472 | 98.0556 | 2,877 | 99.8958 |

| Seven-day measure | VP | TU | Alerts |
|---|---:|---:|---:|
| Expected polls | 60,480 | 60,480 | 20,160 |
| Journaled attempts | 60,257 | 59,721 | 20,155 |
| HTTP + parse successes | 60,256 | 59,720 | 20,155 |
| Coverage | **99.629630%** | **98.743386%** | **99.975198%** |
| Recorded scheduler misses | 205 | 739 | 0 |
| HTTP failures | 1 | 1 | 0 |
| Parse/raw/spool/change-tracking failures | 0 | 0 | 0 |
| Raw snapshots | 60,256 | 1,919 | 20,155 |
| Raw median spacing | 10.000013s | 316.628595s | 29.999892s |
| Derived rows = Parquet rows | 59,965,454 | 326,521,827 | 160,676 |
| Inter-success gaps >1.5× cadence | 204 | 628 | 6 |
| Maximum observed inter-success gap | 50.92s | 61.10s | 80.92s |

VP/TU had **zero recorded stale/frozen-source flags**. Alerts had **4,346
`source_timestamp_unchanged_warning` events**, which old manifests mislabeled
as freshness incidents. These are valid unchanged-source responses; they do
not represent missing observations. New manifests count them separately.

After subtracting logged scheduler misses and HTTP failures, 18 VP, 20 TU and
5 Alerts expected slots remain unexplained by those counters. Restarts, day
boundary phase shifts and pre-journal observations limit attribution. Do not
report these as proven write losses or pretend all were scheduler skips.

At 12:34 UTC, runtime submitted/completed counters were VP 8,221/8,221,
TU 8,180/8,180, Alerts 2,750/2,750; missed counters 27/68/0 since latest restart.
VP/TU each had a result pending processing, so completed network requests are
not necessarily already durably journaled. Median HTTP latency was 46.079ms VP,
119.160ms TU, 37.317ms Alerts. The much larger scheduler loss is consistent with
local processing pressure; exact per-phase production profiling was not added.

The kernel, unlike current Docker `OOMKilled=false`, records collector cgroup
OOM kills on Sep 1 05:10:19, Sep 2 13:47:07, Sep 3 14:15:59, Sep 4 13:33:26 and
13:39:11 UTC. RSS at death was approximately 1.56GB. These align with the longest
gaps. Docker restart count was 5; maintenance restart count was 0.

## Storage and retention

All GB below are decimal; `df` available space excludes reserved filesystem
blocks. Filesystem: **39,956,590,592 bytes total**, **18,994,860,032 used**,
**19,306,102,784 available**. Runtime warn/critical: **8,000,000,000 /
4,000,000,000 bytes**, now also repository defaults.

| Consumer | Logical size at observation |
|---|---:|
| Raw (including protected legacy) | 4.917GB |
| Parquet | 9.150GB |
| Static GTFS | 0.743GB |
| Poll/manifests metadata | 0.344GB |
| Spool | 0.00056GB, fluctuating during collection |
| Application logs | 0.00103GB, rotated |
| Host `/var/log` allocated | 0.384GB |

Later per-feed inventory (current date continued growing): raw VP **2.106923GB**,
TU **2.249621GB**, Alerts **0.575844GB**; Parquet VP **2.549320GB**, TU
**6.548190GB**, Alerts **0.064849GB**.

Allocated data total was approximately 15.18GB; roughly 3.8GB of filesystem
usage is outside this dataset. Journal/log rotation is configured. Preserve
fixed-size legacy artifacts; no new migration is needed (five dates were
already inventoried before this audit, with unconfirmed remote copies).

Parquet per date: Aug 29 **0.504197GB**, Aug 30 **0.459587GB**, Aug 31
**0.688566GB**, Sep 1 **0.746084GB**, Sep 2 **0.751638GB**, Sep 3 **0.745435GB**,
Sep 4 **0.741576GB**. Mean **0.662441GB/day**; recent weekdays are nearer 0.75.
Sep 4 by feed: VP **0.193948GB**, TU **0.543803GB**, Alerts **0.003825GB**.
Each recent complete date/feed has one compacted Parquet file. All 21 recent
files match the current typed schema. Historical cutover errors were left alone.

Raw Sep 2–4 averaged **0.862667GB/day**, above the earlier 0.6–0.8 estimate.
Current local v2 raw dates are Sep 2–5, plus protected Aug 18–22. VP/Alerts raw
contains every recorded successful response. TU uses a separate 300s minimum
elapsed interval; processing and poll alignment make observed spacing ~317s.
No cadence/tolerance changes were made to make coverage look better.

Without changes, mean net growth (Parquet + 0.045488GB/day static +
0.025308GB/day journals, raw cache roughly stable) is **0.733238GB/day**.
Starting at the measured free space, linear projections are:

- 8GB warning: **Sep 20 ~22:38 UTC**;
- 4GB critical: **Sep 26 ~09:34 UTC**;
- available space exhausted: **Oct 1 ~20:29 UTC**.

These are estimates, not deadlines; weekday activity, row-group encoding,
backup delays, temporary compaction files and reserved blocks affect them.

Seven-day Parquet retention can initially free **3,903,429,072 bytes** (verified
Aug 23–28). It retains ~4.64GB of the seven completed days plus today's partial
data and ~0.348GB of protected legacy Parquet. Raw keeps three completed days
plus today, subject to verified backup. No automatic eviction of static,
journals, receipts, manifests or recovery state is introduced.

The remaining ~70.8MB/day provenance growth projects the warning to **Apr 8
2027**, critical to **Jun 3**, and exhaustion to **Jul 30**, assuming cache size
stays comparable. This is why 38GB is not a comfortable nine-month capacity
target. Provision at least 64GB usable space or add capacity before April;
weekly monitoring must track the actual slope. No safe retention policy can
bound storage through an indefinitely unavailable remote archive while also
preserving every unverified observation.

## Backup and recovery proof

13 v2 receipts exist for Aug 23–Sep 4; no current v2 backlog. Seven recent
receipts contain 24–30 artifacts. All requested remote paths exist. LFS
sizes/SHA-256 match. At HF head `5c94870fd7146789a78e195139197b6dd1339096`, older
`static_gtfs/history.jsonl` differs in size and hash, and `state.json` differs
in hash **despite equal size**. This demonstrates why size-only verification
and unpinned restoration are insufficient.

The Sep 3 receipt-upload commit is
**`9ac4e8b0c06ec452da747d504de0ae0277c7c849`** (Sep 4 03:09:44 UTC).
Restoration at that revision passed:

| Restored artifact | Bytes | SHA-256 |
|---|---:|---|
| `raw/alerts/date=2026-09-03/alerts.rawlog` | 72,117,972 | `a0d0ad44acd224b4f7346e70abc8b6aec6faa57ecbb17b25f9a1c9271d6adca9` |
| `parquet/alerts/date=2026-09-03/part-compacted-fa73185e6582d90fd81a.parquet` | 4,785,038 | `3e1acbe3627ae7a19063d73ce5485d3c2889c629cc56a48cee1255b18f7af0af` |
| `static_gtfs/history.jsonl` | 5,283 | `be152c3c873125c67f7bcdfb9be29649b10abf715bfdb80ba1a393036acbc67b` |
| `static_gtfs/state.json` | 486 | `88f4baf656979b54b53839454ffae74b04ef115bfa68df7551b70fb0c2af3b6e` |

The restored raw log scans cleanly with **2,879 records**; restored Parquet has
**39,398 rows**. Additional authenticated downloads: static ZIP 37,936,876
bytes (`3adfd67e…cab3bf1`), Alerts poll journal 3,404,625 bytes
(`d3ac0dde…38b1d7`), daily manifest 12,195 bytes (`74cfb0f8…678623`). These
immutable date/hash paths also matched at head. Every recovery-test directory
was newly created under `/tmp`, then removed; no production artifact was removed.

New receipts add the verified `remote_revision`. Ordinary Git objects are
stream-downloaded and SHA-256 checked; LFS hashes are checked at that revision.
Old receipts stay unchanged. New pruning revalidates even old v2 cache objects
and the remote receipt before deletion. `verify_backup.py` resolves old receipts
to their authenticated historical commit for full provenance recovery.

## Findings and changes

| Severity | Root cause / issue | Action |
|---|---|---|
| P0 | Daily tracker accumulation and multi-segment row materialization contributed to repeated OOM kills and irrecoverable observation gaps | Expire only heartbeat-eligible tracker entries; per-poll overlay instead of full dictionary copy; stream commits one decoded segment / 4,096 Arrow rows at a time |
| P1 | Parquet grew indefinitely toward September disk pressure | Default 7-day verified eviction, complete date preflight and auditable recovery intent |
| P1 | Non-LFS verification accepted equal-size different content | Download/hash ordinary Git files; verify all hashes before receipt publication |
| P1 | Mutable static metadata paths made older receipts ambiguous at HF head | Pin new receipts to a Git revision; diagnostic authenticates old receipt-upload commit |
| P1 | Recursive raw directory deletion could remove unlisted nested artifacts | Shared strict raw/Parquet validation; reject nested/symlink/extra files; unlink exact verified files only |
| P1, capacity action required | Protected static/journals continue growing on 38GB disk | Keep provenance; document measured forecast and ≥64GB capacity target for nine months |
| P2 | Alerts liveness patch existed only in dirty production source | Integrate equivalent logic and tests; preserve VP/TU strictness |
| P2 | Advisory Alerts flags inflated stale-feed manifest counts | Separate advisory counters prospectively; immutable old manifests retained |
| P2 | HTTP failure could leave health green until 180s absence elapsed | Current consecutive HTTP failure now explicitly unhealthy; success clears it |
| P2 | Defaults differed from deployed disk/raw configuration | Defaults now 8GB/4GB and raw 3 days, keeping environment override behavior |
| P2, deployment setting | Existing manifests report `collector_git_commit=unknown` | Preserve old provenance honestly; deployment instructions set the actual revision for future manifests |

Prune validation is all-or-nothing **before deletion**. Cross-directory unlink
cannot be one filesystem transaction. Mid-deletion crash/errors can leave a
partial verified cache; durable intent and remote checks make recovery safe.
The tests explicitly exercise this instead of claiming impossible multi-file
atomicity. Refused dates remain untouched and maintenance remains healthy;
accumulating disk pressure or a stale backup backlog still fails health.

Deliberately unchanged: 10/10/30s polling, TU raw 300s, 2s tolerance and
last-durable-value `abs(new-old) > tolerance`, parser/schema/GTFS semantics,
Docker architecture/1536MiB limits, raw retention cutoff semantics, legacy
artifacts, and Aug 23 schema history. No new infrastructure/dependencies.

## Restart, memory and monitoring validation

Existing tests still cover first raw sample after reboot, midnight tail repair,
failed Parquet writes, deterministic spool cleanup/restart, forced raw fallback,
and non-overlapping/coalescing schedules. New tests cover real streamed Parquet,
mid-stream failure/restart, retention interruptions, null/heartbeat equivalence,
and monitoring. A randomized 1,000-poll tracker comparison includes failed
durable commits and matches an unexpired reference.

A synthetic six-hour workload introduced 360,000 unique full-policy identities:
the new tracker retained at most **30,000**, peak process RSS **42.26MiB**.
The identical workload with heartbeat expiry disabled retained **360,000**
entries and reached **343.60MiB** RSS (isolates expiry; not the entire old collector).
This is a bounded synthetic test, not a production peak-memory guarantee.
Production samples before the fix: collector ~947MiB, maintenance ~625.5MiB.
Container limits remain unchanged. After deployment check kernel OOM history
and day-long RSS, not only `docker inspect .State.OOMKilled`.

Healthchecks URL and HF credentials are configured; code gates the dead-man
ping on fresh healthy collector status, healthy maintenance and enabled backup.
No external Healthchecks account access was available to verify notification
delivery/grace settings. An unhealthy Docker state alone does not restart a
container; Docker restart policy handles exited processes. Weekly checks should
include failed pings/notifications, backlog, disk slope, spool age and RSS.

## Validation and deployment

Baseline: **57/57** tests passed. Expanded full suite: **88/88 passed** after
diff review (`Ran 88 tests in 6.349s`, `OK`); command is
`.venv/bin/python -m unittest discover -s tests -v`. Expected simulated disk-write
and tail-repair error logs are regression-test fixtures, not suite failures.
Syntax check: `.venv/bin/python -m compileall -q *.py tests`.
No configured linter/type checker exists. Docker build is deferred to the server
operator because the local WSL Docker integration is unavailable.

Production currently has a tracked modification to `monitoring.py` and an
untracked `monitoring.py.before-alert-stale-fix`. Preserve both. The tested new
version includes the Alerts behavior. These are operator-run steps; the audit
did not execute them:

```bash
ssh root@46.225.61.62
cd /root/bkk-collector-new
git status --short
git rev-parse HEAD                         # record rollback revision
git stash push -m 'preserve production Alerts patch before retention update' -- monitoring.py
git pull --ff-only
# Keep the data symlink, .env, and untracked historical patch backup intact.
nano .env
# Ensure: PRUNE_LOCAL_RAW_AFTER_DAYS=3
#         PRUNE_LOCAL_PARQUET_AFTER_DAYS=7
#         DISK_WARN_FREE_GB=8
#         DISK_CRITICAL_FREE_GB=4
# Keep existing credentials and 10/10/30s, 300s raw and 2s tolerance.
# Set COLLECTOR_GIT_COMMIT to the output of git rev-parse HEAD for provenance.
docker compose config --quiet
docker compose build
docker compose run --rm --no-deps maintenance python -m unittest discover -s tests -v
docker compose run --rm --no-deps maintenance python verify_backup.py 2026-09-03
# Proceed only after tests and recovery pass. Replacements happen here:
docker compose stop maintenance
docker compose up -d --no-deps collector
docker compose up -d --no-deps maintenance
```

Do not apply the old stash over the new monitoring implementation. Resizing
the provider disk/instance is an operator capacity action; provision ≥64GB
usable filesystem and confirm with `df -h` before a nine-month unattended run.

```bash
docker compose ps
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
docker stats --no-stream bkk_collector bkk_maintenance
jq '{healthy,reasons,warnings,disk_free_bytes,scheduler}' data/health/status.json
jq '{healthy,reasons,pending_backup_dates}' data/maintenance/status.json
tail -n 6 data/prune_history.jsonl
find data/spool -name 'batch-*.json.gz' -mmin +10 -print
du -sh data/raw data/parquet data/static_gtfs data/metadata
df -h .
docker compose logs --since=30m collector maintenance
journalctl -k --since='1 day ago' --no-pager | grep -Ei 'oom-kill|Killed process' || true
# After the next daily backup, verify its receipt's remote_revision and run:
# docker compose run --rm --no-deps maintenance python verify_backup.py YYYY-MM-DD
```

Retention runs on the maintenance backup retry schedule after 03:00 UTC.
Inspect a complete `parquet_prune` event and declining old Parquet cache size;
refusals protect data and include a log reason. No legacy/raw/static/journal
data should disappear through the Parquet path.

Rollback if necessary: stop maintenance, set both prune variables to `0`,
check out the recorded old commit, build, and replace the two services. Before
rebuilding the old collector, reapply only the saved Alerts monitoring patch
(the stash is retained) if returning to `3d16058`. Preserve the existing `.env`
and data symlink. New Parquet files keep the same schema/format; new receipt
fields are additive. Already evicted cache files remain on HF and can be
authenticated/restored at receipt revisions. Rolling back restores the known
OOM/unbounded-retention defects, so it is a temporary incident measure.
