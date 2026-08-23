# BKK realtime research collector

This service continuously collects BKK GTFS-Realtime VehiclePositions,
TripUpdates, and Alerts, plus content-addressed versions of BKK's static GTFS.
It is designed for a small Ubuntu VPS and prioritizes recoverability and clear
data-quality evidence over infrastructure complexity.

Data source attribution: **BKK Zrt., CC BY 4.0**.

## What is collected, at what resolution

Realtime feeds have independent monotonic schedules. Every request has its own
random `poll_id`, scheduled deadline, scheduler lag, missed-deadline count, and
UTC `request_started_at` / `response_received_at`. There is no synchronous
three-feed cycle.

| Feed | Raw protobuf default | Derived Parquet default |
| --- | --- | --- |
| VehiclePositions | every successful poll (10 s default) | every observation (10 s default) |
| TripUpdates | every 300 s, plus forced failure fallback | evaluated every poll (10 s); meaningful changes plus 30-minute heartbeat |
| Alerts | every successful poll (30 s default) | changed rows plus 30-minute heartbeat |

The raw archive is authoritative **at the timestamps it contains**. In
particular, a default five-minute TripUpdates raw archive cannot reconstruct
the 10-second derived change stream. The derived stream is therefore staged to
disk before dedup state advances and survives a collector crash, but it is not
equivalent to full 10-second raw protobuf history. The first successful sample
after every collector start is always raw-archived for every feed.

`DELAY_CHANGE_THRESHOLD_SECONDS=15` controls delay-field emission, while
`TRIPUPDATE_TIME_TOLERANCE_SECONDS` independently controls absolute
arrival/departure prediction revisions (5 seconds by default). Stored
values are never rounded. The identity is:

```text
entity_id + trip_id + start_date + start_time + stop_sequence + stop_id
```

This distinguishes repeated visits by one trip to the same stop. Every parsed
TripUpdates field is explicitly classified in `trip_update_policy.py`.
Meaningful schedule, trip-property, vehicle-assignment, uncertainty, stop-state,
and BKK-extension changes emit rows. Delay, predicted-time, and stop-distance
noise uses the configured tolerance against the last durably staged value.
Per-request/feed timestamps and enum-name aliases do not manufacture changes.
If an upstream stop update omits `stop_sequence`, its list ordinal is retained
as an explicit fallback identity rather than collapsing repeated stop IDs.

This is mode-agnostic. In particular, BKK metro TripUpdates often have null
delay fields, and live evidence now shows the same pattern for bus, tram,
trolleybus, HÉV, ferry, and unknown-route entities. Realtime predictions are
carried in absolute `arrival_time` / `departure_time`; sub-threshold jitter is
suppressed, but a revision larger than the prediction-time tolerance emits even
when every delay remains null.

For a fixed trip-instance/stop-visit key, scheduled time is constant, so
`delta(predicted - scheduled) == delta(predicted)`. Static GTFS is therefore not
loaded into the realtime hot path merely to make the emission decision. The
stored absolute prediction can be joined to the manifest's timestamp-selected static GTFS
offline to calculate semantic delay. Schedule relationships, trip properties,
and scheduled-time extension changes are tracked independently.

`CHANGE_TRACKER_NULL_GUARD_ROWS=1000` is a fail-loud schema/signal guard. If
that many consecutive TripUpdates rows have every standard delay and absolute
prediction field null, change tracking fails health, logs `CRITICAL`, and forces
full raw fallback snapshots. A tracker configured with no mutable fields is
rejected immediately at process startup.

To validate a threshold at 10-second cadence, temporarily set:

```dotenv
TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS=10
```

Then analyze the resulting raw log:

```bash
docker compose run --rm --no-deps maintenance python check_threshold.py \
  /data/raw/tripupdates/date=YYYY-MM-DD/tripupdates.rawlog \
  --field arrival_time --expected-interval 10 --require-high-frequency
```

With the production 300-second raw interval, the script prints a warning and
does not claim that five-minute transitions validate a 10-second threshold.
The exact revision distribution can be inspected without loading millions of
values into memory:

```bash
docker compose run --rm --no-deps maintenance python diagnostics.py prediction-revisions \
  --date YYYY-MM-DD --start-hour 7 --end-hour 11 --max-snapshots 40
```

Forty legacy five-minute snapshots supplied during this audit contained
3,612,217 comparable arrival predictions: 87.4% were identical, 11.50% moved
by more than 5 seconds, p90 was 13 seconds, p95 43 seconds, p99 100 seconds,
and the maximum was 12,490 seconds. This supports tracking absolute times and
preserving large revisions, but does **not** establish the optimal tolerance at
the new 10-second polling cadence. Five seconds is a deliberately
data-preserving provisional default. Re-run the diagnostic on temporary
10-second raw observations after deployment and revisit it using measured
emission/storage rates; outliers are reported and never clamped.

## Process architecture

`collector` is the realtime-only process. A dedicated scheduler owns one
anchored monotonic deadline and at most one active HTTP request per feed. A
slow Alerts or TripUpdates request cannot defer another feed. Missed deadlines
are counted and coalesced; they never become an unbounded request queue and a
feed never overlaps itself. The hard bound is one result awaiting processing
plus one subsequent request per feed.

The main collector thread remains the sole owner of protobuf parsing,
change-tracking, raw/spool writes, poll journals, and health state. This avoids
cross-feed durability races. As each response finishes, it is archived
before parsing when that feed's raw interval is due. A parse or spool failure
forces a raw snapshot for that poll. Derived rows are written to small atomic,
gzip-compressed spool segments; bounded-size commits prevent a long failed-write
backlog from being loaded into RAM at once. A successful, validated atomic
Parquet commit is the only event that removes those segments.

`maintenance` is a separate process/container. It performs:

- daily static GTFS checks and hash versioning;
- completed-day Parquet compaction;
- daily manifest generation and validation;
- Hugging Face uploads and remote verification;
- receipt-gated local raw pruning;
- external healthcheck pings.

A hung/crashed upload may delay backup, but cannot delay realtime polling.
Docker restarts the two processes independently.

## Crash and restart behavior

- Raw records are one append plus `fsync` by default. On startup, an invalid
  tail in today's log and the latest prior existing UTC partition for each feed
  is detached to `*.corrupt-tail-*` for forensic recovery;
  the proven-valid prefix then safely accepts new records. An atomic last-good
  byte checkpoint makes later restarts inspect only a possible trailing append;
  legacy logs are scanned once to establish that checkpoint.
- Spool files are atomically renamed into place. If Parquet writing fails,
  every segment remains pending and is retried. On startup, pending segments
  are flushed before the next poll.
- A crash after the Parquet rename but before spool cleanup is idempotent: the
  deterministic output is row-count validated, then the old segments are
  removed. If external/manual changes make the transaction ambiguous, cleanup
  stops and preserves source segments for inspection rather than risking loss.
- SIGTERM/SIGINT stops new submissions, waits for bounded in-flight requests,
  durably processes their results, forces a spool flush, and exits. If Parquet
  remains unavailable, the durable spool remains on disk.
- Every derived file and state JSON uses a same-directory temporary file plus
  `fsync` and atomic rename.
- Compaction writes and validates its output before deleting source parts. A
  transaction marker completes cleanup after a crash.

## Data layout

```text
data/
  raw/<feed>/date=YYYY-MM-DD/<feed>.rawlog
  raw/<feed>/date=YYYY-MM-DD/<feed>.rawlog.checkpoint.json
  parquet/<feed>/date=YYYY-MM-DD/part-*.parquet
  spool/<feed>/date=YYYY-MM-DD/batch-*.json.gz
  metadata/polls/<feed>/date=YYYY-MM-DD/polls.jsonl
  metadata/manifests/date=YYYY-MM-DD.json
  metadata/legacy_inventory.json       # only after explicit migration
  static_gtfs/
    versions/<sha256>.zip
    history.jsonl
    state.json
    budapest_gtfs_YYYY-MM-DD.zip       # preserved legacy files, if any
  health/status.json
  health/freshness_state.json
  maintenance/status.json
  backup_receipts/date=YYYY-MM-DD.json
  backup_receipts/legacy/date=YYYY-MM-DD.json
  backup_history.jsonl
  backup_status.json
  prune_history.jsonl
  logs/collector.log
  logs/maintenance.log
  parquet_rebuild_previous/...        # rollback copies created by rebuilds
```

Raw log framing remains backward compatible:

```text
8-byte big-endian float64 response timestamp
4-byte big-endian uint32 compressed length
gzip-compressed protobuf bytes
```

Poll journals contain the per-feed configured cadence, scheduler deadline/lag,
coalesced missed deadlines, HTTP status, request/response timestamps, latency,
payload size/SHA-256, feed header timestamp, min/max entity timestamps where
available, raw/parse/spool outcomes, row counts, and freshness incidents.

Parquet schema version 2 keeps scalar research fields plus canonical JSON for
repeated/nested structures. It includes current standard GTFS-RT metadata,
vehicle/trip/stop-event/alert fields and BKK's published realCity fields:
vehicle model/type, deviated/door state, stop distance, scheduled stop events,
and BKK alert text/route details. The original protobuf remains the fallback
for unknown future extensions. New schema versions may add nullable columns;
when querying mixed historical files, use schema unioning (for example DuckDB
`read_parquet(..., union_by_name=true)`).

## Daily manifests and backup semantics

For every completed UTC day, maintenance builds a manifest with:

- per-feed cadence and expected/attempted/successful/failed polls;
- first and last successes;
- raw snapshot counts and raw-log integrity;
- parsed/emitted/Parquet row counts;
- stale-feed incidents and pending spool segments;
- applicable static GTFS hash/version;
- size and SHA-256 for every required artifact;
- collector schema/version/optional git commit;
- separate `complete` and `quality_ok` results.

`complete` means required local artifacts are internally consistent and can be
backed up. `quality_ok` is stricter evidence about polling gaps/staleness. A
real HTTP-200 empty feed has a raw protobuf, poll evidence, and a typed empty
Parquet artifact. Missing collection does not masquerade as an empty feed.

Backup backlog is derived as:

```text
completed local dates - dates with verified v2 receipts
```

The worker retries older failures automatically. Explicitly inventoried legacy
dates use a separate path described below and cannot starve newer complete v2
dates. It uploads each
manifest-listed file idempotently, then checks every remote path and size.
For Hugging Face LFS objects it also compares the remote LFS SHA-256. A local
receipt is written only after all checks pass. Non-LFS remote verification is
path+size (the manifest still stores the local SHA-256), so do not interpret it
as an independent download-and-rehash proof.
The receipt is itself uploaded and remotely verified before its local final
name is installed.

`backup_success_dates.txt` from collector v1 is preserved but deliberately not
trusted, because v1 could record a partial date. Only files under
`backup_receipts/` authorize new pruning. Old dates without v2 poll journals
are never falsely upgraded to v2 complete and are never newly pruned
automatically.

## Explicit legacy inventory and off-site copy

Run migration manually; it is intentionally not part of collector startup.
The dry run hashes and reports every discovered raw/Parquet/static artifact but
does not write, rename, delete, or upload anything:

```bash
docker compose run --rm maintenance python migrate_legacy.py --dry-run
docker compose run --rm maintenance python migrate_legacy.py
```

The second command atomically writes `metadata/legacy_inventory.json`, uploads
existing legacy artifacts when Hugging Face credentials are available, checks
remote path/size and LFS SHA-256, and writes separate legacy receipts. Original
files remain byte-for-byte untouched. Classifications are:

- `v2_complete`: a complete v2 manifest and all v2 evidence validate;
- `legacy_present_but_completeness_unverifiable`: required legacy artifacts are
  readable, but poll evidence that never existed cannot be reconstructed;
- `corrupt_or_missing`: one or more expected artifacts are absent, truncated,
  staged, or unreadable; existing bytes are still inventoried/copied.

Legacy receipts always state `scientific_complete: false`; they never authorize
pruning. Re-running the command is idempotent and rechecks remote matches. Once
explicitly inventoried, a legacy date is removed from the v2 backup worklist so
it cannot consume retry time needed by newer dates. If credentials were absent,
the maintenance status warns that its remote copy is unconfirmed; rerun the
same command after configuring credentials.

When `PRUNE_LOCAL_RAW_AFTER_DAYS` is positive, raw date directories older than
that age are removed only if a receipt exists and every still-local raw file
still matches the receipt. Parquet, manifests, static GTFS, and receipts are not
auto-pruned. Set the value to `0` for no automatic raw pruning.

Hugging Face is one off-server copy, not a complete 3-2-1 backup strategy. For
irreplaceable research, periodically replicate the dataset repo to a second
provider or offline disk.

## Static GTFS version history

Maintenance downloads and validates the current ZIP at least daily. It checks
ZIP integrity and required tables, computes SHA-256, and stores a new
`versions/<sha256>.zip` only when the content is new. `history.jsonl` records
every successful observation time, content hash, size, path, and `feed_version`
when present. Existing `budapest_gtfs_YYYY-MM-DD.zip` files are indexed in place
without moving or deleting them.

Daily manifests contain a timestamped `static_gtfs_timeline`, so an intraday
version change is not hidden by a single end-of-day hash. Timestamped joins use
the latest version the collector had actually observed at that instant and
never silently substitute the newest ZIP. Because checks are daily, the exact
BKK publication instant remains bounded between two observations; the project
claims an exact collector-observed version, not an unknowable publication
instant. Legacy dated ZIPs are explicitly labeled
`legacy_schedule_uncertain`; missing legacy versions are never inferred.
Every distinct static archive known locally, including preserved legacy
archives, is manifest-listed for off-server backup. Previously verified LFS
objects are checksum-matched and skipped on retries/daily reuse.

Inspect TripUpdate-to-static join quality for a UTC date with:

```bash
docker compose run --rm --no-deps maintenance python diagnostics.py static-join --date 2026-08-22
```

The report samples the archived full raw snapshots at their configured cadence,
streams them into a disk-backed exact join index, reports
trip and `(trip_id, stop_sequence, stop_id)` match rates by mode, lists
unmatched route IDs, and warns about low mode-specific rates. Unmatched rows,
including integrated/external services such as `IC`, `IR`, `Ex`, `S10`, `S40`,
`S70`, and `Z72`, are valid realtime observations: they are reported but never
dropped or treated as collector failure. A stale/missing legacy static version
is reported as schedule-uncertain, not replaced with a newer schedule.

## Health and staleness

HTTP 200 alone is not healthy. The collector persists and evaluates:

- feed header timestamp age;
- unchanged header timestamp duration;
- unchanged entity-content hash duration (excluding the changing feed header);
- time since last successful response;
- HTTP, protobuf parse, raw write, spool, and Parquet commit failures separately;
- per-feed scheduler lag, in-flight duration, and coalesced missed deadlines;
- free disk space.

Alerts use a one-day default unchanged-payload warning, and empty Alerts
content does not trigger it while its header advances, because legitimately
unchanged alerts are common. That warning is evidence in feed status/manifests,
not a liveness failure by itself. Tune thresholds if BKK's header semantics
produce false positives; do not disable absence/disk checks casually.

Docker marks `collector` unhealthy when `health/status.json` is stale or
degraded. `maintenance` independently checks daily static freshness and a stale
v2 backup backlog. `HEALTHCHECK_URL`, when configured, is called only by the
maintenance process and only while both status files are healthy and off-site
backup is enabled. Disabled backup remains a visible maintenance warning.

Verify health:

```bash
docker compose ps
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
docker compose logs --since=30m collector maintenance
docker stats --no-stream bkk_collector bkk_maintenance
du -sh data/raw data/parquet data/spool data/static_gtfs
df -h .
```

Useful evidence:

```bash
jq . data/health/status.json
jq . data/maintenance/status.json
jq . data/metadata/manifests/date=YYYY-MM-DD.json
jq . data/backup_receipts/date=YYYY-MM-DD.json
```

Any spool segments older than the configured five-minute flush, repeated
`CRITICAL`, `Parquet flush failed`, stale-source reasons, low disk, or a growing
backup backlog requires investigation.

The 1536 MiB Compose values are limits, not reservations. They remain at the
existing conservative defaults. Raw/static hashing and downloads stream in
1 MiB blocks; rebuilds use bounded row chunks; compaction iterates 65,536-row
record batches; spool commits consume at most 20 segments. Daily journals are
the only completed-day structure materialized by manifest generation and are
small enough at the supported cadence. Use `docker stats` to look for a rising
baseline or a maintenance process repeatedly approaching its limit. An OOM in
maintenance cannot kill or block the separately limited collector, and atomic
compaction markers preserve recovery state. Heavy archive diagnostics are run
as one-off `maintenance` containers, not inside the realtime collector's memory
cgroup; schedule full-day reports away from a CPU-constrained peak period.

To validate the currently deployed BKK realCity population without archiving a
probe or exposing the key, run:

```bash
docker compose run --rm --no-deps maintenance python diagnostics.py live-schema
```

It prints aggregate entity/extension/non-null-field counts only. An omitted or
changed extension produces null derived fields rather than failing collection;
the raw protobuf remains authoritative.

## Configuration

Copy `.env.example` to `.env`. Existing environment names are retained. New
variables all have safe defaults; the most important are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `BKK_API_KEY` | required | BKK realtime key |
| `DATA_DIR` | `/data` | mounted persistent data root |
| `VEHICLE_POSITIONS_INTERVAL_SECONDS` | `10` | independent VehiclePositions cadence |
| `TRIP_UPDATES_INTERVAL_SECONDS` | `10` | independent TripUpdates cadence |
| `ALERTS_INTERVAL_SECONDS` | `30` | independent Alerts cadence |
| `POLL_INTERVAL_SECONDS` | unset | deprecated fallback for omitted feed cadences |
| `PARQUET_FLUSH_MINUTES` | `5` | maximum normal spool residence |
| `TRIPUPDATES_RAW_ARCHIVE_SECONDS` | `300` | TripUpdates raw cadence |
| `TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS` | `0` | optional temporary faster raw cadence |
| `DELAY_CHANGE_THRESHOLD_SECONDS` | `15` | TripUpdates event tolerance |
| `TRIPUPDATE_TIME_TOLERANCE_SECONDS` | `5` | absolute arrival/departure prediction tolerance; the older `PREDICTION_TIME_CHANGE_THRESHOLD_SECONDS` name remains a deprecated fallback |
| `BKK_STOP_DISTANCE_CHANGE_THRESHOLD` | inherits `15` | BKK stop-distance tolerance |
| `CHANGE_TRACKER_NULL_GUARD_ROWS` | `1000` | all-null prediction signal fail-loud threshold |
| `HEARTBEAT_SECONDS` | `1800` | forced dedup heartbeat |
| `HF_TOKEN`, `HF_REPO_ID` | empty | enable private dataset backup |
| `BACKUP_HOUR_UTC` | `3` | earliest daily backup hour |
| `BACKUP_RETRY_SECONDS` | `900` | pending backlog retry interval |
| `PRUNE_LOCAL_RAW_AFTER_DAYS` | `14` | receipt-gated raw retention; `0` disables |
| `STATIC_GTFS_CHECK_INTERVAL_SECONDS` | `86400` | successful static check interval |
| `FEED_STALE_SECONDS` | `180` | source timestamp age limit |
| `FEED_ABSENT_SECONDS` | `180` | no-success limit |
| `FROZEN_PAYLOAD_SECONDS` | `300` | VP/TU unchanged limit |
| `ALERTS_FROZEN_PAYLOAD_SECONDS` | `86400` | Alerts unchanged warning limit |
| `DISK_WARN_FREE_GB` | `2.0` | unhealthy warning threshold |
| `DISK_CRITICAL_FREE_GB` | `0.5` | critical disk threshold |
| `PARQUET_COMPACTION_ENABLED` | `true` | completed-day background compaction |
| `HEALTHCHECK_URL` | empty | optional external dead-man ping |
| `COLLECTOR_MEMORY_LIMIT` | `1536m` | Compose collector memory limit |
| `MAINTENANCE_MEMORY_LIMIT` | `1536m` | Compose maintenance memory limit |

See `.env.example` for HTTP bounds and the remaining maintenance settings.

## Initial deployment

On Ubuntu with Docker Engine and Compose v2:

```bash
git clone YOUR_REPOSITORY_URL bkk_collector
cd bkk_collector
cp .env.example .env
nano .env
docker compose build
docker compose up -d
docker compose ps
docker compose logs -f --tail=100 collector maintenance
```

Do not put tokens in Compose YAML or commit `.env`. `.dockerignore` excludes the
data directory and secrets from Docker build context—important once raw data is
large.

## Safe upgrade from collector v1

Building and legacy inventory do not stop the realtime service, so do both
first. Do not overwrite the server's existing `.env`; add the three explicit
cadences. A remaining `POLL_INTERVAL_SECONDS` is harmless and deprecated because
feed-specific values win. Set pruning to zero for the first 24–48 hours while
you inspect v2 receipts.

Ensure the existing `.env` contains:

```dotenv
VEHICLE_POSITIONS_INTERVAL_SECONDS=10
TRIP_UPDATES_INTERVAL_SECONDS=10
ALERTS_INTERVAL_SECONDS=30
TRIPUPDATES_RAW_ARCHIVE_SECONDS=300
TRIPUPDATE_TIME_TOLERANCE_SECONDS=5
CHANGE_TRACKER_NULL_GUARD_ROWS=1000
PRUNE_LOCAL_RAW_AFTER_DAYS=0
```

```bash
cd /path/to/bkk_collector
git status --short
git pull --ff-only
docker compose build
docker compose run --rm --no-deps collector python -m unittest discover -s tests -v
docker compose config --quiet
docker compose stop maintenance
docker compose run --rm --no-deps maintenance python migrate_legacy.py --dry-run
docker compose run --rm --no-deps maintenance python migrate_legacy.py
docker compose up -d --no-deps collector
docker compose up -d maintenance
docker compose ps
docker compose logs --since=10m collector maintenance
```

Compose sends SIGTERM to v1 and honors the grace period, allowing its RAM
buffer to flush before replacement. The collector interruption is normally a
few seconds. Existing `raw/`, `parquet/`, dated static ZIPs, state, and backup
files are not renamed or deleted by migration. V2 begins writing additional
directories beside them.

Failure handling is intentionally isolated:

- reboot/startup takes a first raw sample immediately and repairs today's plus
  the latest prior UTC raw tails;
- two days of Hugging Face outage leave dates pending while realtime continues;
- maintenance OOM/restart leaves atomic download/compaction state retryable and
  cannot consume the collector container's memory limit;
- staged Parquet survives collector restart and commits before normal polling;
- low/critical disk, write failures, stale HTTP-200 data, and scheduler misses
  make data health unhealthy instead of being hidden by process liveness;
- a missing/changed realCity extension yields nullable derived columns while raw
  bytes preserve the unknown message;
- a request slower than its interval skips/coalesces that feed's deadlines,
  never overlaps itself, and does not delay the other two schedulers.

After one completed UTC day:

```bash
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
ls -l data/metadata/manifests data/backup_receipts
find data/spool -name 'batch-*.json.gz' -mmin +10 -print
```

## Rebuilding derived Parquet

Rebuilds stream rows in bounded chunks and never delete the installed
partition before a complete replacement exists. The old partition is moved to
`data/parquet_rebuild_previous/` for rollback.

```bash
# Prevent maintenance from compacting/backing up a partition mid-rebuild.
# Realtime collection can continue when only completed dates are rebuilt.
docker compose stop maintenance

# One completed day/feed (recommended first)
docker compose run --rm collector python rebuild_parquet.py \
  --date YYYY-MM-DD --feed tripupdates

# All completed raw dates/feeds
docker compose run --rm collector python rebuild_parquet.py
docker compose start maintenance
```

By default, a corrupt raw tail or any parse failure leaves the current Parquet
untouched. `--allow-parse-errors` is an explicit acceptance of partial derived
output and should only be used after inspecting the raw issue. Rebuilding the
current UTC date is refused unless explicitly overridden; stop the collector
before using that override.

Rebuild limitations:

- it reconstructs only raw snapshot timestamps;
- default TripUpdates history is five-minute, not the live 10-second event
  stream;
- v1 raw framing contains only response timestamp and bytes, so exact v2
  request-start time/poll grouping is synthesized during rebuild;
- an upstream interval in which no successful response was archived cannot be
  recovered later.

Each replaced partition remains under `data/parquet_rebuild_previous/`. To
roll back, stop maintenance, move the installed derived partition aside, move
the chosen previous directory back to
`data/parquet/<feed>/date=YYYY-MM-DD`, and restart maintenance. Raw archives
are never changed by a rebuild.

## Tests

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m compileall -q .
```

The suite covers same-stop TripUpdates identity, write-failure retention,
restart recovery, UTC midnight partitioning, stale/frozen detection, missing
protobuf optionals, multi-period/multilingual/BKK alerts, raw-tail recovery,
static hash behavior, real PyArrow schemas, compaction row preservation, empty
versus missing daily artifacts, backup retry/remote verification, independent
scheduler behavior, and a full synthetic three-feed collection pass.

## Deployment rollback

Rollback changes code/containers only; never roll back or replace `data/`.
Record the pre-upgrade commit before pulling. If the new collector is unhealthy,
check out that commit, rebuild, and replace the services. Data written by v2 is
left in place; the old code ignores unfamiliar metadata/spool directories.

```bash
docker compose stop maintenance
git switch --detach PRE_UPGRADE_COMMIT
docker compose build collector
docker compose up -d --no-deps collector
docker compose ps
```

Keep the older maintenance worker stopped: it does not understand the new
legacy-inventory protections. If the rollback target predates the separate
maintenance architecture, also blank `HF_TOKEN` and `HF_REPO_ID` in that old
collector's environment so synchronous uploads cannot block realtime polling.
Return to the maintained branch after diagnosis
with `git switch main`, rebuild, and restart both services. Do not delete v2
raw, spool, Parquet, journal, manifest, inventory, or receipt files.
