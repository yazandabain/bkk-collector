# BKK realtime research collector

This service continuously collects BKK GTFS-Realtime VehiclePositions,
TripUpdates, and Alerts, plus content-addressed versions of BKK's static GTFS.
It is designed for a small Ubuntu VPS and prioritizes recoverability and clear
data-quality evidence over infrastructure complexity.

Data source attribution: **BKK Zrt., CC BY 4.0**.

## What is collected, at what resolution

The default realtime poll interval is 30 seconds. Every feed request has its
own UTC `request_started_at` and `response_received_at`; a shared random
`poll_id` groups the three requests from one cycle.

| Feed | Raw protobuf default | Derived Parquet default |
| --- | --- | --- |
| VehiclePositions | every successful poll (~30 s) | every observation |
| TripUpdates | every 300 s | evaluated every poll; changed rows plus 30-minute heartbeat |
| Alerts | every successful poll (~30 s) | changed rows plus 30-minute heartbeat |

The raw archive is authoritative **at the timestamps it contains**. In
particular, a default five-minute TripUpdates raw archive cannot reconstruct
the 30-second derived change stream. The derived stream is therefore staged to
disk before dedup state advances and survives a collector crash, but it is not
equivalent to full 30-second raw protobuf history.

`DELAY_CHANGE_THRESHOLD_SECONDS=15` only controls whether a TripUpdates row is
emitted. Stored delays are never rounded. The identity is:

```text
entity_id + trip_id + start_date + start_time + stop_sequence + stop_id
```

This distinguishes repeated visits by one trip to the same stop. Changes to
categorical schedule/vehicle fields also emit a row. Delay and predicted-time
fields use the configured tolerance against the last durably staged value.
If an upstream stop update omits `stop_sequence`, its list ordinal is retained
as an explicit fallback identity rather than collapsing repeated stop IDs.

To validate a threshold at 30-second cadence, temporarily set:

```dotenv
TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS=30
```

Then analyze the resulting raw log:

```bash
docker compose exec collector python check_threshold.py \
  /data/raw/tripupdates/date=YYYY-MM-DD/tripupdates.rawlog \
  --expected-interval 30 --require-high-frequency
```

With the production 300-second raw interval, the script prints a warning and
does not claim that five-minute transitions validate a 30-second threshold.

## Process architecture

`collector` is the realtime-only process. The three requests run concurrently
with bounded connect/read retries. As each response finishes, it is archived
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
  tail in today's log is detached to `*.corrupt-tail-*` for forensic recovery;
  the proven-valid prefix then safely accepts new records. An atomic last-good
  byte checkpoint makes later restarts inspect only a possible trailing append;
  legacy logs are scanned once to establish that checkpoint.
- Spool files are atomically renamed into place. If Parquet writing fails,
  every segment remains pending and is retried. On startup, pending segments
  are flushed before the next poll.
- A crash after the Parquet rename but before spool cleanup is idempotent: the
  deterministic output is row-count validated, then the old segments are
  removed. This can create a recoverable duplicate only if external/manual
  changes defeat that transaction; loss is preferred against duplicates.
- SIGTERM/SIGINT stops new polls, forces a spool flush, and exits. If Parquet
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
  static_gtfs/
    versions/<sha256>.zip
    history.jsonl
    state.json
    budapest_gtfs_YYYY-MM-DD.zip       # preserved legacy files, if any
  health/status.json
  health/freshness_state.json
  maintenance/status.json
  backup_receipts/date=YYYY-MM-DD.json
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

Poll journals contain HTTP status, request/response timestamps, latency,
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

- expected/attempted/successful/failed polls per feed;
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

The worker retries older failures automatically and skips permanently
incomplete legacy dates without starving newer complete dates. It uploads each
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
remain visibly incomplete and are never newly pruned automatically.

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

The applicable version in a daily manifest is the newest version observed by
the end of that UTC day. Because checks are daily, the exact BKK publication
instant is only bounded between two observations; the project does not claim
finer historical applicability.
Every distinct static archive known locally, including preserved legacy
archives, is manifest-listed for off-server backup. Previously verified LFS
objects are checksum-matched and skipped on retries/daily reuse.

## Health and staleness

HTTP 200 alone is not healthy. The collector persists and evaluates:

- feed header timestamp age;
- unchanged header timestamp duration;
- unchanged entity-content hash duration (excluding the changing feed header);
- time since last successful response;
- parse/raw/spool/Parquet failures;
- free disk space.

Alerts use a one-day default unchanged-payload warning, and empty Alerts
content does not trigger it while its header advances, because legitimately
unchanged alerts are common. That warning is evidence in feed status/manifests,
not a liveness failure by itself. Tune thresholds if BKK's header semantics
produce false positives; do not disable absence/disk checks casually.

Docker marks `collector` unhealthy when `health/status.json` is stale or
degraded. `maintenance` independently checks daily static freshness and a stale
v2 backup backlog. `HEALTHCHECK_URL`, when configured, is called only by the
maintenance process and only while both status files are healthy.

Verify health:

```bash
docker compose ps
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
docker compose logs --since=30m collector maintenance
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

## Configuration

Copy `.env.example` to `.env`. Existing environment names are retained. New
variables all have safe defaults; the most important are:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `BKK_API_KEY` | required | BKK realtime key |
| `DATA_DIR` | `/data` | mounted persistent data root |
| `POLL_INTERVAL_SECONDS` | `30` | realtime cycle cadence |
| `PARQUET_FLUSH_MINUTES` | `5` | maximum normal spool residence |
| `TRIPUPDATES_RAW_ARCHIVE_SECONDS` | `300` | TripUpdates raw cadence |
| `TRIPUPDATES_ANALYSIS_SAMPLE_SECONDS` | `0` | optional temporary faster raw cadence |
| `DELAY_CHANGE_THRESHOLD_SECONDS` | `15` | TripUpdates event tolerance |
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

Building does not stop the old container, so build first. Do not overwrite the
server's existing `.env`; merge new defaults into it. For the first 24–48 hours,
setting `PRUNE_LOCAL_RAW_AFTER_DAYS=0` is a conservative way to inspect v2
receipts before re-enabling pruning.

```bash
cd /path/to/bkk_collector
git status --short
git pull --ff-only
docker compose build
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
- default TripUpdates history is five-minute, not the live 30-second event
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
versus missing daily artifacts, backup retry/remote verification, and a full
synthetic three-feed collection cycle.
