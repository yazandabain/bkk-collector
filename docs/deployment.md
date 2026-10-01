# Deployment and recovery

Use Docker Compose on a host with sufficient disk, memory, and remote archive
capacity. Replace the SSH destination, checkout path, rollback path, and commit
placeholders below with private deployment values. Keep those values, `.env`,
collected data, receipts, and operational logs out of Git.

Run each section in order, in the same shell. Stop on a failed check. Never
delete data to make an update proceed, run two collectors against one data
directory, or use `docker compose down -v`.

## Preflight

~~~bash
ssh SSH_USER@HOST
cd /path/to/checkout
set -e

OLD_REVISION=$(git rev-parse HEAD)
TARGET_REVISION=FULL_TARGET_COMMIT_SHA
ROLLBACK_DIR=/path/to/private-rollback
git diff --exit-code
git diff --cached --exit-code
git status --short
readlink -f data
docker compose config --quiet
docker compose ps
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
docker stats --no-stream
df -h .
du -sh data/raw data/parquet data/spool data/static_gtfs data/metadata
~~~

Preserve untracked files and existing data mounts. Check archive quota and
recent storage growth: verified retention bounds raw/Parquet caches, not
protected journals or static history. Do not publish expanded Compose
configuration, container environments, or unredacted logs.

## Preserve rollback assets and test the replacement

Build while existing containers continue collecting. Save the actual running
collector image rather than relying on a tag that the build will overwrite.

~~~bash
COLLECTOR_ID=$(docker compose ps -q collector)
COLLECTOR_IMAGE=$(docker inspect --format '{{.Config.Image}}' "$COLLECTOR_ID")
ROLLBACK_IMAGE="bkk-collector-rollback:${OLD_REVISION:0:12}"
docker image tag "$(docker inspect --format '{{.Image}}' "$COLLECTOR_ID")" "$ROLLBACK_IMAGE"
test ! -e "$ROLLBACK_DIR"
install -d -m 700 "$ROLLBACK_DIR"
install -m 600 .env "$ROLLBACK_DIR/.env"
printf '%s\n' "$OLD_REVISION" > "$ROLLBACK_DIR/revision"

git fetch origin
git cat-file -e "$TARGET_REVISION^{commit}"
git switch --detach "$TARGET_REVISION"
docker compose config --quiet
docker compose build collector maintenance
TARGET_IMAGE=$(docker image inspect --format '{{.Id}}' "$COLLECTOR_IMAGE")
docker run --rm --network none --memory 1536m "$TARGET_IMAGE" python -m compileall -q .
docker run --rm --network none --memory 1536m "$TARGET_IMAGE" python -m unittest discover -s tests -v
~~~

These tests have no production mount, credentials, or network. Do not replace
them with `docker compose run collector`, which mounts production data.
Retain the saved image; do not run image/volume pruning during the update.

Set the existing provenance setting without changing collection configuration:

~~~bash
if grep -q '^COLLECTOR_GIT_COMMIT=' .env; then
  sed -i "s/^COLLECTOR_GIT_COMMIT=.*/COLLECTOR_GIT_COMMIT=$TARGET_REVISION/" .env
else
  printf '\nCOLLECTOR_GIT_COMMIT=%s\n' "$TARGET_REVISION" >> .env
fi
docker compose config --quiet
~~~

## Replace and verify

Stop maintenance first so an older version cannot finalize manifests without
new evidence. Allow grace for in-flight requests and durable writes. An update
can create a short collection gap; building beforehand minimizes downtime.

~~~bash
UPDATE_STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
docker compose stop -t 180 maintenance
docker compose stop -t 180 collector
docker inspect --format 'exit={{.State.ExitCode}} oom={{.State.OOMKilled}}' "$COLLECTOR_ID"
docker compose up -d --no-build --no-deps --force-recreate collector
test "$(docker inspect --format '{{.Image}}' "$(docker compose ps -q collector)")" = "$TARGET_IMAGE"
docker compose up -d --no-build --no-deps --force-recreate maintenance
docker compose ps
~~~

Investigate an OOM or forced kill rather than assuming clean shutdown. After
initial successful observations, run both healthchecks and inspect persistence:

~~~bash
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
jq '{healthy, reasons, warnings, pending_spool_segments, parquet_worker, scheduler}' data/health/status.json
docker compose exec -T collector python diagnostics.py collection-window --since "$UPDATE_STARTED_AT"
docker stats --no-stream
STEADY_STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
~~~

After ten minutes, repeat the healthchecks and the diagnostic using
`--since "$STEADY_STARTED_AT"`. At default cadences, expect approximately
60/60/20 VP/TU/Alerts observations, allowing boundary variation. Check:

- no HTTP, parse, storage, or presence failures;
- successful background commits and a stable spool backlog;
- a first raw TU sample and presence baseline, followed by confirmed presence
  observations even when prediction rows are suppressed;
- run provenance matching the deployed commit and intended configuration;
- disk trend and memory use within operational limits.

Do not lower polling cadence to conceal sustained scheduler misses.

## Authenticate a completed backup

Wait for normal maintenance to back up a completed upgraded UTC date. Do not
launch a competing backup worker.

~~~bash
VERIFIED_DATE=YYYY-MM-DD
jq '{complete, quality_ok, completeness_errors, quality_flags}' "data/metadata/manifests/date=$VERIFIED_DATE.json"
jq -e '.remote_verified == true and (.remote_revision | length) > 0 and
       any(.artifacts[]; .kind == "tripupdates_presence" and (.path | endswith("presence.jsonlog"))) and
       any(.artifacts[]; .kind == "collector_run_metadata")' "data/backup_receipts/date=$VERIFIED_DATE.json"
docker compose run --rm --no-deps maintenance python verify_backup.py "$VERIFIED_DATE"
~~~

The restore check downloads representatives into temporary storage and verifies
size/SHA-256 at the receipt revision. Old receipts do not prove new presence
evidence. Never rewrite them to imply otherwise.

## Rollback

Rollback changes code and containers, not data. Preserve current credentials;
do not blindly restore a configuration backup that may predate key rotation.

~~~bash
docker compose stop -t 120 maintenance
docker compose stop -t 120 collector
git switch --detach "$OLD_REVISION"
if grep -q '^COLLECTOR_GIT_COMMIT=' .env; then
  sed -i "s/^COLLECTOR_GIT_COMMIT=.*/COLLECTOR_GIT_COMMIT=$OLD_REVISION/" .env
fi
docker image tag "$ROLLBACK_IMAGE" "$COLLECTOR_IMAGE"
docker compose up -d --no-build --no-deps --force-recreate collector
docker compose exec -T collector python healthcheck.py
~~~

Keep older maintenance stopped if it cannot archive the new presence/run
evidence. Preserve all spool segments, journals, checkpoints, manifests,
receipts, and static history. A later compatible collector starts a new
presence baseline; it cannot reconstruct observations missed during rollback.
Roll forward promptly to a tested collector and matching maintenance version.
