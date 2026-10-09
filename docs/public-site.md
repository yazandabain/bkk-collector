# Public observatory

The public site is a read-only window into the collector, not another part of
ingestion. It shows reported vehicle positions, current data health and counts
from collection evidence. It does not provide journey planning, historical
analytics or learned arrival predictions.

```text
collector evidence (read-only mounts)
  → isolated public-exporter
  → authenticated HTTPS publication of one sanitized snapshot
  → Cloudflare Worker + one R2 Standard object
  → static React site and same-origin /api/snapshot
```

The existing collector and maintenance containers have no new dependencies,
ports or credentials. The exporter has no BKK/Hugging Face tokens, Docker socket
or writable collector mount. A public outage cannot stop collection. Starting
or stopping the exporter never requires restarting either existing service.

## Data contract and interpretation

[`shared/snapshot.ts`](../frontend/shared/snapshot.ts) defines schema version 1
and validates it in the Worker and browser. The publisher is a closed projection,
not a proxy for operational JSON. Unknown fields are refused before publication.
Only `/api/snapshot` is public; `/api/publish` accepts a bounded JSON body with a
separate machine token, writes a fixed object key and refuses older snapshots.
Conditional R2 writes prevent a competing publication from overwriting a newer
object. There is no public control endpoint or arbitrary object access.

The private upload uses `application/vnd.bkk-observatory.snapshot.v1+json`:
each vehicle is an eight-field tuple `[id, longitude, latitude, route_label,
mode, color, bearing, recorded_at]`. This avoids thousands of repeated GeoJSON
keys in the free Worker's CPU-limited ingress path. Every field is validated;
the Worker expands and stores canonical GeoJSON. Local exporter output and
`/api/snapshot` remain schema version 1; stored/public JSON is capped at 2 MiB
of UTF-8. Old `application/json` publishers are still accepted. Deploy the dual-format Worker
before updating the exporter; there is no collector or historical-data migration.

- Vehicle data comes from the latest **checkpointed** raw VehiclePositions
  frame. The exporter seeks through record headers once and reads only new
  committed frames afterward; it never repairs the source. It decompresses only
  the latest bounded frame. Source observation, vehicle and feed-header times
  remain distinct from public snapshot generation time.
- Route labels/colors come from the hash-verified current static `routes.txt`,
  cached until its content hash changes. Missing assignments and integrated
  services remain visible as “Other / unassigned”. There is no historical static
  join, filtering of unmatched research rows or position interpolation.
- Original vehicle/entity IDs, license plates, trip IDs, error messages, paths,
  disk details, credentials and backup object listings are not public. Map IDs
  are stable truncated hashes of public-source identifiers, not a claim of
  cryptographic anonymization. Invalid/missing coordinates are not plotted;
  omitted record counts are explicit. These choices affect only the display.
- Health is a whitelist of states and issue codes. HTTP/parse/storage/source
  failures differ from unchanged-alert advisories. A stale public snapshot means
  **current collector health is unknown**, not that the collector has failed.
- Statistics count completed-day manifests with poll evidence and verified v2
  backup receipts. They are not uptime, unique journeys or scientific completeness.
  Older manifests may lack successful-persistence counts; the site says “Not
  measured” rather than inventing zero. Verified receipts prove backup integrity,
  not perfect source coverage. Metadata counts are cached for five minutes.

The exporter ticks every 10 seconds with at most one bounded upload in flight.
Errors retry next tick without an accumulating queue. The browser refreshes every
15 seconds, pauses while hidden and retains last-known data with an explicit
staleness notice. Public snapshots older than 90 seconds are not current; old
source timestamps are separately flagged. R2 stores only `latest.json`, not an
ever-growing public history. Actual research data stays in the existing archive.

## Local verification

Python 3.12 uses the existing root requirements; Node 22.12+ uses the separate
frontend lockfile. Tests use synthetic fixtures and require no production data,
credentials or BKK/Hugging Face/Cloudflare services.

```bash
python -m pip install -r requirements.txt
python -m compileall -q .
python -m unittest discover -s tests -v
python -m unittest discover -s public_exporter/tests -v

cd frontend
npm ci
npm run lint
npm test
npm run build
npm run worker:check
npx playwright install --with-deps chromium
npm run test:e2e
```

`npm run dev` serves the UI; without a published snapshot it intentionally shows
unavailable data, not a fake live fleet. To privately inspect an exported file:

```bash
python -m public_exporter --once --no-publish \
  --input /path/to/read-only-inputs --output /path/to/private-output
```

Inputs are directories `raw`, `health`, `maintenance`, `manifests`, `receipts`
and `static`, mapped as in `docker-compose.public.yml`. Do not check copied
operational files or production protobuf into Git.

## Deploy the site

Use the same Cloudflare account for the Worker and the R2 bucket. Enable **R2
Standard** in the dashboard; this may require a billing method. No paid Workers
plan is required. Review current allowances in [Workers pricing](https://developers.cloudflare.com/workers/platform/pricing/)
and [R2 pricing](https://developers.cloudflare.com/r2/pricing/) before deployment.
At a 10-second publishing cadence there are about 260,000 R2 writes/month; the
current Standard allowance is one million. One latest snapshot uses negligible
storage. Free Workers has a 100,000/day dynamic-request limit shared by publication
and public reads; static assets have separate free serving. A popular site may
exhaust the free dynamic allowance and show stale data without affecting ingestion.
Do not enable a paid subscription automatically to mask that condition.

Free Workers also has a 10 ms CPU budget per request; network waits do not count.
Publication validates every vehicle, but reuses validation of repeated source
timestamps within each snapshot. Check Worker CPU percentiles and
`exceededResources` outcomes as well as request totals, especially at peak fleet
size. A CPU-limit failure interrupts public publication, not research ingestion.
The publisher logs refused uploads by HTTP status only; it never logs response
bodies, authenticated URLs or tokens. See [Worker limits](https://developers.cloudflare.com/workers/platform/limits/).

```bash
cd frontend
npm ci
npx wrangler login
npx wrangler r2 bucket list
# Only create if this bucket does not already exist:
npx wrangler r2 bucket create bkk-public-snapshots
npm run deploy
# Enter a random token of at least 32 characters; do not put it in shell history.
npx wrangler secret put PUBLISH_TOKEN
```

The resulting `*.workers.dev` address needs no custom domain or public VPS port.
`wrangler.jsonc` intentionally contains no account identifiers or tokens. OAuth
credentials remain outside the repository. Keep frontend deployment manual;
CI validates the build but receives no production credentials and never deploys.

## Start the isolated publisher

First confirm existing collector/maintenance health. On the collector host,
check out the verified site revision without modifying `.env` or recreating the
running collector. Copy `.env.public.example` to a private mode-600 `.env.public`,
set `PUBLIC_PUBLISH_URL` to the Worker `/api/publish` URL and use the same random
token as the Worker secret. Do not reuse a BKK or Hugging Face token.

Before Compose starts, verify all source directories exist and the files are
readable by UID 65532. Bind **directories**, not atomic status files: binding a
single renamed file would pin an old inode. Do not chmod historical data to make
the public layer work; resolve permission problems in the isolated deployment.

```bash
test -d data/raw/vehiclepositions
test -d data/health
test -d data/maintenance
test -d data/metadata/manifests
test -d data/backup_receipts
test -d data/static_gtfs
install -d -m 750 -o 65532 -g 65532 public-output
chmod 600 .env.public
docker compose -f docker-compose.yml -f docker-compose.public.yml config --quiet
docker compose -f docker-compose.yml -f docker-compose.public.yml build public-exporter
docker compose -f docker-compose.yml -f docker-compose.public.yml \
  up -d --no-deps public-exporter
```

The sidecar has a read-only root filesystem, selected read-only source mounts,
no capabilities, UID 65532, a 256 MiB limit and a 0.25 CPU limit. Only its small
overwritten `public-output` snapshot/status files and bounded logs are writable.
Its healthcheck covers publication, not collector health. Never mount `.env`,
the entire data root, journals, spool, logs or the Docker socket into it.

## Verify and operate

```bash
docker compose exec -T collector python healthcheck.py
docker compose exec -T maintenance python maintenance_healthcheck.py
docker compose -f docker-compose.yml -f docker-compose.public.yml \
  exec -T public-exporter python -m public_exporter.healthcheck
docker compose -f docker-compose.yml -f docker-compose.public.yml ps
docker stats --no-stream
curl --fail --silent --show-error https://YOUR-WORKER.YOUR-SUBDOMAIN.workers.dev/api/snapshot
```

Check that `generated_at`, `vehicles.observed_at` and health observation times
advance, that the displayed fleet matches the current source, and that unknown
states do not linger. Confirm actual collector poll journals continue at their
configured cadence before/after adding the sidecar; do not infer continuity from
Docker “healthy” alone. Inspect the site on desktop/mobile and check Worker/R2
usage weekly. Basemap requests go directly to OpenFreeMap, not the VPS; preserve
OpenFreeMap/OpenMapTiles/OpenStreetMap attribution. No analytics, user accounts,
cookies, geolocation collection or browser persistence are added by this app.

Deploy a PR revision for verification, merge only after CI and the actual site
pass, then rebuild/redeploy from the merge commit. Tag the release after the
merged site and publisher have been verified. The unchanged collector may remain
on its known-good v0.1.0 image: the frontend release does not pretend to upgrade
collector provenance. Updates to this layer need restart only of public-exporter.

Rollback the public layer independently: redeploy a previously verified frontend
revision, or stop only `public-exporter` with the Compose overlay. The site will
mark the last snapshot stale. Keep the collector and maintenance running, and do
not delete collected data, receipts or version history as part of rollback.
If rolling the Worker back to a version before compact uploads were supported,
first restore the matching older exporter image or stop the exporter; otherwise
uploads will safely fail with HTTP 415 until their formats agree.
