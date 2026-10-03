# Backend development

Use Python 3.12 or newer. Runtime dependencies remain pinned in
`requirements.txt`; the package uses that same file rather than a second
dependency list.

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m pip install --no-deps -e .
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -m unittest discover -s public_exporter/tests -v
.venv/bin/python -m compileall -q bkk_collector public_exporter tests
.venv/bin/python -m pip check
```

The tests use synthetic feeds and temporary directories, not production mounts,
credentials or live services. Frontend development remains separate; see
[the public site guide](public-site.md).

## Package boundaries

```text
bkk_collector/
  collector.py, maintenance.py    process entrypoints and orchestration
  config.py, monitoring.py       configuration and collection health
  realtime/                     parsing, scheduling, compression and presence
  storage/                      durable logs, journals, spool and Parquet
  archive/                      manifests, verified backup, retention and GTFS
  cli/                          diagnostics, healthchecks, rebuild and recovery
public_exporter/                isolated public snapshot exporter
frontend/                       static site and publication endpoint
tests/                          offline backend regression tests
```

Import library helpers through their canonical package paths, for example
`bkk_collector.storage.raw_log.iter_records` and
`bkk_collector.realtime.gtfs_rt_parse.parse_feed`. Importing modules does not
start services or inspect credentials. Research code should depend on these
helpers rather than import a compatibility launcher.

The root scripts remain small compatibility launchers, so existing Compose,
healthchecks, deployment and recovery commands continue to work. Installed
commands include `bkk-collector`, `bkk-maintenance`, `bkk-diagnostics`,
`bkk-rebuild-parquet` and `bkk-verify-backup`. Module equivalents include
`python -m bkk_collector`, `python -m bkk_collector.maintenance` and
`python -m bkk_collector.cli.diagnostics`. All daemon entrypoints install the
same graceful shutdown handlers.

## Compatibility invariants

Moving code is not a data migration. Preserve on-disk paths, schema versions,
raw framing, receipt semantics and environment names. Do not weaken durable
stage/commit boundaries, verified pruning or failure fallbacks to simplify an
analysis. The public exporter has a separate allowlisted contract.

Package versions describe software releases, not data schema versions. Run
metadata retains the deployed Git commit and existing schema identifiers.
Collection frequency, tolerance and reconstruction limits are documented in
[the collection policy](collection-policy.md).

CI builds and installs a wheel, checks imports outside the checkout, exercises
the production images offline, and runs the existing backend/exporter/frontend
suites. No data migration, production deployment or package publishing is
performed by CI.
