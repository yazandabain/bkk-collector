"""
BKK FUTAR realtime collector.

Every POLL_INTERVAL_SECONDS, fetches VehiclePositions, TripUpdates and Alerts,
writes the raw bytes to a gzip-appended log (see raw_log.py -- this is the
part that can never be recreated), parses them into rows, and periodically
flushes those rows to date-partitioned Parquet files. Once a day it uploads
the previous day's data to a Hugging Face dataset repo, and once a month it
re-downloads the static GTFS schedule zip.

Run it: `python collector.py`, or via Docker (see Dockerfile / docker-compose.yml).
Config is entirely via environment variables -- see .env.example.
"""

from __future__ import annotations

import gzip
import logging
import logging.handlers
import os
import shutil
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from gtfs_rt_parse import PARSERS, parse_feed
from raw_log import append_record
from dedup import ChangeTracker

# --------------------------------------------------------------------------
# Configuration (all from environment -- nothing here should need editing)
# --------------------------------------------------------------------------

API_KEY = os.environ["BKK_API_KEY"]  # required, will KeyError loudly if missing
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
POLL_INTERVAL_SECONDS = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
PARQUET_FLUSH_MINUTES = int(os.environ.get("PARQUET_FLUSH_MINUTES", "5"))
BACKUP_HOUR_UTC = int(os.environ.get("BACKUP_HOUR_UTC", "3"))
DISK_WARN_FREE_GB = float(os.environ.get("DISK_WARN_FREE_GB", "2.0"))

# TripUpdates is measured at ~5MB per poll for the Budapest network (BKK
# retransmits full remaining-stop predictions for every active trip on every
# poll). Polling every 30s is still cheap; ARCHIVING every 30s is not --
# unthrottled that's ~14GB/day uncompressed. So: keep polling frequent for
# freshness, but only persist rows/raw-snapshots when something changed.
TRIPUPDATES_RAW_ARCHIVE_SECONDS = int(os.environ.get("TRIPUPDATES_RAW_ARCHIVE_SECONDS", "300"))

# BKK recalculates ETAs continuously off live GPS, so arrival_delay/departure_delay
# can shift by a couple of seconds on nearly every poll even when nothing
# meaningfully changed -- exact-equality dedup treats that jitter as a real
# change. Anything moving by less than this many seconds doesn't count as
# "changed" for storage purposes (the raw, un-rounded value is still what
# gets stored in any row that IS written -- this only affects what triggers
# a write).
DELAY_CHANGE_THRESHOLD_SECONDS = int(os.environ.get("DELAY_CHANGE_THRESHOLD_SECONDS", "15"))
HEARTBEAT_SECONDS = int(os.environ.get("HEARTBEAT_SECONDS", "1800"))

# The VM's disk only needs to hold a rolling buffer -- Hugging Face holds the
# real multi-month history. Once a date's raw files are confirmed backed up
# AND older than this many days, they're deleted locally. Parquet (much
# smaller) is never auto-pruned. Set to 0 to disable pruning entirely.
PRUNE_LOCAL_RAW_AFTER_DAYS = int(os.environ.get("PRUNE_LOCAL_RAW_AFTER_DAYS", "14"))

# Dead-man's switch. If set, the collector pings this URL after every
# successful Parquet flush. Configure the check with a period longer than
# PARQUET_FLUSH_MINUTES (e.g. 1 hour) and healthchecks.io emails you when
# the pings STOP -- which is what catches the silent failures that a
# `restart: unless-stopped` container can't: a revoked API key, a stuck
# restart loop, a full disk. Optional; unset means no pinging.
HEALTHCHECK_URL = os.environ.get("HEALTHCHECK_URL", "").strip()

# Per-feed minimum gap between raw archive writes. Unset (0) = archive every poll.
RAW_ARCHIVE_INTERVAL = {
    "vehiclepositions": 0,
    "alerts": 0,
    "tripupdates": TRIPUPDATES_RAW_ARCHIVE_SECONDS,
}

HF_TOKEN = os.environ.get("HF_TOKEN")  # optional -- backup disabled if unset
HF_REPO_ID = os.environ.get("HF_REPO_ID")  # e.g. "yourname/bkk-transit-raw"

FEEDS = {
    "vehiclepositions": f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key={API_KEY}",
    "tripupdates": f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb?key={API_KEY}",
    "alerts": f"https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/Alerts.pb?key={API_KEY}",
}
STATIC_GTFS_URL = "https://go.bkk.hu/api/static/v1/public-gtfs/budapest_gtfs.zip"

RAW_DIR = DATA_DIR / "raw"
PARQUET_DIR = DATA_DIR / "parquet"
STATIC_DIR = DATA_DIR / "static_gtfs"
LOG_DIR = DATA_DIR / "logs"
STATE_FILE = DATA_DIR / "collector_state.txt"  # tiny file: last backup date, last static-download month
BACKUP_SUCCESS_FILE = DATA_DIR / "backup_success_dates.txt"  # append-only list of dates confirmed on HF

# --------------------------------------------------------------------------
# Logging -- to both stdout (docker logs) and a rotating file (readable even
# without docker, and survives container restarts since DATA_DIR is a volume)
# --------------------------------------------------------------------------

LOG_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("bkk_collector")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
_stream = logging.StreamHandler(sys.stdout)
_stream.setFormatter(_fmt)
_file = logging.handlers.RotatingFileHandler(LOG_DIR / "collector.log", maxBytes=10_000_000, backupCount=5)
_file.setFormatter(_fmt)
logger.addHandler(_stream)
logger.addHandler(_file)

session = requests.Session()
session.headers.update({"User-Agent": "bkk-collector/1.0 (personal transit research project)"})

_shutdown_requested = False


def _handle_signal(signum, frame):
    global _shutdown_requested
    logger.info("Received signal %s, will flush and exit after this cycle.", signum)
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# --------------------------------------------------------------------------
# State (which date we last backed up, which month we last fetched static GTFS)
# --------------------------------------------------------------------------

def load_state() -> dict:
    state = {"last_backup_date": "", "last_static_month": ""}
    if STATE_FILE.exists():
        for line in STATE_FILE.read_text().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                state[k] = v
    return state


def save_state(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text("\n".join(f"{k}={v}" for k, v in state.items()))


# --------------------------------------------------------------------------
# Parquet buffering
# --------------------------------------------------------------------------

_buffers: dict[str, list[dict]] = {name: [] for name in FEEDS}
_last_flush = time.monotonic()
_last_raw_archive: dict[str, float] = {name: 0.0 for name in FEEDS}


# VehiclePositions isn't tracked: a vehicle's lat/lon is essentially always
# different from the last poll, so dedup wouldn't help and the feed is cheap
# anyway (~190KB). TripUpdates and Alerts are the high-redundancy ones.
_trackers = {
    "tripupdates": ChangeTracker(
        key_fields=("trip_id", "start_date", "stop_id"),
        numeric_tolerance_fields=("arrival_delay", "departure_delay", "trip_delay"),
        tolerance=DELAY_CHANGE_THRESHOLD_SECONDS,
        heartbeat_seconds=HEARTBEAT_SECONDS,
    ),
    "alerts": ChangeTracker(
        key_fields=("entity_id", "affected_route_id", "affected_stop_id", "affected_trip_id"),
        value_fields=("cause", "effect", "header_text", "description_text", "active_period_start", "active_period_end"),
        heartbeat_seconds=HEARTBEAT_SECONDS,
    ),
}


def ping_healthcheck() -> None:
    """Signals 'still alive AND still collecting'. Deliberately called only
    after a flush that actually wrote rows -- if BKK revokes the key or the
    feeds go empty, the buffers stay empty, no ping is sent, and the check
    fires. A ping that merely proved the process was running would report
    healthy in exactly the failure case you most need to hear about."""
    if not HEALTHCHECK_URL:
        return
    try:
        session.get(HEALTHCHECK_URL, timeout=10)
    except Exception as e:
        # Never let monitoring break collection.
        logger.warning("Healthcheck ping failed (collection unaffected): %s", e)


def flush_parquet(force: bool = False) -> None:
    global _last_flush
    elapsed_min = (time.monotonic() - _last_flush) / 60
    if not force and elapsed_min < PARQUET_FLUSH_MINUTES:
        return
    import pandas as pd  # local import keeps startup fast if pandas is slow to import

    now = datetime.now(timezone.utc)
    wrote_anything = False
    for feed_name, rows in _buffers.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        out_dir = PARQUET_DIR / feed_name / f"date={now:%Y-%m-%d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"part-{now:%H%M%S}.parquet"
        try:
            df.to_parquet(out_path, index=False, compression="zstd")
            logger.info("Flushed %d rows -> %s", len(rows), out_path)
            wrote_anything = True
        except Exception:
            logger.exception("Failed writing Parquet for %s (raw data is still safe on disk)", feed_name)
        _buffers[feed_name] = []
    _last_flush = time.monotonic()

    if wrote_anything:
        ping_healthcheck()


# --------------------------------------------------------------------------
# One poll cycle
# --------------------------------------------------------------------------

def poll_once() -> None:
    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()

    for feed_name, url in FEEDS.items():
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            raw_bytes = resp.content
        except Exception as e:
            logger.warning("Fetch failed for %s: %s", feed_name, e)
            continue

        # Archive the raw bytes FIRST, before attempting to parse anything.
        # A parsing bug must never cost us the underlying data. Throttled per
        # feed (see RAW_ARCHIVE_INTERVAL) so TripUpdates doesn't write a fresh
        # ~5MB snapshot every 30 seconds -- see README for the math.
        min_gap = RAW_ARCHIVE_INTERVAL.get(feed_name, 0)
        if now.timestamp() - _last_raw_archive[feed_name] >= min_gap:
            raw_path = RAW_DIR / feed_name / f"date={now:%Y-%m-%d}" / f"{feed_name}.rawlog"
            try:
                append_record(raw_path, now.timestamp(), raw_bytes)
                _last_raw_archive[feed_name] = now.timestamp()
            except Exception:
                logger.exception("CRITICAL: failed to write raw archive for %s -- check disk space now", feed_name)

        try:
            feed = parse_feed(raw_bytes)
            rows = PARSERS[feed_name](feed, fetched_at)
            tracker = _trackers.get(feed_name)
            if tracker is not None:
                rows = tracker.filter(rows, f"{now:%Y-%m-%d}", now.timestamp())
            _buffers[feed_name].extend(rows)
        except Exception:
            logger.exception("Failed to parse %s this cycle (raw bytes were still archived on their own schedule)", feed_name)


def check_disk_space() -> None:
    total, used, free = shutil.disk_usage(DATA_DIR if DATA_DIR.exists() else "/")
    free_gb = free / 1e9
    if free_gb < DISK_WARN_FREE_GB:
        logger.error(
            "LOW DISK SPACE: only %.2f GB free. Collector will keep running but will "
            "eventually crash if this isn't fixed -- prune old local raw files once "
            "they're confirmed backed up (see rebuild_parquet.py / README).",
            free_gb,
        )


# --------------------------------------------------------------------------
# Daily backup to Hugging Face, monthly static GTFS refresh
# --------------------------------------------------------------------------

def maybe_backup_and_refresh(state: dict) -> None:
    now = datetime.now(timezone.utc)
    today = f"{now:%Y-%m-%d}"
    yesterday = f"{(now - timedelta(days=1)):%Y-%m-%d}"
    this_month = f"{now:%Y-%m}"

    if now.hour >= BACKUP_HOUR_UTC and state.get("last_backup_date") != today:
        flush_parquet(force=True)
        if HF_TOKEN and HF_REPO_ID:
            if backup_date_to_hf(yesterday):
                mark_backup_success(yesterday)
        else:
            logger.info("HF_TOKEN/HF_REPO_ID not set -- skipping cloud backup. Data is still safe locally.")
        state["last_backup_date"] = today
        save_state(state)
        prune_old_local_raw()

    if state.get("last_static_month") != this_month:
        download_static_gtfs()
        state["last_static_month"] = this_month
        save_state(state)


def backup_date_to_hf(date_str: str) -> bool:
    try:
        from huggingface_hub import HfApi

        api = HfApi(token=HF_TOKEN)
        api.create_repo(repo_id=HF_REPO_ID, repo_type="dataset", exist_ok=True, private=True)
        for base in (RAW_DIR, PARQUET_DIR):
            for feed_name in FEEDS:
                local_dir = base / feed_name / f"date={date_str}"
                if not local_dir.exists():
                    continue
                remote_prefix = f"{base.name}/{feed_name}/date={date_str}"
                api.upload_folder(
                    repo_id=HF_REPO_ID,
                    repo_type="dataset",
                    folder_path=str(local_dir),
                    path_in_repo=remote_prefix,
                )
        logger.info("Backed up %s to hf.co/datasets/%s", date_str, HF_REPO_ID)
        return True
    except Exception:
        logger.exception("HF backup failed for %s -- local copy is untouched, will not retry until tomorrow "
                          "and will NOT be pruned locally until a backup succeeds. Fix credentials/network.", date_str)
        return False


def mark_backup_success(date_str: str) -> None:
    with open(BACKUP_SUCCESS_FILE, "a") as f:
        f.write(date_str + "\n")


def backed_up_dates() -> set:
    if not BACKUP_SUCCESS_FILE.exists():
        return set()
    return set(BACKUP_SUCCESS_FILE.read_text().split())


def prune_old_local_raw() -> None:
    """Deletes local raw/<feed>/date=X folders once X is both older than
    PRUNE_LOCAL_RAW_AFTER_DAYS and confirmed present in the HF backup.
    Parquet is never touched here -- it's small and worth keeping locally too."""
    if PRUNE_LOCAL_RAW_AFTER_DAYS <= 0:
        return
    cutoff = datetime.now(timezone.utc) - timedelta(days=PRUNE_LOCAL_RAW_AFTER_DAYS)
    done = backed_up_dates()
    for feed_name in FEEDS:
        feed_raw_dir = RAW_DIR / feed_name
        if not feed_raw_dir.exists():
            continue
        for date_dir in feed_raw_dir.glob("date=*"):
            date_str = date_dir.name.replace("date=", "")
            try:
                d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if d < cutoff and date_str in done:
                shutil.rmtree(date_dir, ignore_errors=True)
                logger.info("Pruned local raw for %s/date=%s (confirmed on Hugging Face already)", feed_name, date_str)


def download_static_gtfs() -> None:
    now = datetime.now(timezone.utc)
    STATIC_DIR.mkdir(parents=True, exist_ok=True)
    out_path = STATIC_DIR / f"budapest_gtfs_{now:%Y-%m-%d}.zip"
    try:
        resp = session.get(STATIC_GTFS_URL, timeout=60)
        resp.raise_for_status()
        out_path.write_bytes(resp.content)
        logger.info("Downloaded static GTFS -> %s (%.1f MB)", out_path, len(resp.content) / 1e6)
    except Exception:
        logger.exception("Failed to download static GTFS this month -- will retry next cycle check")


# --------------------------------------------------------------------------
# Main loop
# --------------------------------------------------------------------------

def main() -> None:
    logger.info("Starting BKK collector. Data dir: %s. Poll interval: %ss.", DATA_DIR, POLL_INTERVAL_SECONDS)
    if not HF_TOKEN or not HF_REPO_ID:
        logger.warning("HF_TOKEN/HF_REPO_ID not both set -- running WITHOUT cloud backup. "
                        "Data only exists on this machine's disk until you configure it.")

    state = load_state()
    disk_check_counter = 0

    while not _shutdown_requested:
        cycle_start = time.monotonic()

        poll_once()
        flush_parquet()

        disk_check_counter += 1
        if disk_check_counter % 20 == 0:  # roughly every ~10 min at 30s interval
            check_disk_space()

        maybe_backup_and_refresh(state)

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, POLL_INTERVAL_SECONDS - elapsed)
        # Sleep in small slices so a shutdown signal is picked up quickly.
        slept = 0.0
        while slept < sleep_for and not _shutdown_requested:
            time.sleep(min(1.0, sleep_for - slept))
            slept += 1.0

    logger.info("Shutting down -- flushing remaining buffered rows.")
    flush_parquet(force=True)
    logger.info("Clean shutdown complete.")


if __name__ == "__main__":
    main()
