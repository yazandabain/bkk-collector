# BKK realtime collector
  
  Collects BKK VehiclePositions, TripUpdates, and Alerts GTFS-Realtime feeds.
  This initial version polls every 30 seconds by default. Raw protobuf archives
  and parsed Parquet files are written beneath the configured data directory.
  
  ## Data collection
  
  - VehiclePositions and Alerts raw snapshots are archived on each poll.
  - TripUpdates are parsed on every poll; raw snapshots are archived every
    300 seconds by default, controlled by `TRIPUPDATES_RAW_ARCHIVE_SECONDS`.
  - Derived TripUpdates record changes relative to the last emitted delay value
    using `DELAY_CHANGE_THRESHOLD_SECONDS`, plus periodic heartbeats controlled
    by `HEARTBEAT_SECONDS`.
  - Static GTFS is downloaded monthly in this version and stored as dated ZIPs.
  - Optional Hugging Face backup uploads previous-day data. Local raw retention
    is controlled by `PRUNE_LOCAL_RAW_AFTER_DAYS` and depends on backup state.
  
  Raw TripUpdates snapshots cannot reconstruct intermediate predictions between
  archived observations. Parsing or collection failures can leave gaps; backup
  state in this initial implementation is not a cryptographic restore guarantee.
  
  ## Setup
  
  Use an always-on host with Docker Compose, Git, and sufficient disk space.
  Replace all SSH and filesystem placeholders with private deployment values.
  Never commit credentials, collected data, or host-specific configuration.
  
  ```bash
  ssh SSH_USER@HOST
  cd /path/to/checkout
  cp .env.example .env
  ```
  
  Edit `.env` privately. Set `BKK_API_KEY` and, if backup is required, `HF_TOKEN`
  and `HF_REPO_ID`. Review the polling, raw archival, and retention settings.
  
  ```bash
  make up
  make status
  make logs
  ```
  
  Docker's restart policy allows collection to resume after a host reboot.
  Check collection errors, actual persisted observations, free space, and remote
  backup availability regularly. Do not publish unredacted operational logs.
  Do not delete local history without independently confirming recoverability.
  
  ## Rebuilding parsed data
  
  `rebuild_parquet.py` reconstructs derived rows from archived raw snapshots;
  `make rebuild` invokes the script. It cannot recover observations that were
  never archived. Preserve the original raw data and existing derived files
  before rebuilding historical partitions.
  
  ## Data attribution
  
  Data source: BKK Zrt., CC BY 4.0. The data attribution does not grant a license
  to third-party software or change the licensing of collected datasets.
  