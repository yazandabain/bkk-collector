# BKK realtime collector

Polls BKK's `VehiclePositions`, `TripUpdates` and `Alerts` GTFS-realtime feeds every
30 seconds, forever, and writes two copies of everything:

1. **`data/raw/<feed>/date=YYYY-MM-DD/<feed>.rawlog`** — the exact bytes BKK sent
   back, gzip-compressed, appended in order. This is the irreplaceable part.
2. **`data/parquet/<feed>/date=YYYY-MM-DD/part-*.parquet`** — the same data, parsed
   into flat tables, for actually working with in pandas/Polars/DuckDB later.

If the parsing code ever turns out to have a bug, `rebuild_parquet.py` regenerates
(2) from (1) — nothing is lost, you just get a cleaner version later. This is the
`make rebuild` target the study plan calls for.

Once a day it also uploads the previous day's data to a private Hugging Face
dataset repo, so the data doesn't only exist on one disk. Once a month it
re-downloads the static schedule zip (with a dated filename, since old versions
can't be recovered either).

---

## 0. Storage, honestly

Measured feed sizes for the live Budapest network: VehiclePositions ~189KB,
Alerts ~185KB, **TripUpdates ~4.9MB**. That last one is what matters: BKK
retransmits full remaining-stop predictions for every active trip on *every*
poll, so archiving it raw every 30 seconds would mean ~14GB/day, uncompressed
— several terabytes over 9 months. Not workable on a cheap VPS or free
storage.

So the collector doesn't do that. It still *polls* TripUpdates every 30s
(cheap — one HTTP request), but:

- **Only writes a row when something actually changed** for that specific
  (trip, stop) — a delay prediction that's identical to last poll doesn't get
  re-stored, only re-confirmed every 30 minutes as a heartbeat (`HEARTBEAT_SECONDS`).

  "Changed" for delays means **moved by ≥ `DELAY_CHANGE_THRESHOLD_SECONDS`
  (default 15s) from the value last written for that stop** — not "differs by
  any amount." BKK recalculates ETAs continuously off live GPS, so a delay
  wobbles by a second or two on nearly every poll even when nothing has
  actually happened; exact-equality comparison treats that noise as signal.
  Comparison is against the last *written* value rather than a fixed grid, so
  a value can hover anywhere without triggering writes, while slow cumulative
  drift still gets recorded each time it accumulates past the threshold. The
  raw, un-rounded delay is what's stored in every row that does get written —
  the threshold only decides *when* to write, never what.

  ⚠️ Two earlier versions of this got it wrong, both caught by testing:
  (1) `HEARTBEAT_SECONDS` was set to 300s, the same as the flush interval, so
  nearly everything got force-rewritten every flush; (2) a floor-division
  bucketing approach flapped across bucket boundaries — a delay oscillating
  43↔47 crossed the 45 boundary and wrote on ~7 of 10 polls. The current
  tolerance-against-last-written approach has neither failure mode.

### Validating the threshold against your own data

`check_threshold.py` measures the real distribution of delay changes from your
archived raw logs and reports what each candidate threshold would suppress:

```bash
docker compose exec collector python3 check_threshold.py
docker compose exec collector python3 check_threshold.py /data/raw/tripupdates/date=2026-08-19/tripupdates.rawlog
```

Read the "fraction SUPPRESSED at each candidate threshold" table. If 15s is
suppressing well over ~90% you could probably go lower and keep more detail;
if it's suppressing very little, most changes are genuinely large and the
volume you're seeing is real rather than noise. Adjust
`DELAY_CHANGE_THRESHOLD_SECONDS` in `.env` and `make up` to apply.
  Same logic applies to Alerts. VehiclePositions is left alone — a vehicle's
  position is basically always different from last poll, so dedup wouldn't
  help, and the feed is small anyway.
- **Archives the raw TripUpdates bytes every 5 minutes instead of every 30
  seconds** (`TRIPUPDATES_RAW_ARCHIVE_SECONDS`) — still a solid ground-truth
  trail for `rebuild_parquet.py`, just not a wasteful one.
- **Prunes old local raw files automatically**, but *only* once they're
  confirmed uploaded to Hugging Face and older than 14 days
  (`PRUNE_LOCAL_RAW_AFTER_DAYS`). The VPS disk only ever needs to hold a
  rolling couple of weeks — Hugging Face holds the real history. It will
  never delete anything it hasn't confirmed is backed up somewhere else.

With this, expect roughly **1–2 GB/day** of new raw data (mostly TripUpdates)
plus a much smaller Parquet layer (dedup means most polls add few or no new
rows). Over 9 months that's a genuinely non-trivial dataset — plausibly
**100–200GB** — which is exactly what you want for the flagship project, but
it does mean: **start on Hugging Face's free tier, and when you're a few
weeks in, check your usage at huggingface.co/settings** (free private storage
is 100GB). If you're approaching that, you have two easy outs: **HF PRO is
$9/month for 1TB** (the same $9/month your plan already earmarks for ZeroGPU
quota — same product, dual benefit), or you can simply flip the dataset repo
to public earlier than planned (BKK's data is CC BY 4.0, so nothing stops
you) — a rougher, less-documented public repo now is a fine trade for not
worrying about storage, and you polish it into the flagship writeup later.
I'd revisit this at the reassessment checkpoint in November rather than
solving it today.

**Before you deploy anything, confirm the numbers above still hold and that
the key actually works** — I can't reach `bkk.hu` from where I am to test
this live, so you're the first one to actually hit these endpoints:

```bash
curl -s -o vp.pb "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/VehiclePositions.pb?key=YOUR_KEY"
curl -s -o tu.pb "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/TripUpdates.pb?key=YOUR_KEY"
curl -s -o al.pb "https://go.bkk.hu/api/query/v1/ws/gtfs-rt/full/Alerts.pb?key=YOUR_KEY"
ls -la vp.pb tu.pb al.pb
pip install gtfs-realtime-bindings --break-system-packages
python3 -c "
from google.transit import gtfs_realtime_pb2 as pb
f = pb.FeedMessage(); f.ParseFromString(open('tu.pb','rb').read())
print(len(f.entity), 'entities')
"
```

If TripUpdates is dramatically bigger than 5MB or the entity count looks
off, tell me before you deploy — the throttle intervals above are tuned to
what you already measured, not a guess.

## 1. Where to run it

You want this on a machine that's on 24/7 with its own internet connection —
**not your laptop**. Two options, both fine:

- **A small paid VPS (recommended): Hetzner Cloud CX22/CX23, ~€5–6/month.**
  Simple signup, no capacity or account-approval roulette, EU-based (good
  latency to Budapest, and GDPR-friendly). Given this data cannot be
  recreated, I'd rather you spend €5/month than lose weeks to a free-tier
  account getting flagged or a region running out of capacity — both are
  real, documented problems with the free option below in 2026.
- **Oracle Cloud "Always Free" tier — genuinely €0/month forever.** An ARM VM
  (currently 2 OCPU / 12 GB RAM after a June 2026 reduction, still plenty for
  this) with 200 GB disk. The catch: signup sometimes gets flagged for manual
  review, and some regions report "out of capacity" for the free shape. If
  you have patience to retry and want to spend nothing, this works fine —
  just don't let signup friction eat into the runway before term starts.

Either way, once you have a Ubuntu VM with a public IP and SSH access, every
step below is identical. Pick whichever and let me know if you get stuck on
that provider's specific signup flow.

## 2. Set up the VM

```bash
ssh root@YOUR_VM_IP
apt update && apt install -y docker.io docker-compose-plugin git
systemctl enable --now docker
```

## 3. Get the code onto the VM and configure it

From your own machine, copy this whole folder to the VM (simplest way — `scp`):

```bash
scp -r bkk_collector root@YOUR_VM_IP:/root/
```

Then on the VM:

```bash
cd /root/bkk_collector
cp .env.example .env
nano .env   # paste your real BKK_API_KEY; optionally HF_TOKEN + HF_REPO_ID
```

For the Hugging Face backup (optional but recommended — you already have an
HF account per the plan's Phase 0 checklist):
1. huggingface.co → Settings → Access Tokens → create one with **write** role.
2. Pick a repo id like `yourname/bkk-transit-raw` — the script creates it
   automatically as a **private** dataset repo on first backup. You keep it
   private until Block 3, when the flagship project publishes a cleaned,
   public version.

## 4. Start it

```bash
make up
make logs      # watch it for a minute, Ctrl+C to stop watching (container keeps running)
```

You should see lines like `Flushed N rows -> .../part-HHMMSS.parquet` every few
minutes. If you instead see repeated `Fetch failed for ...` warnings, the key or
URL is wrong — recheck `.env`.

`restart: unless-stopped` in docker-compose.yml means it survives VM reboots and
crashes automatically — you don't need to babysit it.

## 5. Weekly, ~10 minutes (matches the plan's maintenance budget)

```bash
make status    # container still running? how much disk is data/ using? how much disk is free overall?
make logs      # skim for repeated errors
```

Also glance at your HF dataset repo (if configured) to confirm yesterday's
folders are actually landing there — that's your real backup, not just a
nice-to-have.

## 6. If disk fills up

The collector logs a loud `LOW DISK SPACE` warning well before it would crash,
but the safe fix is: once you've confirmed (via the HF repo) that a date's data
is backed up, you can delete the local `data/raw/*/date=OLD-DATE` folders for
that date — the Parquet stays useful on its own, and the raw copy already lives
on Hugging Face. Never delete a local raw folder you haven't confirmed is backed
up somewhere else.

## 7. About the No-AI-debt rule

I wrote this. That's a reasonable call for a piece of infrastructure where the
cost of *not* getting it running this week is unrecoverable, unlike CS50P or
Karpathy where the resource *is* the point. But the spirit of your own rule 1
is worth honoring here too, on your own timeline rather than under this
week's time pressure: once it's safely running, put an hour into actually
reading `gtfs_rt_parse.py` and `collector.py` — during Missing Semester /
Docker in Block 1 is a natural moment, since that's where you'd learn this
stuff properly anyway. It's ~250 lines total and none of it is exotic. You'll
want that understanding anyway: "I built and operated a data pipeline" is a
bullet point in your plan, and an interviewer asking "walk me through how it
works" deserves a real answer, not a recited one.

## 8. License

Per BKK's terms, anything you publish derived from this data must credit:
**"Data source: BKK Zrt., CC BY 4.0"**. Keep this in mind for Project 4's
dataset card and README — it belongs in both the HF dataset card and any
published repo.
