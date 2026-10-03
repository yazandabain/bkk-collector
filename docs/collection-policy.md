# Collection policy and research boundaries

The archive records what the collector could observe, not ground truth. Use UTC
request/response times and immutable run metadata when constructing as-of views.
Do not backdate an observation to its source timestamp or use a later prediction
as ground truth for an earlier forecast.

## Cadences

| Source | Polling | Durable preservation |
| --- | --- | --- |
| VehiclePositions | 10 s | Full raw protobuf and parsed rows for every successful poll |
| TripUpdates | 10 s | Meaningful-change rows; membership and source-timestamp evidence for every valid full-feed observation |
| TripUpdates full response | Same 10 s requests | First successful response, approximately every 300 s, auxiliary-state changes, and failure fallbacks |
| Alerts | 30 s | Full raw protobuf for every successful poll; change-compressed parsed rows |
| Static GTFS | Daily | SHA-256-addressed ZIP versions plus every successful check's observation time |

These are independent, configurable schedules, not a synchronized poll cycle.
HTTP failures, deadline coalescing, storage failures and source staleness can
reduce actual coverage; consult poll journals and manifests rather than assuming
the configured cadence was achieved. No existing preservation is reduced.

Arrival/departure predictions use a **provisional 2 s tolerance**, across all
modes: `abs(new - last_durably_emitted) > 2`. Null transitions and meaningful
categorical changes emit; the 1,800 s heartbeat remains. Small suppressed
revisions cannot be reconstructed exactly from the event stream. The five-minute
raw stream cannot recreate all intermediate ten-second predictions.

Presence records describe membership, not cancellation or actual arrival. Their
optional `source_timestamps_version=1` section records per-trip timestamp deltas
independently of prediction compression. Baselines cover every present trip;
withdrawals remove its timestamp; null means the producer omitted it. Older
records have **unknown**, not inferred, source-timestamp history. Replay membership
before `apply_source_timestamp_record`. Gaps, restarts and UTC midnight establish
a new baseline rather than inventing withdrawals.

Realtime shapes, dynamic stops and trip modifications have no tabular projection.
Any change in their order-independent serialized state, including disappearance,
forces an exact full TripUpdates raw snapshot. The comparison baseline advances
only after a successful raw write. Periodic snapshots and failure fallbacks stay
enabled; this is not a replacement raw format or a new data source.

## Evidence behind the decisions

A bounded, isolated ten-minute experiment on 2026-10-03 collected 120 successful
five-second observations per realtime feed without changing production. Comparing
that trace with its ten-second subsample found:

- 11,255,231 stop predictions, with no populated delay fields. Absolute times
  remain the prediction signal. A prediction-only 2 s tracker emitted 473,973
  rows at 5 s versus 430,676 at 10 s, including initial observations. These are
  **not** full production emission rates: other mutable fields and longer
  heartbeats were not simulated.
- Three realtime shape withdrawals between regular full-snapshot times, justifying
  preservation on auxiliary-state change without continuous full TU archival.
- Two brief `STOPPED_AT` and two door-open observations absent from the 10 s
  subsample, after requiring advancing/nondecreasing vehicle source timestamps.
  This establishes a sampling limitation, not reconstructed stop events.
- No Alert content or membership changes across 120 observations.

This short Saturday window does not establish an optimal cadence, guarantee that
all short-lived events are captured, or represent weekday peaks. Keep VP/TU at
10 s: measured production TU processing already approached/exceeded that budget.
Doubling TU processing or full-raw writes is not justified without resource
headroom and a representative experiment. Alerts remain at 30 s; this sample
provides no evidence for increasing their cadence. Do not raise tolerances or
reduce archival merely to save storage.

## Occupancy and capacity

Twenty-seven archived samples across 2026-08-21 and 2026-10-01–03 contained 24,007
vehicle entities and 2,114,760 stop updates. None populated vehicle occupancy
status/percentage, carriage occupancy, or departure occupancy. The parser already
preserves those optional standard fields and keeps missing values null. This is
an observed absence, not proof that BKK never supplies them.

Vehicle model/type, door state, stop distance and occasional congestion values
were populated. They are not passenger counts or measured capacity. The
[published realCity schema](https://opendata.bkk.hu/docs/gtfs-realtime-realcity.proto)
defines no passenger-count/capacity extension. Do not fabricate demand labels or
seek APC data as part of collection. Raw bytes preserve unknown fields; in this
sample Alerts contained unknown fields, demonstrating why parsed rows alone are
not authoritative. Standard field meanings follow the
[GTFS-Realtime reference](https://gtfs.org/documentation/realtime/reference/).

## Disruptions: use Alerts, not another scraper

The 2026-10-03 comparison matched all 66 archived `bkkinfo-*` Alert IDs to the
[BKK Info list](https://bkk.hu/apps/bkkinfo/lista). All 37 planned/future website
events were already in Alerts. The only additional current website entry had
already ended before the archived sample. Alerts included future starts through
2026-11-12 and an end period extending into 2027.

Alerts already preserve active periods, multilingual text, route/trip/stop
selectors, URLs and realCity metadata. This one comparison does not prove
permanent equality of the two interfaces or their publication times. However,
no useful incremental temporal coverage was demonstrated, and BKK's
[website terms](https://bkk.hu/jogi-tudnivalok/jogi-nyilatkozat/) do not grant the
same archiving/reuse permission as the existing GTFS source attribution. No BKK
Info scraping service is introduced. Revisit only for a documented information
gap and a supported, appropriately licensed access method.

## Static schedule provenance

Forty-three history observations contained 42 distinct ZIP hashes. Recent feed
versions changed daily; daily checks are retained rather than returning to
infrequent downloads. Select schedules by observation timestamp using
`StaticGtfsStore.observed_version_at`, never by the newest ZIP or day-end alone.
The history identifies what the collector observed; BKK's exact publication time
and unobserved intermediate versions between checks remain uncertain. Legacy
schedule applicability stays explicitly uncertain. Preserve unmatched and
external/integrated realtime services rather than dropping them.

## Next phase

Collection remains separate from research. Begin with leakage-safe stop-event
and ground-truth reconstruction that quantifies sampling, source-clock reordering,
gaps, prediction compression and static-version uncertainty. Do not treat
predicted times, disappearance or door/stop status alone as verified arrivals.
No new production infrastructure, modelling sources or frontend features are
required by this policy.
