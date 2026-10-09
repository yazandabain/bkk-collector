import type { Snapshot } from '../shared/snapshot'

/** Entirely synthetic; no production payload, identifier or credential. */
export function snapshotAt(time = Date.now()): Snapshot {
  const timestamp = new Date(Math.floor(time / 1000) * 1000).toISOString().replace('.000Z', 'Z')
  const feed = { state: 'healthy' as const, observed_at: timestamp, source_at: timestamp, cadence_seconds: 10, entities: 2, issues: [] }
  return {
    schema_version: 1, generated_at: timestamp,
    vehicles: { type: 'FeatureCollection', observed_at: timestamp, source_at: timestamp, records_in_source: 2, omitted_records: 0, features: [
      { type: 'Feature', id: 'a'.repeat(20), geometry: { type: 'Point', coordinates: [19.055, 47.497] }, properties: { route_label: '4', mode: 'tram', color: '#ffd800', bearing: 30, recorded_at: timestamp } },
      { type: 'Feature', id: 'b'.repeat(20), geometry: { type: 'Point', coordinates: [19.062, 47.501] }, properties: { route_label: '7', mode: 'bus', color: '#009fe3', bearing: null, recorded_at: null } },
    ] },
    health: { state: 'healthy', observed_at: timestamp, feeds: {
      vehiclepositions: { ...feed }, tripupdates: { ...feed }, alerts: { ...feed, cadence_seconds: 30, entities: 0, issues: ['source_timestamp_unchanged_warning'] },
    }, maintenance: { state: 'healthy', archive_enabled: true, pending_days: 0 } },
    statistics: { as_of: timestamp, evidenced_since: '2026-08-23T01:15:16Z', evidenced_days: 40, verified_days: 39,
      last_verified_date: '2026-10-01', polls_recorded: 790000, event_rows: 2100000000, raw_snapshots: 460000,
      latest_day: { date: '2026-10-01', quality: 'flagged', feeds: {
        vehiclepositions: { expected_polls: 8640, recorded_polls: 8639, data_polls: 8638 },
        tripupdates: { expected_polls: 8640, recorded_polls: 8640, data_polls: 8640 },
        alerts: { expected_polls: 2880, recorded_polls: 2880, data_polls: null },
      } },
    }, public_layer_issues: [],
  }
}

/** Repeated source timestamps and unique map IDs, as in a full fleet snapshot. */
export function fleetAt(size = 2000, time = Date.now()): Snapshot {
  const value = snapshotAt(time)
  const templates = value.vehicles.features
  value.vehicles.features = Array.from({ length: size }, (_, index) => {
    const feature = structuredClone(templates[index % templates.length])
    feature.id = index.toString(16).padStart(20, '0')
    return feature
  })
  value.vehicles.records_in_source = size
  return value
}
