/** The complete public contract. Unknown fields are rejected at publication. */
export const feedNames = ['vehiclepositions', 'tripupdates', 'alerts'] as const
export const modes = ['bus', 'tram', 'trolleybus', 'metro', 'rail', 'ferry', 'other'] as const
export type Mode = typeof modes[number]
export type State = 'healthy' | 'degraded' | 'unknown'
export type FeedName = typeof feedNames[number]
export const issueCodes = [
  'absent', 'http_failed', 'poll_journal_failed', 'protobuf_parse_failed', 'source_timestamp_stale',
  'source_timestamp_frozen', 'entity_timestamp_stale', 'payload_frozen', 'raw_archive_failed',
  'derived_spool_failed', 'tripupdates_presence_failed', 'change_tracker_failed', 'request_stuck',
  'scheduler_overdue', 'source_timestamp_unchanged_warning', 'payload_unchanged_warning',
  'request_exceeds_cadence', 'scheduler_missed_deadline',
] as const

export interface Vehicle {
  type: 'Feature'
  id: string
  geometry: { type: 'Point'; coordinates: [number, number] }
  properties: { route_label: string; mode: Mode; color: string; bearing: number | null; recorded_at: string | null }
}
export interface FeedHealth {
  state: State
  observed_at: string | null
  source_at: string | null
  cadence_seconds: number | null
  entities: number | null
  issues: string[]
}
export interface DayCounts {
  expected_polls: number | null
  recorded_polls: number | null
  data_polls: number | null
}
export interface Snapshot {
  schema_version: 1
  generated_at: string
  vehicles: {
    type: 'FeatureCollection'; observed_at: string | null; source_at: string | null
    records_in_source: number; omitted_records: number; features: Vehicle[]
  }
  health: {
    state: State; observed_at: string | null; feeds: Record<FeedName, FeedHealth>
    maintenance: { state: State; archive_enabled: boolean; pending_days: number }
  }
  statistics: {
    as_of: string; evidenced_since: string | null; evidenced_days: number; verified_days: number
    last_verified_date: string | null; polls_recorded: number; event_rows: number; raw_snapshots: number
    latest_day: { date: string; quality: 'good' | 'flagged'; feeds: Record<FeedName, DayCounts> } | null
  }
  public_layer_issues: ('route_catalog_unavailable' | 'vehicle_snapshot_unavailable')[]
}

function object(value: unknown, keys: readonly string[]): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key))
}
function timestamp(value: unknown, nullable = true): boolean {
  return (nullable && value === null) || (typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/.test(value)
    && Number.isFinite(Date.parse(value)) && new Date(value).toISOString() === value.replace('Z', '.000Z'))
}
function date(value: unknown): boolean {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && timestamp(value + 'T00:00:00Z', false)
}
function count(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0
}
function nullableCount(value: unknown): boolean { return value === null || count(value) }
function state(value: unknown): boolean { return typeof value === 'string' && ['healthy', 'degraded', 'unknown'].includes(value) }
function finite(value: unknown, min: number, max: number): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= min && value <= max
}

function vehicle(value: unknown): boolean {
  if (!object(value, ['type', 'id', 'geometry', 'properties']) || value.type !== 'Feature'
      || typeof value.id !== 'string' || !/^[a-f0-9]{20}$/.test(value.id)) return false
  const point = value.geometry, properties = value.properties
  if (!object(point, ['type', 'coordinates']) || point.type !== 'Point' || !Array.isArray(point.coordinates)
      || point.coordinates.length !== 2 || !finite(point.coordinates[0], -180, 180) || !finite(point.coordinates[1], -90, 90)) return false
  return object(properties, ['route_label', 'mode', 'color', 'bearing', 'recorded_at'])
    && typeof properties.route_label === 'string' && properties.route_label.length <= 48
    && modes.includes(properties.mode as Mode) && typeof properties.color === 'string' && /^#[a-f0-9]{6}$/.test(properties.color)
    && (properties.bearing === null || finite(properties.bearing, 0, 360)) && timestamp(properties.recorded_at)
}
function feedHealth(value: unknown): boolean {
  return object(value, ['state', 'observed_at', 'source_at', 'cadence_seconds', 'entities', 'issues'])
    && state(value.state) && timestamp(value.observed_at) && timestamp(value.source_at)
    && (value.cadence_seconds === null || finite(value.cadence_seconds, 5, 3600)) && nullableCount(value.entities)
    && Array.isArray(value.issues) && value.issues.length <= issueCodes.length
    && value.issues.every(code => issueCodes.includes(code))
}
function dayCounts(value: unknown): boolean {
  return object(value, ['expected_polls', 'recorded_polls', 'data_polls'])
    && nullableCount(value.expected_polls) && nullableCount(value.recorded_polls) && nullableCount(value.data_polls)
}

export function isSnapshot(value: unknown): value is Snapshot {
  if (!object(value, ['schema_version', 'generated_at', 'vehicles', 'health', 'statistics', 'public_layer_issues'])
      || value.schema_version !== 1 || !timestamp(value.generated_at, false)) return false
  const vehicles = value.vehicles, health = value.health, stats = value.statistics
  if (!object(vehicles, ['type', 'observed_at', 'source_at', 'records_in_source', 'omitted_records', 'features'])
      || vehicles.type !== 'FeatureCollection' || !timestamp(vehicles.observed_at) || !timestamp(vehicles.source_at)
      || !count(vehicles.records_in_source) || !count(vehicles.omitted_records) || !Array.isArray(vehicles.features)
      || vehicles.features.length > 10000 || !vehicles.features.every(vehicle)
      || vehicles.features.length + vehicles.omitted_records !== vehicles.records_in_source
      || new Set(vehicles.features.map(item => item.id)).size !== vehicles.features.length) return false
  if (!object(health, ['state', 'observed_at', 'feeds', 'maintenance']) || !state(health.state) || !timestamp(health.observed_at)) return false
  const feeds = health.feeds
  if (!object(feeds, feedNames) || !feedNames.every(name => feedHealth(feeds[name]))) return false
  const maintenance = health.maintenance
  if (!object(maintenance, ['state', 'archive_enabled', 'pending_days']) || !state(maintenance.state)
      || typeof maintenance.archive_enabled !== 'boolean' || !count(maintenance.pending_days)) return false
  if (!object(stats, ['as_of', 'evidenced_since', 'evidenced_days', 'verified_days', 'last_verified_date',
    'polls_recorded', 'event_rows', 'raw_snapshots', 'latest_day']) || !timestamp(stats.as_of, false)
      || !timestamp(stats.evidenced_since) || !(stats.last_verified_date === null || date(stats.last_verified_date))
      || !['evidenced_days', 'verified_days', 'polls_recorded', 'event_rows', 'raw_snapshots'].every(key => count(stats[key]))) return false
  const latest = stats.latest_day
  if (latest !== null) {
    if (!object(latest, ['date', 'quality', 'feeds']) || !date(latest.date) || typeof latest.quality !== 'string' || !['good', 'flagged'].includes(latest.quality)) return false
    const latestFeeds = latest.feeds
    if (!object(latestFeeds, feedNames) || !feedNames.every(name => dayCounts(latestFeeds[name]))) return false
  }
  return Array.isArray(value.public_layer_issues) && value.public_layer_issues.length <= 2
    && value.public_layer_issues.every(issue => ['route_catalog_unavailable', 'vehicle_snapshot_unavailable'].includes(issue))
}
