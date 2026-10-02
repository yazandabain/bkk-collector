import { describe, expect, it } from 'vitest'
import { isSnapshot } from '../shared/snapshot'
import { age, ageLabel, coverage, effectiveState } from '../src/format'
import { snapshotAt } from './fixture'

describe('closed public snapshot', () => {
  it('accepts optional fields, all transport modes and known advisories', () => {
    expect(isSnapshot(snapshotAt())).toBe(true)
  })
  it('rejects extra private fields at every boundary', () => {
    for (const path of ['', 'health', 'statistics', 'vehicles', 'feature', 'properties', 'feed', 'maintenance']) {
      const value = snapshotAt()
      const targets: Record<string, object> = { '': value, health: value.health, statistics: value.statistics,
        vehicles: value.vehicles, feature: value.vehicles.features[0], properties: value.vehicles.features[0].properties,
        feed: value.health.feeds.tripupdates, maintenance: value.health.maintenance }
      Object.assign(targets[path], { private_error: 'must never publish' })
      expect(isSnapshot(value), path).toBe(false)
    }
  })
  it('rejects invalid dates, nonfinite coordinates, unsafe colors and duplicate IDs', () => {
    const changes = [
      (value: ReturnType<typeof snapshotAt>) => { value.generated_at = '2026-02-30T00:00:00Z' },
      (value: ReturnType<typeof snapshotAt>) => { value.vehicles.features[0].geometry.coordinates[0] = NaN },
      (value: ReturnType<typeof snapshotAt>) => { value.vehicles.features[0].properties.color = 'url(private)' },
      (value: ReturnType<typeof snapshotAt>) => { value.vehicles.features[1].id = value.vehicles.features[0].id },
      (value: ReturnType<typeof snapshotAt>) => { value.vehicles.omitted_records = 20 },
    ]
    for (const change of changes) { const value = snapshotAt(); change(value); expect(isSnapshot(value)).toBe(false) }
  })
  it('rejects arbitrary issue strings instead of forwarding exception messages', () => {
    const value = snapshotAt()
    value.health.feeds.tripupdates.issues = ['error at private host?key=secret']
    expect(isSnapshot(value)).toBe(false)
  })
  it('does not coerce array states into valid enum strings', () => {
    const value = snapshotAt()
    Object.assign(value.health, { state: ['healthy'] })
    expect(isSnapshot(value)).toBe(false)
  })
})

describe('honest freshness and coverage', () => {
  it('stale public snapshot is unknown, not proof of collector failure', () => {
    expect(effectiveState(snapshotAt(1000000), 1090001)).toBe('unknown')
    expect(effectiveState(snapshotAt(1000000), 1020000)).toBe('healthy')
    expect(effectiveState(null, Date.now())).toBe('unknown')
  })
  it('does not substitute generation time for source observation time', () => {
    const value = snapshotAt()
    value.vehicles.observed_at = '2026-01-01T00:00:00Z'
    expect(age(value.vehicles.observed_at, Date.now())).toBeGreaterThan(90)
  })
  it('nullable historical evidence is not zero coverage', () => {
    expect(coverage(null, 8640)).toBe('Not measured')
    expect(coverage(8639, 8640)).toBe('99.99%')
    expect(coverage(0, 8640)).toBe('0.00%')
  })
  it('ages are explicit and source-null is not epoch zero', () => {
    expect(age(null, Date.now())).toBeNull()
    expect(ageLabel(null)).toBe('No observation')
    expect(ageLabel(74)).toBe('1m ago')
  })
})
