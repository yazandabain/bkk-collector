import { describe, expect, it, vi } from 'vitest'
import { decodeCompactSnapshot, isSnapshot, modes } from '../shared/snapshot'
import { age, ageLabel, coverage, effectiveState } from '../src/format'
import { compactSnapshot, fleetAt, snapshotAt } from './fixture'

describe('closed public snapshot', () => {
  it('accepts optional fields, all transport modes and known advisories', () => {
    expect(isSnapshot(snapshotAt())).toBe(true)
  })
  it('rejects extra private fields at every boundary', () => {
    for (const path of ['', 'health', 'statistics', 'vehicles', 'feature', 'geometry', 'properties', 'feed', 'maintenance']) {
      const value = snapshotAt()
      const targets: Record<string, object> = { '': value, health: value.health, statistics: value.statistics,
        vehicles: value.vehicles, feature: value.vehicles.features[0], geometry: value.vehicles.features[0].geometry,
        properties: value.vehicles.features[0].properties,
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
  it('validates repeated fleet timestamps once per distinct value, not once per vehicle', () => {
    const value = fleetAt()
    const timestamps = ['2026-10-02T12:00:01Z', '2026-10-02T12:00:02Z']
    value.vehicles.features.forEach((feature, index) => { feature.properties.recorded_at = timestamps[index % timestamps.length] })
    const parse = vi.spyOn(Date, 'parse')
    try {
      expect(isSnapshot(value)).toBe(true)
      for (const timestamp of timestamps) expect(parse.mock.calls.filter(([argument]) => argument === timestamp)).toHaveLength(1)
      // No cross-request cache or stale validation state.
      expect(isSnapshot(value)).toBe(true)
      for (const timestamp of timestamps) expect(parse.mock.calls.filter(([argument]) => argument === timestamp)).toHaveLength(2)
    } finally { parse.mockRestore() }
  })
  it('still checks the final vehicle for private fields, duplicate IDs and missing keys', () => {
    for (const change of [
      (value: ReturnType<typeof fleetAt>) => { Object.assign(value.vehicles.features.at(-1)!.properties, { api_key: 'private' }) },
      (value: ReturnType<typeof fleetAt>) => { value.vehicles.features.at(-1)!.id = value.vehicles.features[0].id },
      (value: ReturnType<typeof fleetAt>) => { Reflect.deleteProperty(value.vehicles.features.at(-1)!.properties, 'recorded_at') },
    ]) {
      const value = fleetAt()
      change(value)
      expect(isSnapshot(value)).toBe(false)
    }
  })
  it('retains strict calendar validation for repeated fleet timestamps', () => {
    const value = fleetAt()
    for (const [timestamp, valid] of [
      ['2024-02-29T00:00:00Z', true], ['2026-02-29T00:00:00Z', false],
      ['2026-04-31T00:00:00Z', false], ['2026-10-02T24:00:00Z', false],
      ['2026-10-02T12:00:00.000Z', false], ['2026-10-02T12:00:00+00:00', false],
    ] as const) {
      value.vehicles.features.forEach(feature => { feature.properties.recorded_at = timestamp })
      expect(isSnapshot(value), timestamp).toBe(valid)
    }
    value.vehicles.features.forEach(feature => { feature.properties.recorded_at = null })
    expect(isSnapshot(value)).toBe(true)
  })
})

describe('compact publication contract', () => {
  it('round-trips every mode and nullable field without changing public schema', () => {
    const value = fleetAt(modes.length)
    value.vehicles.features.forEach((feature, index) => { feature.properties.mode = modes[index] })
    expect(decodeCompactSnapshot(compactSnapshot(value))).toEqual(value)
    expect(decodeCompactSnapshot(compactSnapshot(fleetAt(0)))).toEqual(fleetAt(0))
  })
  it('rejects invalid tuples, late duplicate IDs, private metadata and count mismatches', () => {
    for (const mutate of [
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)!.push('private') },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)!.pop() },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![0] = value.vehicles.features[0][0] },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![1] = Infinity },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![2] = -91 },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![3] = 'x'.repeat(49) },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![4] = 'unknown-mode' },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![5] = 'url(private)' },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![6] = -1 },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.features.at(-1)![7] = '2026-02-30T00:00:00Z' },
      (value: ReturnType<typeof compactSnapshot>) => { Object.assign(value.health.feeds.alerts, { private_error: 'private' }) },
      (value: ReturnType<typeof compactSnapshot>) => { value.vehicles.omitted_records++ },
    ]) {
      const value = compactSnapshot(fleetAt())
      mutate(value)
      expect(decodeCompactSnapshot(value)).toBeNull()
    }
    const row = compactSnapshot(snapshotAt()).vehicles.features[0]
    for (const index of [0, 1, 2, 3, 4, 5, 6, 7]) {
      const value = compactSnapshot(snapshotAt())
      value.vehicles.features[0] = [...row]
      Object.assign(value.vehicles.features[0], { [index]: { api_key: 'private' } })
      expect(decodeCompactSnapshot(value)).toBeNull()
    }
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
