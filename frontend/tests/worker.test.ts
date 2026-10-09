import { beforeEach, describe, expect, it } from 'vitest'
import { handle, type Environment } from '../worker/index'
import { compactMediaType } from '../shared/snapshot'
import { compactSnapshot, fleetAt, snapshotAt } from './fixture'

const secret = 'synthetic-test-secret-'.repeat(3)
class MemoryBucket {
  payload: string | null = null
  generation = '0'
  writes = 0
  conflict = false
  async get(key: string) {
    expect(key).toBe('latest.json')
    return this.payload ? { httpEtag: '"test-etag"', body: new Response(this.payload).body! } : null
  }
  async head(key: string) { expect(key).toBe('latest.json'); return this.payload ? { etag: 'test-etag', customMetadata: { generated: this.generation } } : null }
  async put(key: string, payload: string | Uint8Array, options: Parameters<Environment['SNAPSHOTS']['put']>[2]) {
    expect(key).toBe('latest.json')
    expect(options.onlyIf).toEqual(this.payload ? { etagMatches: 'test-etag' } : { etagDoesNotMatch: '*' })
    if (this.conflict) return null
    this.payload = typeof payload === 'string' ? payload : new TextDecoder().decode(payload)
    this.generation = options.customMetadata.generated; this.writes++
    return { etag: 'test-etag' }
  }
}

describe('public Worker boundary', () => {
  let bucket: MemoryBucket, env: Environment
  beforeEach(() => {
    bucket = new MemoryBucket()
    env = { SNAPSHOTS: bucket, PUBLISH_TOKEN: secret, ASSETS: { fetch: async () => new Response('asset') } }
  })
  const publish = (body: unknown, token = secret, contentType = 'application/json') => new Request('https://example.invalid/api/publish', {
    method: 'PUT', headers: { Authorization: 'Bearer ' + token, 'Content-Type': contentType }, body: JSON.stringify(body),
  })
  it('round-trips actual bytes through the single fixed object', async () => {
    const value = snapshotAt()
    expect((await handle(publish(value), env)).status).toBe(204)
    const result = await handle(new Request('https://example.invalid/api/snapshot'), env)
    expect(await result.json()).toEqual(value)
    expect(result.headers.get('Cache-Control')).toBe('public, max-age=10')
    expect(bucket.writes).toBe(1)
  })
  it('publishes a full fleet without dropping features or leaking discarded JSON values', async () => {
    const value = fleetAt()
    const body = JSON.stringify(value).replace('{', '{"vehicles":{"api_key":"hidden-secret"},')
    const request = new Request('https://example.invalid/api/publish', {
      method: 'PUT', headers: { Authorization: 'Bearer ' + secret, 'Content-Type': 'application/json' }, body,
    })
    expect((await handle(request, env)).status).toBe(204)
    const result = await handle(new Request('https://example.invalid/api/snapshot'), env)
    expect(await result.json()).toEqual(value)
    expect(bucket.payload).not.toContain('hidden-secret')
    expect(bucket.payload).not.toContain('api_key')
    const previous = bucket.payload
    Object.assign(value.vehicles.features.at(-1)!.geometry, { private_path: 'private' })
    expect((await handle(publish(value), env)).status).toBe(400)
    expect(bucket.payload).toBe(previous)
    expect(bucket.writes).toBe(1)
  })
  it('compact publication returns exactly the same public GeoJSON and accepts old publishers', async () => {
    const value = fleetAt()
    value.vehicles.features[0].properties.route_label = 'Őrjárat 🚋'
    expect((await handle(publish(compactSnapshot(value), secret, compactMediaType), env)).status).toBe(204)
    expect(await (await handle(new Request('https://example.invalid/api/snapshot'), env)).json()).toEqual(value)
    expect((await handle(publish(value), env)).status).toBe(204)
    expect(JSON.parse(bucket.payload!)).toEqual(value)
    expect(bucket.writes).toBe(2)
  })
  it('compact publication still refuses stale data and preserves conditional-write conflicts', async () => {
    expect((await handle(publish(compactSnapshot(fleetAt(2000, Date.now() - 180000)), secret, compactMediaType), env)).status).toBe(400)
    bucket.conflict = true
    expect((await handle(publish(compactSnapshot(fleetAt()), secret, compactMediaType), env)).status).toBe(412)
    expect(bucket.writes).toBe(0)
  })
  it('compact duplicate JSON metadata cannot leak discarded private values', async () => {
    const value = compactSnapshot(snapshotAt())
    const body = JSON.stringify(value).replace('{', '{"health":{"api_key":"hidden-secret"},')
    const request = new Request('https://example.invalid/api/publish', {
      method: 'PUT', headers: { Authorization: 'Bearer ' + secret, 'Content-Type': compactMediaType }, body,
    })
    expect((await handle(request, env)).status).toBe(204)
    expect(JSON.parse(bucket.payload!)).toEqual(snapshotAt(Date.parse(value.generated_at)))
    expect(bucket.payload).not.toContain('hidden-secret')
  })
  it('limits expanded public UTF-8 bytes, not just compact input or string length', async () => {
    const value = fleetAt(7000)
    value.vehicles.features.forEach(feature => { feature.properties.route_label = 'ű'.repeat(48) })
    const compact = compactSnapshot(value)
    expect(new TextEncoder().encode(JSON.stringify(compact)).byteLength).toBeLessThan(2 * 1024 * 1024)
    expect(JSON.stringify(value).length).toBeLessThan(2 * 1024 * 1024)
    expect(new TextEncoder().encode(JSON.stringify(value)).byteLength).toBeGreaterThan(2 * 1024 * 1024)
    expect((await handle(publish(compact, secret, compactMediaType), env)).status).toBe(413)
    expect(bucket.writes).toBe(0)
  })
  it('a missing secret and invalid tokens never write', async () => {
    env.PUBLISH_TOKEN = undefined
    expect((await handle(publish(snapshotAt(), 'undefined'), env)).status).toBe(401)
    env.PUBLISH_TOKEN = secret
    expect((await handle(publish(snapshotAt(), 'wrong'), env)).status).toBe(401)
    expect(bucket.writes).toBe(0)
  })
  it('private operational objects and unsupported fields cannot be published', async () => {
    const value = snapshotAt()
    Object.assign(value.health, { ssh_path: 'private', api_key: 'private' })
    expect((await handle(publish(value), env)).status).toBe(400)
    expect(bucket.writes).toBe(0)
  })
  it('discarded duplicate JSON values never leak into the public object', async () => {
    const body = JSON.stringify(snapshotAt()).replace('{', '{"health":{"api_key":"hidden-secret"},')
    const request = new Request('https://example.invalid/api/publish', {
      method: 'PUT', headers: { Authorization: 'Bearer ' + secret, 'Content-Type': 'application/json' }, body,
    })
    expect((await handle(request, env)).status).toBe(204)
    expect(bucket.payload).not.toContain('hidden-secret')
    expect(bucket.payload).not.toContain('api_key')
  })
  it('rejects oversized actual bodies and wrong media types', async () => {
    expect((await handle(publish('a'.repeat(2 * 1024 * 1024)), env)).status).toBe(413)
    expect((await handle(publish(snapshotAt(), secret, 'text/plain'), env)).status).toBe(415)
    expect((await handle(publish(compactSnapshot(snapshotAt()), secret, compactMediaType.replace('.v1+', '.v2+')), env)).status).toBe(415)
    expect(bucket.writes).toBe(0)
  })
  it('stale upload and conditional write conflicts never replace the object', async () => {
    expect((await handle(publish(snapshotAt(Date.now() - 180000)), env)).status).toBe(400)
    bucket.conflict = true
    expect((await handle(publish(snapshotAt()), env)).status).toBe(412)
    expect(bucket.writes).toBe(0)
  })
  it('an older delayed request cannot roll back a newer snapshot', async () => {
    await handle(publish(snapshotAt()), env)
    const previous = bucket.payload
    expect((await handle(publish(snapshotAt(Date.now() - 10000)), env)).status).toBe(409)
    expect(bucket.payload).toBe(previous)
  })
  it('returns ETag 304 without fabricating a new timestamp', async () => {
    await handle(publish(snapshotAt()), env)
    const result = await handle(new Request('https://example.invalid/api/snapshot', { headers: { 'If-None-Match': '"test-etag"' } }), env)
    expect(result.status).toBe(304)
    expect(await result.text()).toBe('')
  })
  it('honors compressed-response weak ETags, lists and wildcards for GET and HEAD', async () => {
    await handle(publish(snapshotAt()), env)
    const previous = bucket.payload
    for (const method of ['GET', 'HEAD']) {
      for (const tag of ['W/"test-etag"', '"other", W/"test-etag"', 'W/"first,other", "test-etag"', '*']) {
        const result = await handle(new Request('https://example.invalid/api/snapshot', {
          method, headers: { 'If-None-Match': tag },
        }), env)
        expect(result.status, `${method} ${tag}`).toBe(304)
        expect(await result.text()).toBe('')
        expect(result.headers.get('ETag')).toBe('"test-etag"')
      }
      for (const tag of ['W/"different"', '"other", "different"', 'w/"test-etag"', 'test-etag']) {
        expect((await handle(new Request('https://example.invalid/api/snapshot', {
          method, headers: { 'If-None-Match': tag },
        }), env)).status, `${method} ${tag}`).toBe(200)
      }
    }
    expect(bucket.payload).toBe(previous)
    expect(bucket.writes).toBe(1)
  })
  it('unknown API paths cannot proxy arbitrary objects or assets', async () => {
    expect((await handle(new Request('https://example.invalid/api/private.json'), env)).status).toBe(404)
    expect((await handle(new Request('https://example.invalid/api/publish'), env)).status).toBe(404)
  })
  it('missing objects and upstream failures are safe generic unavailable states', async () => {
    expect((await handle(new Request('https://example.invalid/api/snapshot'), env)).status).toBe(503)
    env.SNAPSHOTS.get = async () => { throw new Error('private secret upstream') }
    const response = await handle(new Request('https://example.invalid/api/snapshot'), env)
    expect(response.status).toBe(503)
    expect(await response.text()).not.toContain('secret')
  })
})
