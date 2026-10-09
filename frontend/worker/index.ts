import { compactMediaType, decodeCompactSnapshot, isSnapshot } from '../shared/snapshot'

export interface Environment {
  SNAPSHOTS: {
    get(key: string): Promise<{ httpEtag: string; body: ReadableStream<Uint8Array> } | null>
    head(key: string): Promise<{ etag: string; customMetadata?: Record<string, string> } | null>
    put(key: string, value: string | Uint8Array, options: {
      httpMetadata: { contentType: string }; customMetadata: Record<string, string>
      onlyIf: { etagMatches: string } | { etagDoesNotMatch: string }
    }): Promise<unknown>
  }
  ASSETS: { fetch(request: Request): Promise<Response> }
  PUBLISH_TOKEN?: string
}
const KEY = 'latest.json'
const MAX_BYTES = 2 * 1024 * 1024
const noCache = { 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' }

async function authorized(header: string | null, secret: string | undefined): Promise<boolean> {
  if (!secret || secret.length < 32 || !header?.startsWith('Bearer ') || header.length > 512) return false
  const encoder = new TextEncoder()
  const [expected, supplied] = await Promise.all([secret, header.slice(7)].map(value =>
    crypto.subtle.digest('SHA-256', encoder.encode(value))))
  const left = new Uint8Array(expected), right = new Uint8Array(supplied)
  let difference = 0
  for (let i = 0; i < left.length; i++) difference |= left[i] ^ right[i]
  return difference === 0
}

async function boundedBody(request: Request): Promise<string> {
  if (Number(request.headers.get('Content-Length')) > MAX_BYTES) throw new RangeError()
  const reader = request.body?.getReader()
  if (!reader) throw new SyntaxError()
  const chunks: Uint8Array[] = []
  let length = 0
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      length += value.byteLength
      if (length > MAX_BYTES) { await reader.cancel(); throw new RangeError() }
      chunks.push(value)
    }
  } finally { reader.releaseLock() }
  const bytes = new Uint8Array(length)
  let offset = 0
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length }
  return new TextDecoder('utf-8', { fatal: true, ignoreBOM: false }).decode(bytes)
}

export async function handle(request: Request, env: Environment): Promise<Response> {
  const path = new URL(request.url).pathname
  try {
    if (path === '/api/snapshot' && (request.method === 'GET' || request.method === 'HEAD')) {
      const object = await env.SNAPSHOTS.get(KEY)
      if (!object) return new Response('Snapshot unavailable', { status: 503, headers: { ...noCache, 'Retry-After': '15' } })
      const headers = new Headers({ 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'public, max-age=10',
        'ETag': object.httpEtag, 'X-Content-Type-Options': 'nosniff' })
      if (request.headers.get('If-None-Match') === object.httpEtag) return new Response(null, { status: 304, headers })
      return new Response(request.method === 'HEAD' ? null : object.body, { headers })
    }
    if (path === '/api/publish' && request.method === 'PUT') {
      if (!await authorized(request.headers.get('Authorization'), env.PUBLISH_TOKEN)) return new Response('Unauthorized', { status: 401, headers: noCache })
      const mediaType = request.headers.get('Content-Type')?.split(';')[0].trim()
      if (mediaType !== 'application/json' && mediaType !== compactMediaType) return new Response('JSON required', { status: 415, headers: noCache })
      let payload: string
      try { payload = await boundedBody(request) } catch (error) {
        return new Response('Invalid payload', { status: error instanceof RangeError ? 413 : 400, headers: noCache })
      }
      let incoming: unknown
      try { incoming = JSON.parse(payload) } catch { return new Response('Invalid JSON', { status: 400, headers: noCache }) }
      const snapshot = mediaType === compactMediaType ? decodeCompactSnapshot(incoming) : isSnapshot(incoming) ? incoming : null
      if (!snapshot) return new Response('Invalid snapshot', { status: 400, headers: noCache })
      const generated = Date.parse(snapshot.generated_at)
      if (Math.abs(Date.now() - generated) > 120000) return new Response('Snapshot time invalid', { status: 400, headers: noCache })
      // Canonical bytes remove discarded duplicate JSON values and keep the
      // original public size limit even when the private transfer is compact.
      const canonical = new TextEncoder().encode(JSON.stringify(snapshot))
      if (canonical.byteLength > MAX_BYTES) return new Response('Invalid payload', { status: 413, headers: noCache })
      const previous = await env.SNAPSHOTS.head(KEY)
      if (previous && Number(previous.customMetadata?.generated) > generated) return new Response('Older snapshot refused', { status: 409, headers: noCache })
      const result = await env.SNAPSHOTS.put(KEY, canonical, {
        httpMetadata: { contentType: 'application/json; charset=utf-8' },
        customMetadata: { generated: String(generated) },
        onlyIf: previous ? { etagMatches: previous.etag } : { etagDoesNotMatch: '*' },
      })
      return new Response(null, { status: result ? 204 : 412, headers: noCache })
    }
    if (path.startsWith('/api/')) return new Response('Not found', { status: 404, headers: noCache })
    return env.ASSETS.fetch(request)
  } catch {
    // No binding names, tokens, object contents or upstream details in errors.
    return new Response('Public data temporarily unavailable', { status: 503, headers: noCache })
  }
}

export default { fetch: handle }
