import { useEffect, useState } from 'react'
import { isSnapshot, type Snapshot } from '../shared/snapshot'

export function useSnapshot() {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null)
  const [error, setError] = useState(false)
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    let disposed = false
    let timer: ReturnType<typeof setTimeout> | undefined
    let controller: AbortController | undefined
    async function poll() {
      if (disposed || document.hidden) return
      controller = new AbortController()
      const active = controller
      const timeout = setTimeout(() => active.abort(), 8000)
      try {
        const response = await fetch('/api/snapshot', { signal: active.signal })
        if (!response.ok || Number(response.headers.get('Content-Length')) > 2 * 1024 * 1024) throw new Error('unavailable')
        const data: unknown = await response.json()
        if (!isSnapshot(data)) throw new Error('invalid snapshot')
        if (!disposed && controller === active) { setSnapshot(data); setError(false) }
      } catch {
        if (!disposed && controller === active && !document.hidden) setError(true)
      } finally {
        clearTimeout(timeout)
        if (!disposed && controller === active && !document.hidden) timer = setTimeout(poll, 15000)
      }
    }
    function visibility() {
      clearTimeout(timer)
      controller?.abort()
      controller = undefined
      if (!document.hidden) void poll()
    }
    void poll()
    const clock = setInterval(() => setNow(Date.now()), 1000)
    document.addEventListener('visibilitychange', visibility)
    return () => { disposed = true; clearTimeout(timer); clearInterval(clock); controller?.abort(); document.removeEventListener('visibilitychange', visibility) }
  }, [])
  return { snapshot, error, now }
}
