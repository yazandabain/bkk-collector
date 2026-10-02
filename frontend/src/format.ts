import type { Snapshot, State } from '../shared/snapshot'

export const modeLabels = { bus: 'Bus', tram: 'Tram', trolleybus: 'Trolleybus', metro: 'Metro', rail: 'Rail / HÉV', ferry: 'Ferry', other: 'Other / unassigned' }
export const feedLabels = { vehiclepositions: 'Vehicle positions', tripupdates: 'Trip predictions', alerts: 'Service alerts' }
export const integer = new Intl.NumberFormat('en-GB')
export const compact = new Intl.NumberFormat('en-GB', { notation: 'compact', maximumFractionDigits: 1 })
const clock = new Intl.DateTimeFormat('en-GB', { timeZone: 'Europe/Budapest', hour: '2-digit', minute: '2-digit', second: '2-digit' })
const calendar = new Intl.DateTimeFormat('en-GB', { timeZone: 'UTC', day: 'numeric', month: 'short', year: 'numeric' })

export function localClock(timestamp: string | null): string { return timestamp ? clock.format(new Date(timestamp)) : 'Not available' }
export function calendarDate(timestamp: string | null): string { return timestamp ? calendar.format(new Date(timestamp)) : 'Not available' }
export function age(timestamp: string | null, now: number): number | null { return timestamp ? Math.max(0, (now - Date.parse(timestamp)) / 1000) : null }
export function ageLabel(seconds: number | null): string {
  if (seconds === null) return 'No observation'
  if (seconds < 60) return `${Math.floor(seconds)}s ago`
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`
  return `${Math.floor(seconds / 86400)}d ago`
}
export function effectiveState(snapshot: Snapshot | null, now: number): State {
  if (!snapshot || (age(snapshot.generated_at, now) ?? Infinity) > 90 || Date.parse(snapshot.generated_at) - now > 30000) return 'unknown'
  return snapshot.health.state
}
export function coverage(data: number | null, expected: number | null): string {
  if (data === null || !expected) return 'Not measured'
  return `${(data / expected * 100).toFixed(2)}%`
}
