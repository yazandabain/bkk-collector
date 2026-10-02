import { lazy, Suspense, useMemo, useState } from 'react'
import { feedNames, modes, type Mode, type Snapshot, type State } from '../shared/snapshot'
import { age, ageLabel, calendarDate, compact, coverage, effectiveState, feedLabels, integer, localClock, modeLabels } from './format'
import { useSnapshot } from './useSnapshot'

const github = 'https://github.com/yazandabain/bkk-collector'
const TransitMap = lazy(() => import('./TransitMap').then(module => ({ default: module.TransitMap })))
const colors: Record<Mode, string> = { bus: '#2477b7', tram: '#c58b12', trolleybus: '#c64652', metro: '#745fbb', rail: '#338b70', ferry: '#258b9b', other: '#657489' }
const issueLabels: Record<string, string> = {
  absent: 'No recent response', http_failed: 'Request failed', protobuf_parse_failed: 'Parsing failed',
  poll_journal_failed: 'Poll evidence write failed', raw_archive_failed: 'Raw archive write failed', derived_spool_failed: 'Derived write failed',
  tripupdates_presence_failed: 'Presence evidence write failed', change_tracker_failed: 'Prediction tracking degraded',
  source_timestamp_stale: 'Source timestamp is stale', source_timestamp_frozen: 'Source timestamp is frozen', entity_timestamp_stale: 'Entity timestamps are stale',
  payload_frozen: 'Source content appears frozen', scheduler_overdue: 'Scheduler overdue', request_stuck: 'Request overdue',
  source_timestamp_unchanged_warning: 'Unchanged alert timestamp · advisory only', payload_unchanged_warning: 'Unchanged alerts · advisory only',
  request_exceeds_cadence: 'Response exceeded polling interval', scheduler_missed_deadline: 'Recent deadline missed',
}

function Mark() {
  return <svg className="brand-mark" viewBox="0 0 40 40" aria-hidden="true"><rect width="40" height="40" rx="11" fill="#172d42" /><path d="M10 29V11h10a6 6 0 0 1 0 12H10m10 0 10 6" fill="none" stroke="#6cdbb1" strokeWidth="3" strokeLinecap="round" /><circle cx="29" cy="29" r="3" fill="#ffd66d" /></svg>
}
function Status({ state, text }: { state: State; text?: string }) {
  return <span className={`status status-${state}`}><i />{text ?? ({ healthy: 'Operational', degraded: 'Degraded', unknown: 'Not current' }[state])}</span>
}

function HealthPanel({ snapshot, now, publicCurrent }: { snapshot: Snapshot | null; now: number; publicCurrent: boolean }) {
  const [expanded, setExpanded] = useState(false)
  const maintenance = snapshot?.health.maintenance
  return <aside className="health-panel" aria-labelledby="health-title">
    <div className="panel-heading"><span className="eyebrow">Behind the map</span><h2 id="health-title">Pipeline health</h2><p>Healthy data, not just a running process.</p></div>
    <div className="feed-list">
      {feedNames.map((name, index) => {
        const feed = snapshot?.health.feeds[name]
        const state = publicCurrent ? feed?.state ?? 'unknown' : 'unknown'
        return <div className="feed" key={name}>
          <div className="feed-title"><span className="feed-index">0{index + 1}</span><strong>{feedLabels[name]}</strong><span className={`state-dot state-${state}`} aria-label={state} /></div>
          <div className="feed-meta"><span>{feed?.cadence_seconds ? `Every ${feed.cadence_seconds}s` : 'Cadence unavailable'}</span><span>{ageLabel(age(feed?.observed_at ?? null, now))}</span></div>
          {feed?.issues.filter(code => !code.endsWith('_warning') || expanded).map(code => <p key={code} className="feed-issue">{issueLabels[code]}</p>)}
          {expanded && <dl className="source-details"><dt>Source timestamp</dt><dd>{localClock(feed?.source_at ?? null)}</dd><dt>Entities in response</dt><dd>{feed?.entities === null || feed?.entities === undefined ? 'Not available' : integer.format(feed.entities)}</dd></dl>}
        </div>
      })}
    </div>
    <button className="text-button" onClick={() => setExpanded(!expanded)} aria-expanded={expanded}>{expanded ? 'Hide source details −' : 'View source details +'} </button>
    <div className="archive-state">
      <div><span className="archive-icon" aria-hidden="true">↗</span><strong>Off-site archive</strong><span className={`state-dot state-${publicCurrent ? maintenance?.state ?? 'unknown' : 'unknown'}`} /></div>
      <p>{maintenance?.archive_enabled ? `${maintenance.pending_days} completed ${maintenance.pending_days === 1 ? 'day' : 'days'} awaiting backup` : 'Archive status unavailable'}</p>
      <small>Last verified day: {snapshot?.statistics.last_verified_date ?? 'Not available'} (UTC)</small>
    </div>
    <p className="health-footnote">An old public snapshot means current collector health is unknown. Unchanged alerts alone are not a failure.</p>
  </aside>
}

function Statistics({ snapshot }: { snapshot: Snapshot | null }) {
  const stats = snapshot?.statistics
  const latest = stats?.latest_day
  const metrics = [
    { label: 'Days with collection evidence', value: stats ? integer.format(stats.evidenced_days) : '—', note: stats?.evidenced_since ? `Since ${calendarDate(stats.evidenced_since)}` : 'From completed-day manifests', icon: '◷' },
    { label: 'Recorded feed polls', value: stats ? compact.format(stats.polls_recorded) : '—', note: 'Logged attempts, not uptime', icon: '⌁' },
    { label: 'Derived rows preserved', value: stats ? compact.format(stats.event_rows) : '—', note: 'Observations + prediction changes', icon: '≋' },
    { label: 'Verified archive days', value: stats ? integer.format(stats.verified_days) : '—', note: 'Size- and hash-verified backups', icon: '↗' },
  ]
  return <section className="dataset-section" id="dataset" aria-labelledby="dataset-title">
    <div className="section-heading"><div><span className="eyebrow">Building a record of the city</span><h2 id="dataset-title">A dataset that keeps growing.</h2></div><span className="quiet-note">Completed UTC dates · not a claim of uninterrupted coverage</span></div>
    <div className="metrics">{metrics.map(metric => <article className="metric" key={metric.label}><div className="metric-label">{metric.label}<span aria-hidden="true">{metric.icon}</span></div><strong>{metric.value}</strong><p>{metric.note}</p></article>)}</div>
    {latest && <div className="coverage-strip"><div><strong>Latest completed day</strong><span>{calendarDate(latest.date)} · UTC</span></div>
      {feedNames.map(name => <div key={name}><strong>{feedLabels[name]}</strong><span>{coverage(latest.feeds[name].recorded_polls, latest.feeds[name].expected_polls)} polls recorded</span><small>{coverage(latest.feeds[name].data_polls, latest.feeds[name].expected_polls)} successful persistence</small></div>)}
      <span className={`quality-label quality-${latest.quality}`}>{latest.quality === 'good' ? 'No manifest quality flags' : 'Quality flags recorded'}</span>
    </div>}
    <p className="dataset-note">Counts come from daily evidence, not an estimate. Earlier manifests may not measure successful-persistence coverage. A verified backup is recoverable; it does not certify perfect collection.</p>
  </section>
}

export function App() {
  const { snapshot, error, now } = useSnapshot()
  const [enabled, setEnabled] = useState<Mode[]>([...modes])
  const state = effectiveState(snapshot, now)
  const publicCurrent = !!snapshot && (age(snapshot.generated_at, now) ?? Infinity) <= 90
  const counts = useMemo(() => Object.fromEntries(modes.map(mode => [mode, snapshot?.vehicles.features.filter(vehicle => vehicle.properties.mode === mode).length ?? 0])) as Record<Mode, number>, [snapshot])
  const visible = enabled.reduce((sum, mode) => sum + counts[mode], 0)
  const sourceAge = age(snapshot?.vehicles.observed_at ?? null, now)
  function toggle(mode: Mode) { setEnabled(current => current.includes(mode) ? current.filter(value => value !== mode) : [...current, mode]) }
  return <>
    <a className="skip-link" href="#live-map">Skip to live map</a>
    <header className="masthead"><a href="#" className="brand" aria-label="Budapest Transit Observatory home"><Mark /><span>Budapest<span>TRANSIT OBSERVATORY</span></span></a>
      <nav aria-label="Main navigation"><a href="#methodology">Methodology</a><a href={github} target="_blank" rel="noreferrer">Source code <span aria-hidden="true">↗</span></a></nav>
      <span className="research-label">Independent research infrastructure</span>
    </header>
    <main>
      <section className="intro"><div><span className="eyebrow"><span className="intro-line" />A living record of public transit</span><h1>Budapest, <em>in motion.</em></h1><p>A live window into the network. A durable record for what comes next.</p></div><div className="intro-status"><Status state={state} text={state === 'healthy' ? 'Collection operational' : state === 'degraded' ? 'Collection degraded' : 'Current health unknown'} /><span>Budapest local time · {localClock(new Date(now).toISOString())}</span></div></section>
      {(error || !publicCurrent) && <div className="public-banner" role="status">{snapshot ? 'Public updates are delayed. Showing the last received snapshot; current collector state may differ.' : error ? 'Public data is temporarily unavailable. The collector operates independently; this page will retry.' : 'Connecting to the public snapshot…'}</div>}
      <section className="live-section" id="live-map" aria-label="Live network">
        <div className="map-panel">
          <div className="map-heading"><div><span className="eyebrow">The network, now</span><h2>{snapshot ? integer.format(visible) : '—'} <span>reported vehicles</span></h2></div><span className={`observation-label ${(sourceAge ?? Infinity) > 90 ? 'observation-stale' : ''}`}><i />{ageLabel(sourceAge)}<small>observation</small></span></div>
          <div className="mode-filters" role="group" aria-label="Filter vehicles by transport mode">{modes.map(mode => <button key={mode} aria-pressed={enabled.includes(mode)} onClick={() => toggle(mode)} className={enabled.includes(mode) ? 'enabled' : ''}><i style={{ background: colors[mode] }} />{modeLabels[mode]}<span>{integer.format(counts[mode])}</span></button>)}</div>
          <Suspense fallback={<div className="map-frame"><div className="map-loading"><span className="spinner" />Preparing the city map</div></div>}><TransitMap snapshot={snapshot} enabled={enabled} now={now} /></Suspense>
          <div className="map-meta"><span>Positions from BKK GTFS-Realtime · refreshed independently</span><span>{snapshot?.vehicles.omitted_records ? `${snapshot.vehicles.omitted_records} records not plotted · ` : ''}No inferred movement or arrival promises</span></div>
        </div>
        <HealthPanel snapshot={snapshot} now={now} publicCurrent={publicCurrent} />
      </section>
      <Statistics snapshot={snapshot} />
      <section className="methodology" id="methodology" aria-labelledby="methodology-title"><div className="methodology-intro"><span className="eyebrow">Built for evidence, not just display</span><h2 id="methodology-title">The map is the window.<br />The archive is the work.</h2><p>Budapest’s realtime feeds change continuously. This independent project preserves observations and their provenance so future transit research can start from an honest record.</p><a className="method-link" href={`${github}#what-is-collected-at-what-resolution`} target="_blank" rel="noreferrer">Read the collection methodology ↗</a></div>
        <div className="methodology-details"><div className="architecture"><span>01 <strong>Collect</strong><small>Independent feed schedules</small></span><i>→</i><span>02 <strong>Preserve</strong><small>Raw bytes + durable events</small></span><i>→</i><span>03 <strong>Verify</strong><small>Off-site hashes + receipts</small></span></div>
          <dl className="resolution-list"><div><dt>Vehicle positions</dt><dd>10s observations and full raw snapshots</dd></div><div><dt>Trip predictions</dt><dd>10s evaluation · meaningful changes above 2s</dd></div><div><dt>TripUpdates raw</dt><dd>300s full snapshots + failure fallback</dd></div><div><dt>Service alerts</dt><dd>30s observations · unchanged content is valid</dd></div><div><dt>Static schedules</dt><dd>Daily checks · content-addressed version history</dd></div></dl>
          <p className="method-limitation">Prediction changes compare against the last durably emitted value, with a 30-minute heartbeat. The 2s tolerance is provisional. Five-minute raw snapshots cannot reconstruct every ten-second prediction. Membership changes are recorded separately.</p>
        </div>
      </section>
    </main>
    <footer><div className="footer-brand"><Mark /><div><strong>Budapest Transit Observatory</strong><span>Observe carefully. Preserve faithfully.</span></div></div><div className="attribution">Source data: BKK Zrt. · <a href="https://creativecommons.org/licenses/by/4.0/" target="_blank" rel="noreferrer">CC BY 4.0</a><br />Independent project. Not an official BKK service or journey planner.<br />Source code: <a href="/license.txt">Apache-2.0</a> · <a href="/third-party-notices.txt">Software notices</a> · Map: OpenFreeMap / OpenMapTiles / OpenStreetMap</div></footer>
  </>
}
