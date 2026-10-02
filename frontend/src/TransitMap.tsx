import { useEffect, useRef, useState } from 'react'
import { Map, NavigationControl, setWorkerUrl, type GeoJSONSource } from 'maplibre-gl'
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url'
import 'maplibre-gl/dist/maplibre-gl.css'
import type { Mode, Snapshot, Vehicle } from '../shared/snapshot'
import { age, ageLabel, modeLabels } from './format'

setWorkerUrl(workerUrl)
const empty = { type: 'FeatureCollection' as const, features: [] }

export function TransitMap({ snapshot, enabled, now }: { snapshot: Snapshot | null; enabled: Mode[]; now: number }) {
  const container = useRef<HTMLDivElement>(null)
  const instance = useRef<Map | null>(null)
  const data = useRef(snapshot)
  data.current = snapshot
  const [ready, setReady] = useState(false)
  const [unavailable, setUnavailable] = useState(false)
  const [selected, setSelected] = useState<Vehicle | null>(null)
  const [tileError, setTileError] = useState(false)
  const missingSnapshot = !!snapshot && (snapshot.vehicles.observed_at === null
    || snapshot.public_layer_issues.includes('vehicle_snapshot_unavailable'))
  useEffect(() => {
    if (!container.current) return
    let map: Map
    try {
      map = new Map({ container: container.current, style: 'https://tiles.openfreemap.org/styles/positron',
        center: [19.065, 47.493], zoom: 11.1, minZoom: 7, maxZoom: 17, maxBounds: [[18.3, 46.9], [20.0, 48.2]],
        attributionControl: { compact: true }, cooperativeGestures: true })
    } catch {
      setUnavailable(true)
      return
    }
    instance.current = map
    map.addControl(new NavigationControl({ showCompass: false }), 'top-right')
    map.on('load', () => {
      map.addSource('vehicles', { type: 'geojson', data: empty, promoteId: 'public_id' })
      map.addLayer({ id: 'vehicle-halo', type: 'circle', source: 'vehicles', paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 8, 4, 12, 7, 16, 10],
        'circle-color': '#ffffff', 'circle-opacity': 0.94,
      } })
      map.addLayer({ id: 'vehicles', type: 'circle', source: 'vehicles', paint: {
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 8, 2.5, 12, 4.5, 16, 7],
        'circle-color': ['get', 'color'], 'circle-stroke-color': '#263b4b', 'circle-stroke-width': 0.6,
      } })
      map.addLayer({ id: 'route-labels', type: 'symbol', source: 'vehicles', minzoom: 13, layout: {
        'text-field': ['get', 'route_label'], 'text-size': 11, 'text-offset': [0, 1.2], 'text-anchor': 'top',
      }, paint: { 'text-color': '#172d42', 'text-halo-color': '#ffffff', 'text-halo-width': 2 } })
      setReady(true)
      setTileError(false)
    })
    map.on('click', 'vehicles', event => {
      const id = event.features?.[0]?.properties.public_id
      const match = data.current?.vehicles.features.find(vehicle => vehicle.id === String(id))
      setSelected(match ?? null)
    })
    map.on('mouseenter', 'vehicles', () => { map.getCanvas().style.cursor = 'pointer' })
    map.on('mouseleave', 'vehicles', () => { map.getCanvas().style.cursor = '' })
    map.on('error', () => setTileError(true))
    map.on('idle', () => { if (map.areTilesLoaded()) setTileError(false) })
    return () => { instance.current = null; map.remove() }
  }, [])
  useEffect(() => {
    const map = instance.current
    if (!map || !ready) return
    // GeoJSON tiling may omit string top-level IDs. Keep our opaque public ID
    // as a promoted property so selection survives tiling and later refreshes.
    const features = (missingSnapshot ? [] : snapshot?.vehicles.features)?.filter(vehicle => enabled.includes(vehicle.properties.mode))
      .map(vehicle => ({ ...vehicle, properties: { ...vehicle.properties, public_id: vehicle.id } })) ?? []
    ;(map.getSource('vehicles') as GeoJSONSource).setData({ type: 'FeatureCollection', features })
    if (selected && !features.some(vehicle => vehicle.id === selected.id)) setSelected(null)
  }, [snapshot, enabled, ready, selected, missingSnapshot])
  const selectedCurrent = selected ? snapshot?.vehicles.features.find(vehicle => vehicle.id === selected.id) ?? selected : null
  const stale = (age(snapshot?.vehicles.observed_at ?? null, now) ?? Infinity) > 90
    || (age(snapshot?.generated_at ?? null, now) ?? Infinity) > 90
    || (snapshot?.vehicles.source_at !== null && (age(snapshot?.vehicles.source_at ?? null, now) ?? 0) > 180)
  return <div className="map-frame">
    <div ref={container} className="map" role="region" aria-label="Interactive Budapest vehicle map" />
    {!ready && !unavailable && <div className="map-loading"><span className="spinner" />Preparing the city map</div>}
    {unavailable && <div className="map-loading"><strong>Map rendering is unavailable</strong><p>Your browser may not support WebGL. Live counts and feed health remain available below.</p></div>}
    {ready && (stale || !snapshot || missingSnapshot) && <div className="map-notice">{missingSnapshot ? 'Vehicle map snapshot unavailable · collection operates independently' : snapshot ? 'Last reported positions · source or public updates are stale' : 'Waiting for the first public snapshot'}</div>}
    {tileError && ready && <div className="map-tile-warning">Some basemap tiles are unavailable</div>}
    {ready && <button className="map-reset" onClick={() => instance.current?.easeTo({ center: [19.065, 47.493], zoom: 11.1, duration: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 0 : 500 })} aria-label="Reset map to Budapest"><svg width="15" height="15" viewBox="0 0 20 20" aria-hidden="true"><circle cx="10" cy="10" r="5" fill="none" stroke="currentColor" strokeWidth="1.4" /><path d="M10 1v5m0 8v5M1 10h5m8 0h5" stroke="currentColor" strokeWidth="1.4" /></svg><span>Budapest</span></button>}
    {selectedCurrent && <div className="vehicle-detail">
      <button className="close-detail" aria-label="Close vehicle details" onClick={() => setSelected(null)}>×</button>
      <span className="eyebrow">Selected vehicle</span>
      <strong><span className="route-badge" style={{ background: selectedCurrent.properties.color }}>{selectedCurrent.properties.route_label}</span>{modeLabels[selectedCurrent.properties.mode]}</strong>
      <span>Vehicle timestamp {ageLabel(age(selectedCurrent.properties.recorded_at, now))}</span>
      <small>Reported position, not an interpolated journey.</small>
    </div>}
    <div className="map-caption">Reported positions · Budapest metropolitan area</div>
  </div>
}
