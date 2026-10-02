import { Component, type ReactNode } from 'react'

/** A failed optional map download must not take health/statistics off-screen. */
export class MapBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false }
  static getDerivedStateFromError() { return { failed: true } }
  render() {
    if (this.state.failed) return <div className="map-frame"><div className="map-loading">
      <strong>The map could not be loaded</strong>
      <p>Live counts, collection health and dataset evidence remain available.</p>
      <button className="text-button" onClick={() => window.location.reload()}>Reload the public page</button>
    </div></div>
    return this.props.children
  }
}
