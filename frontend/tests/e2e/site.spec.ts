import { expect, test } from '@playwright/test'
import { snapshotAt } from '../fixture'
import AxeBuilder from '@axe-core/playwright'

// Tests are offline: a small blank vector style exercises real MapLibre/WebGL,
// while public snapshots are synthetic and no BKK/Cloudflare service is called.
test.beforeEach(async ({ page }) => {
  await page.route('https://tiles.openfreemap.org/**', route => route.fulfill({ json: {
    version: 8, sources: {}, layers: [{ id: 'background', type: 'background', paint: { 'background-color': '#eaf0f2' } }],
  } }))
})

test('live layout, filters, details and honest historical counts', async ({ page }) => {
  await page.route('**/api/snapshot', route => route.fulfill({ json: snapshotAt() }))
  await page.goto('/')
  await expect(page.getByRole('heading', { level: 1 })).toHaveText('Budapest, in motion.')
  await expect(page.getByText('Collection operational', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Reset map to Budapest' })).toBeVisible()
  const bus = page.getByRole('button', { name: 'Bus 1', exact: true })
  await bus.click()
  await expect(bus).toHaveAttribute('aria-pressed', 'false')
  await expect(page.getByRole('heading', { name: '1 reported vehicles' })).toBeVisible()
  await page.getByRole('button', { name: 'View source details +' }).click()
  await expect(page.getByText('Unchanged alert timestamp · advisory only')).toBeVisible()
  await expect(page.getByText('Not measured successful persistence')).toBeVisible()
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true)
  await page.getByRole('link', { name: 'Skip to live map' }).focus()
  await expect(page.getByRole('link', { name: 'Skip to live map' })).toBeVisible()
})

test('unavailable public API is not portrayed as collector failure', async ({ page }) => {
  await page.route('**/api/snapshot', route => route.fulfill({ status: 503, body: 'unavailable' }))
  await page.goto('/')
  await expect(page.getByRole('status')).toContainText('collector operates independently')
  await expect(page.getByText('Current health unknown')).toBeVisible()
  await expect(page.getByText('Collection degraded')).not.toBeVisible()
})

test('stale generation and source remain clearly marked', async ({ page }) => {
  await page.route('**/api/snapshot', route => route.fulfill({ json: snapshotAt(Date.now() - 180000) }))
  await page.goto('/')
  await expect(page.getByText('Current health unknown')).toBeVisible()
  await expect(page.getByText('Last reported positions · source or public updates are stale')).toBeVisible()
  await expect(page.getByRole('status')).toContainText('last received snapshot')
})

test('WebGL failure preserves live textual information', async ({ page }) => {
  await page.addInitScript(() => {
    const original = HTMLCanvasElement.prototype.getContext
    HTMLCanvasElement.prototype.getContext = new Proxy(original, { apply(target, canvas, args) {
      if (String(args[0]).includes('webgl')) return null
      return Reflect.apply(target, canvas, args)
    } })
  })
  await page.route('**/api/snapshot', route => route.fulfill({ json: snapshotAt() }))
  await page.goto('/')
  await expect(page.getByText('Map rendering is unavailable')).toBeVisible()
  await expect(page.getByRole('heading', { name: '2 reported vehicles' })).toBeVisible()
  await expect(page.getByText('Collection operational', { exact: true })).toBeVisible()
})

test('extra operational fields fail closed and script-like labels are text only', async ({ page }) => {
  await page.route('**/api/snapshot', route => {
    const snapshot = snapshotAt()
    Object.assign(snapshot, { secret: 'not-public' })
    return route.fulfill({ json: snapshot })
  })
  await page.goto('/')
  await expect(page.getByRole('status')).toContainText('temporarily unavailable')
  await expect(page.getByText('not-public')).not.toBeVisible()
})

test('public interface passes automated accessibility checks', async ({ page }) => {
  await page.route('**/api/snapshot', route => route.fulfill({ json: snapshotAt() }))
  await page.goto('/')
  await expect(page.getByRole('button', { name: 'Reset map to Budapest' })).toBeVisible()
  const result = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21aa']).analyze()
  expect(result.violations).toEqual([])
})
