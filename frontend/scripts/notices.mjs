// Retain upstream license texts alongside the browser distribution.
import { readFileSync, readdirSync, existsSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const lock = JSON.parse(readFileSync('package-lock.json', 'utf8'))
const notices = ['Third-party software notices\n\nOriginal project source: Apache-2.0 (see license.txt).\nBKK source data is separate from software licensing.\n']
for (const [directory, entry] of Object.entries(lock.packages)) {
  if (!directory || entry.dev || !existsSync(directory)) continue
  const pkg = JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8'))
  const files = readdirSync(directory).filter(name => /^(licen[sc]e|copying|notice)(\.|$)/i.test(name)).sort()
  let text = files.map(name => readFileSync(join(directory, name), 'utf8')).join('\n\n')
  // This package distributes its complete MIT text in the README, not LICENSE.
  if (!text && pkg.name === 'murmurhash-js') {
    const readme = readFileSync(join(directory, 'README.md'), 'utf8')
    const start = readme.indexOf('## License (MIT)')
    if (start !== -1) text = readme.slice(start)
  }
  if (!text) throw new Error(`Missing upstream license text for ${pkg.name}`)
  notices.push(`\n--- ${pkg.name} ${pkg.version} ---\n\n${text}`)
}
writeFileSync('dist/third-party-notices.txt', notices.join('\n'))
writeFileSync('dist/license.txt', readFileSync('../LICENSE'))
