/**
 * Stages the built Quarkus backend at `resources/backend/`, ready for
 * electron-builder to ship as an extraResource.
 *
 * The quarkus-app/ tree is a unit: `quarkus-run.jar` is a thin launcher that
 * resolves classes from sibling `lib/`, `app/` and `quarkus/` directories, so
 * the whole tree must travel together.
 */
import { cpSync, existsSync, rmSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const src = join(root, 'backend', 'target', 'quarkus-app')
const dst = join(root, 'resources', 'backend')

if (!existsSync(src)) {
  console.error(
    `assemble: ${src} not found — run \`npm run build:backend\` first`,
  )
  process.exit(1)
}

if (existsSync(dst)) rmSync(dst, { recursive: true, force: true })
cpSync(src, dst, { recursive: true })
console.log(`assemble: ${src} → ${dst}`)
