/**
 * Post-processing for the Electron main-process build.
 *
 * The repo's package.json declares `"type": "module"`, so plain `.js` files
 * are treated as ESM. The Electron main process is compiled to CommonJS
 * (simplest: `require`, `__dirname`, sandboxed preload all just work), so we
 * drop a `package.json` into the output dir to mark it as CommonJS.
 *
 * Also copies static assets (the splash screen) next to the compiled code.
 */
import { copyFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const outDir = join(root, 'electron-dist')

mkdirSync(outDir, { recursive: true })

writeFileSync(
  join(outDir, 'package.json'),
  JSON.stringify({ type: 'commonjs' }, null, 2) + '\n',
)

copyFileSync(
  join(root, 'electron', 'splash.html'),
  join(outDir, 'splash.html'),
)

console.log('finalize-electron-build: wrote electron-dist/package.json + splash.html')
