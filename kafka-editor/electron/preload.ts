import { contextBridge } from 'electron'

/**
 * Exposes the bundled backend's origin to the renderer as
 * `window.kafkaEditor.apiBase`, which `src/api/baseUrl.ts` reads. The main
 * process passes it in as a `--api-base=<url>` entry in `additionalArguments`.
 */
function readApiBase(): string {
  const prefix = '--api-base='
  const arg = process.argv.find((a) => a.startsWith(prefix))
  return arg ? arg.slice(prefix.length) : ''
}

contextBridge.exposeInMainWorld('kafkaEditor', {
  apiBase: readApiBase(),
})
