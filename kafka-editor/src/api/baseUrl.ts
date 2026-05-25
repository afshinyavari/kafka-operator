/**
 * Resolves the origin of the Kafka Editor backend.
 *
 * - In the browser (web dev / `vite build`) this returns `''`, so API clients
 *   use relative paths (`/api/...`) and the Vite dev proxy reaches the backend.
 * - In the packaged desktop app, the Electron preload script injects
 *   `window.kafkaEditor.apiBase` with the real backend origin
 *   (`http://127.0.0.1:<port>`), since there is no dev proxy.
 */

declare global {
  interface Window {
    kafkaEditor?: { apiBase: string }
  }
}

/** Origin to prefix backend URLs with — `''` on the web, an absolute URL in Electron. */
export function apiBase(): string {
  if (typeof window !== 'undefined' && window.kafkaEditor?.apiBase) {
    return window.kafkaEditor.apiBase
  }
  return ''
}
