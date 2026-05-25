/** Pure helpers for the topic-config diff editor. */

export interface ConfigRow {
  id: string
  name: string
  value: string
  /** The value the topic currently has — '' for an unset/default key. */
  original: string
}

/**
 * The config changes to send: name -> new value. A row is a change when its
 * value differs from the original; a blank value means "reset to default"
 * (the backend turns that into a DELETE op). Blank-named rows are ignored.
 */
export function computeConfigDiff(rows: ConfigRow[]): Record<string, string> {
  const diff: Record<string, string> = {}
  for (const row of rows) {
    const name = row.name.trim()
    if (!name) continue
    if (row.value !== row.original) diff[name] = row.value
  }
  return diff
}

/** Parse a `key=value` per line block (used by the create-topic form). */
export function parseConfigText(text: string): Record<string, string> {
  const out: Record<string, string> = {}
  for (const line of text.split('\n')) {
    const trimmed = line.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const eq = trimmed.indexOf('=')
    if (eq <= 0) continue
    out[trimmed.slice(0, eq).trim()] = trimmed.slice(eq + 1).trim()
  }
  return out
}
