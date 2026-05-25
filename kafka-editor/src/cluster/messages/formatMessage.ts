import type { Rendered, RenderedRecord } from '../../api/adminClient'

/** Pure helpers for rendering and filtering browsed messages. */

export type FilterField = 'all' | 'key' | 'value' | 'header'

/** A record's timestamp as `YYYY-MM-DD HH:MM:SS.mmm`, or a dash. */
export function formatTimestamp(ms: number): string {
  if (!ms || ms < 0) return '—'
  return new Date(ms).toISOString().replace('T', ' ').replace('Z', '')
}

/** A single-line preview of a rendered key/value for the row list. */
export function previewText(rendered: Rendered, max = 140): string {
  if (rendered.strategy === 'TOMBSTONE') return '∅ tombstone'
  if (rendered.strategy === 'EMPTY') return '(empty)'
  const collapsed = (rendered.text ?? '').replace(/\s+/g, ' ').trim()
  return collapsed.length > max ? `${collapsed.slice(0, max)}…` : collapsed
}

/** True when a record's value is a tombstone (null). */
export function isTombstone(record: RenderedRecord): boolean {
  return record.value.strategy === 'TOMBSTONE'
}

/** A stable list key for a record. */
export function recordKey(record: RenderedRecord): string {
  return `${record.partition}-${record.offset}`
}

/** Client-side substring filter over a record's key / value / headers. */
export function recordMatchesFilter(
  record: RenderedRecord,
  query: string,
  field: FilterField,
): boolean {
  const needle = query.trim().toLowerCase()
  if (!needle) return true
  const has = (text: string | null) =>
    text != null && text.toLowerCase().includes(needle)
  const inHeaders = () =>
    record.headers.some((h) => has(h.key) || has(h.value))
  switch (field) {
    case 'key':
      return has(record.key.text)
    case 'value':
      return has(record.value.text)
    case 'header':
      return inHeaders()
    default:
      return has(record.key.text) || has(record.value.text) || inHeaders()
  }
}
