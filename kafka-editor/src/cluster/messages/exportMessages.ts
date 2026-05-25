import type { RenderedRecord } from '../../api/adminClient'

/** Export browsed messages to JSON or CSV. */

function valueText(record: RenderedRecord): string | null {
  return record.value.strategy === 'TOMBSTONE' ? null : record.value.text
}

/** A pretty-printed JSON array of the records. */
export function toJson(records: RenderedRecord[]): string {
  return JSON.stringify(
    records.map((r) => ({
      partition: r.partition,
      offset: r.offset,
      timestamp: r.timestamp,
      key: r.key.strategy === 'TOMBSTONE' ? null : r.key.text,
      value: valueText(r),
      tombstone: r.value.strategy === 'TOMBSTONE',
      headers: Object.fromEntries(r.headers.map((h) => [h.key, h.value])),
    })),
    null,
    2,
  )
}

/** Escape one CSV cell — quote when it contains a comma, quote or newline. */
export function csvCell(value: string | null): string {
  if (value == null) return ''
  return /[",\n\r]/.test(value)
    ? `"${value.replace(/"/g, '""')}"`
    : value
}

/** A CSV document with a header row. */
export function toCsv(records: RenderedRecord[]): string {
  const lines = ['partition,offset,timestamp,key,value']
  for (const r of records) {
    lines.push(
      [
        r.partition,
        r.offset,
        r.timestamp,
        csvCell(r.key.strategy === 'TOMBSTONE' ? null : r.key.text),
        csvCell(valueText(r)),
      ].join(','),
    )
  }
  return lines.join('\n')
}

/** Trigger a browser download of text content. */
export function downloadText(
  filename: string,
  text: string,
  mime: string,
): void {
  const blob = new Blob([text], { type: mime })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)
}
