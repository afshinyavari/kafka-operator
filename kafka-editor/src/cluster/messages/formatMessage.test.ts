import { describe, expect, it } from 'vitest'
import type { Rendered, RenderedRecord } from '../../api/adminClient'
import {
  formatTimestamp,
  isTombstone,
  previewText,
  recordMatchesFilter,
} from './formatMessage'

function rendered(strategy: string, text: string | null): Rendered {
  return { strategy, schemaRef: null, text, size: 0, truncated: false, warnings: [] }
}

function rec(over: Partial<RenderedRecord> = {}): RenderedRecord {
  return {
    partition: 0,
    offset: 0,
    timestamp: 1700000000000,
    key: rendered('STRING', 'order-42'),
    value: rendered('JSON', '{"status":"SHIPPED"}'),
    headers: [{ key: 'source', value: 'web' }],
    ...over,
  }
}

describe('formatTimestamp', () => {
  it('formats an epoch as a UTC date-time', () => {
    expect(formatTimestamp(1700000000000)).toBe('2023-11-14 22:13:20.000')
  })

  it('renders a missing timestamp as a dash', () => {
    expect(formatTimestamp(0)).toBe('—')
    expect(formatTimestamp(-1)).toBe('—')
  })
})

describe('previewText', () => {
  it('collapses whitespace', () => {
    expect(previewText(rendered('JSON', '{\n  "a": 1\n}'))).toBe('{ "a": 1 }')
  })

  it('truncates long text', () => {
    expect(previewText(rendered('STRING', 'x'.repeat(200)), 10)).toBe(
      'xxxxxxxxxx…',
    )
  })

  it('labels tombstones and empty values', () => {
    expect(previewText(rendered('TOMBSTONE', null))).toBe('∅ tombstone')
    expect(previewText(rendered('EMPTY', ''))).toBe('(empty)')
  })
})

describe('isTombstone', () => {
  it('detects a null-valued record', () => {
    expect(isTombstone(rec())).toBe(false)
    expect(isTombstone(rec({ value: rendered('TOMBSTONE', null) }))).toBe(true)
  })
})

describe('recordMatchesFilter', () => {
  it('matches everything when the query is blank', () => {
    expect(recordMatchesFilter(rec(), '', 'all')).toBe(true)
  })

  it('matches the key, value and headers case-insensitively', () => {
    expect(recordMatchesFilter(rec(), 'ORDER-42', 'key')).toBe(true)
    expect(recordMatchesFilter(rec(), 'shipped', 'value')).toBe(true)
    expect(recordMatchesFilter(rec(), 'web', 'header')).toBe(true)
  })

  it('respects the field scope', () => {
    expect(recordMatchesFilter(rec(), 'shipped', 'key')).toBe(false)
    expect(recordMatchesFilter(rec(), 'order-42', 'all')).toBe(true)
  })
})
