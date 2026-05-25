import { describe, expect, it } from 'vitest'
import type { Rendered, RenderedRecord } from '../../api/adminClient'
import { csvCell, toCsv, toJson } from './exportMessages'

function rendered(strategy: string, text: string | null): Rendered {
  return { strategy, schemaRef: null, text, size: 0, truncated: false, warnings: [] }
}

function rec(over: Partial<RenderedRecord> = {}): RenderedRecord {
  return {
    partition: 0,
    offset: 0,
    timestamp: 1700000000000,
    key: rendered('STRING', 'k'),
    value: rendered('JSON', '{"a":1}'),
    headers: [],
    ...over,
  }
}

describe('csvCell', () => {
  it('leaves a plain value untouched', () => {
    expect(csvCell('plain')).toBe('plain')
  })

  it('quotes and escapes commas, quotes and newlines', () => {
    expect(csvCell('a,b')).toBe('"a,b"')
    expect(csvCell('say "hi"')).toBe('"say ""hi"""')
    expect(csvCell('line1\nline2')).toBe('"line1\nline2"')
  })

  it('renders null as an empty cell', () => {
    expect(csvCell(null)).toBe('')
  })
})

describe('toCsv', () => {
  it('writes a header row and one row per record', () => {
    const csv = toCsv([rec({ offset: 5 })])
    const lines = csv.split('\n')
    expect(lines[0]).toBe('partition,offset,timestamp,key,value')
    expect(lines[1]).toBe('0,5,1700000000000,k,"{""a"":1}"')
  })

  it('renders a tombstone value as an empty cell', () => {
    const csv = toCsv([rec({ value: rendered('TOMBSTONE', null) })])
    expect(csv.split('\n')[1]).toBe('0,0,1700000000000,k,')
  })
})

describe('toJson', () => {
  it('serialises records with headers as an object', () => {
    const parsed = JSON.parse(
      toJson([rec({ headers: [{ key: 'h', value: 'v' }] })]),
    )
    expect(parsed[0].value).toBe('{"a":1}')
    expect(parsed[0].headers).toEqual({ h: 'v' })
    expect(parsed[0].tombstone).toBe(false)
  })

  it('marks a tombstone record', () => {
    const parsed = JSON.parse(
      toJson([rec({ value: rendered('TOMBSTONE', null) })]),
    )
    expect(parsed[0].value).toBeNull()
    expect(parsed[0].tombstone).toBe(true)
  })
})
