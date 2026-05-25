import { describe, it, expect } from 'vitest'
import { validateConnection } from './connectionRules'
import type { KafkaEdge, KafkaNode } from '../model/graph'

function node(id: string, type: string): KafkaNode {
  return { id, type, position: { x: 0, y: 0 }, data: { config: {} } }
}

const src = node('s', 'source') // KStream
const tbl = node('t', 'table-source') // KTable
const flt = node('f', 'filter') // polymorphic
const mp = node('m', 'map') // KStream-only
const join = node('j', 'stream-table-join') // left KStream, right KTable
const nodes = [src, tbl, flt, mp, join]

function conn(
  source: string,
  sourceHandle: string,
  target: string,
  targetHandle: string,
) {
  return { source, sourceHandle, target, targetHandle }
}

describe('validateConnection', () => {
  it('allows a KStream source into a filter', () => {
    expect(validateConnection(conn('s', 'out', 'f', 'in'), nodes, [])).toBe(true)
  })

  it('allows a KTable source into a polymorphic filter', () => {
    expect(validateConnection(conn('t', 'out', 'f', 'in'), nodes, [])).toBe(true)
  })

  it('rejects a KTable source into a KStream-only map', () => {
    expect(validateConnection(conn('t', 'out', 'm', 'in'), nodes, [])).toBe(false)
  })

  it('rejects self-connections', () => {
    expect(validateConnection(conn('f', 'out', 'f', 'in'), nodes, [])).toBe(false)
  })

  it('rejects a second edge into an occupied input port', () => {
    const existing: KafkaEdge[] = [
      { id: 'e', source: 's', sourceHandle: 'out', target: 'f', targetHandle: 'in' },
    ]
    expect(validateConnection(conn('s', 'out', 'f', 'in'), nodes, existing)).toBe(
      false,
    )
  })

  it('allows a KTable into the table input of a stream-table join', () => {
    expect(validateConnection(conn('t', 'out', 'j', 'right'), nodes, [])).toBe(
      true,
    )
  })

  it('rejects a KStream into the table input of a stream-table join', () => {
    expect(validateConnection(conn('s', 'out', 'j', 'right'), nodes, [])).toBe(
      false,
    )
  })

  it('propagates KTable through a filter — filter fed by a table cannot feed a map', () => {
    const edges: KafkaEdge[] = [
      { id: 'e1', source: 't', sourceHandle: 'out', target: 'f', targetHandle: 'in' },
    ]
    expect(validateConnection(conn('f', 'out', 'm', 'in'), nodes, edges)).toBe(
      false,
    )
  })

  it('propagates KStream through a filter — filter fed by a stream can feed a map', () => {
    const edges: KafkaEdge[] = [
      { id: 'e1', source: 's', sourceHandle: 'out', target: 'f', targetHandle: 'in' },
    ]
    expect(validateConnection(conn('f', 'out', 'm', 'in'), nodes, edges)).toBe(
      true,
    )
  })

  it('is permissive when the source kind is not yet known', () => {
    // An unconnected filter has an undetermined kind — allow and resolve later.
    expect(validateConnection(conn('f', 'out', 'm', 'in'), nodes, [])).toBe(true)
  })
})

describe('validateConnection — stateful operators', () => {
  const stateful = [
    node('s', 'source'),
    node('g', 'group-by-key'),
    node('c', 'count'),
  ]

  it('allows a KStream into group-by-key', () => {
    expect(validateConnection(conn('s', 'out', 'g', 'in'), stateful, [])).toBe(
      true,
    )
  })

  it('allows a KGroupedStream into count', () => {
    expect(validateConnection(conn('g', 'out', 'c', 'in'), stateful, [])).toBe(
      true,
    )
  })

  it('rejects a KStream directly into count (grouping is required first)', () => {
    expect(validateConnection(conn('s', 'out', 'c', 'in'), stateful, [])).toBe(
      false,
    )
  })
})
