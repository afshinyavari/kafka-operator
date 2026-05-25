import { describe, it, expect } from 'vitest'
import {
  inferRecordTypes,
  inputRecordTypeId,
  recordTypeIdForInput,
} from './recordTypeInference'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import type { Catalog } from '../model/catalog'

function node(
  id: string,
  type: string,
  config: Record<string, unknown> = {},
): KafkaNode {
  return { id, type, position: { x: 0, y: 0 }, data: { config } }
}

const edge = (
  id: string,
  source: string,
  target: string,
): KafkaEdge => ({
  id,
  source,
  sourceHandle: 'out',
  target,
  targetHandle: 'in',
})

describe('inferRecordTypes', () => {
  it('reads the declared value type off a source', () => {
    const nodes = [node('s', 'source', { valueRecordTypeId: 'rt1' })]
    expect(inferRecordTypes(nodes, []).get('s:out')).toBe('rt1')
  })

  it('propagates the value type through pass-through operators', () => {
    const nodes = [
      node('s', 'source', { valueRecordTypeId: 'rt1' }),
      node('f', 'filter'),
      node('p', 'peek'),
    ]
    const edges = [edge('e1', 's', 'f'), edge('e2', 'f', 'p')]
    const kinds = inferRecordTypes(nodes, edges)
    expect(kinds.get('f:out')).toBe('rt1')
    expect(kinds.get('p:out')).toBe('rt1')
  })

  it('stops propagation at a value-reshaping operator (map-values)', () => {
    const nodes = [
      node('s', 'source', { valueRecordTypeId: 'rt1' }),
      node('m', 'map-values'),
    ]
    expect(inferRecordTypes(nodes, [edge('e1', 's', 'm')]).has('m:out')).toBe(
      false,
    )
  })

  it('leaves a source with no declared type unresolved', () => {
    expect(inferRecordTypes([node('s', 'source')], []).has('s:out')).toBe(false)
  })
})

describe('catalog topic typing', () => {
  const catalog: Catalog = {
    topics: [
      {
        id: 't1',
        name: 'orders',
        valueFormat: 'avro',
        valueRecordTypeId: 'rtTopic',
      },
    ],
  }

  it('inherits the value type from the referenced catalog topic', () => {
    const nodes = [node('s', 'source', { topicId: 't1' })]
    expect(inferRecordTypes(nodes, [], catalog).get('s:out')).toBe('rtTopic')
  })

  it('lets a node-level Value Type override the topic', () => {
    const nodes = [
      node('s', 'source', { topicId: 't1', valueRecordTypeId: 'rtOverride' }),
    ]
    expect(inferRecordTypes(nodes, [], catalog).get('s:out')).toBe('rtOverride')
  })
})

describe('inputRecordTypeId', () => {
  it('resolves the record type entering a filter', () => {
    const nodes = [
      node('s', 'source', { valueRecordTypeId: 'rt1' }),
      node('f', 'filter'),
    ]
    expect(inputRecordTypeId('f', nodes, [edge('e1', 's', 'f')])).toBe('rt1')
  })

  it('returns undefined for a node with no incoming edge', () => {
    const nodes = [node('s', 'source', { valueRecordTypeId: 'rt1' })]
    expect(inputRecordTypeId('s', nodes, [])).toBeUndefined()
  })
})

describe('recordTypeIdForInput', () => {
  it('resolves each named input port of a join independently', () => {
    const nodes = [
      node('s1', 'source', { valueRecordTypeId: 'rtL' }),
      node('s2', 'table-source', { valueRecordTypeId: 'rtR' }),
      node('j', 'stream-table-join'),
    ]
    const edges: KafkaEdge[] = [
      { id: 'e1', source: 's1', sourceHandle: 'out', target: 'j', targetHandle: 'left' },
      { id: 'e2', source: 's2', sourceHandle: 'out', target: 'j', targetHandle: 'right' },
    ]
    expect(recordTypeIdForInput('j', 'left', nodes, edges)).toBe('rtL')
    expect(recordTypeIdForInput('j', 'right', nodes, edges)).toBe('rtR')
  })
})
