import { describe, it, expect } from 'vitest'
import { inferPortKinds } from './typeInference'
import type { KafkaEdge, KafkaNode } from '../model/graph'

function node(id: string, type: string): KafkaNode {
  return { id, type, position: { x: 0, y: 0 }, data: { config: {} } }
}

describe('inferPortKinds', () => {
  it('resolves fixed source kinds', () => {
    const kinds = inferPortKinds(
      [node('s', 'source'), node('t', 'table-source')],
      [],
    )
    expect(kinds.get('s:out')).toBe('KStream')
    expect(kinds.get('t:out')).toBe('KTable')
  })

  it('inherits KTable through a polymorphic filter', () => {
    const nodes = [node('t', 'table-source'), node('f', 'filter')]
    const edges: KafkaEdge[] = [
      { id: 'e', source: 't', sourceHandle: 'out', target: 'f', targetHandle: 'in' },
    ]
    expect(inferPortKinds(nodes, edges).get('f:out')).toBe('KTable')
  })

  it('inherits KStream through a chain of polymorphic nodes', () => {
    const nodes = [
      node('s', 'source'),
      node('f1', 'filter'),
      node('f2', 'map-values'),
    ]
    const edges: KafkaEdge[] = [
      { id: 'e1', source: 's', sourceHandle: 'out', target: 'f1', targetHandle: 'in' },
      { id: 'e2', source: 'f1', sourceHandle: 'out', target: 'f2', targetHandle: 'in' },
    ]
    expect(inferPortKinds(nodes, edges).get('f2:out')).toBe('KStream')
  })

  it('leaves an unconnected polymorphic node unresolved', () => {
    expect(inferPortKinds([node('f', 'filter')], []).has('f:out')).toBe(false)
  })

  it('resolves a fixed conversion regardless of input', () => {
    // to-stream always produces KStream even fed by a KTable.
    const nodes = [node('t', 'table-source'), node('c', 'to-stream')]
    const edges: KafkaEdge[] = [
      { id: 'e', source: 't', sourceHandle: 'out', target: 'c', targetHandle: 'in' },
    ]
    expect(inferPortKinds(nodes, edges).get('c:out')).toBe('KStream')
  })
})
