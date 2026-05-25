import { describe, it, expect } from 'vitest'
import { validateGraph } from './validation'
import type { KafkaEdge, KafkaNode } from '../model/graph'

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
  sourceHandle: string,
  target: string,
  targetHandle: string,
): KafkaEdge => ({ id, source, sourceHandle, target, targetHandle })

describe('validateGraph', () => {
  it('reports no diagnostics for an empty graph', () => {
    expect(validateGraph([], [])).toEqual([])
  })

  it('flags a missing required property as an error', () => {
    // A source with no topicId set.
    const errors = validateGraph([node('s', 'source')], []).filter(
      (d) => d.severity === 'error',
    )
    expect(errors.some((d) => d.message.includes('Topic'))).toBe(true)
  })

  it('warns about an unconnected input port', () => {
    const diagnostics = validateGraph([node('f', 'filter')], [])
    expect(
      diagnostics.some(
        (d) => d.severity === 'warning' && d.message.includes('not connected'),
      ),
    ).toBe(true)
  })

  it('flags an edge whose data kind the target rejects', () => {
    // KTable source -> map (map is KStream-only).
    const nodes = [node('t', 'table-source'), node('m', 'map')]
    const edges = [edge('e', 't', 'out', 'm', 'in')]
    const errors = validateGraph(nodes, edges).filter(
      (d) => d.severity === 'error',
    )
    expect(errors.some((d) => d.message.includes('expected'))).toBe(true)
  })

  it('detects a cycle', () => {
    const nodes = [node('a', 'filter'), node('b', 'filter')]
    const edges = [
      edge('e1', 'a', 'out', 'b', 'in'),
      edge('e2', 'b', 'out', 'a', 'in'),
    ]
    expect(
      validateGraph(nodes, edges).some((d) =>
        d.message.includes('cycle'),
      ),
    ).toBe(true)
  })
})
