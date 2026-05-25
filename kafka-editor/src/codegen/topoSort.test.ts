import { describe, it, expect } from 'vitest'
import { topoSort } from './topoSort'

describe('topoSort', () => {
  it('orders nodes so dependencies come first', () => {
    const nodes = [{ id: 'c' }, { id: 'a' }, { id: 'b' }]
    const edges = [
      { source: 'a', target: 'b' },
      { source: 'b', target: 'c' },
    ]
    expect(topoSort(nodes, edges)?.map((n) => n.id)).toEqual(['a', 'b', 'c'])
  })

  it('returns null for a cyclic graph', () => {
    const nodes = [{ id: 'a' }, { id: 'b' }]
    const edges = [
      { source: 'a', target: 'b' },
      { source: 'b', target: 'a' },
    ]
    expect(topoSort(nodes, edges)).toBeNull()
  })

  it('handles a graph with no edges', () => {
    expect(topoSort([{ id: 'x' }, { id: 'y' }], [])).toHaveLength(2)
  })

  it('handles a fan-out (one source, two consumers)', () => {
    const nodes = [{ id: 's' }, { id: 'a' }, { id: 'b' }]
    const edges = [
      { source: 's', target: 'a' },
      { source: 's', target: 'b' },
    ]
    const order = topoSort(nodes, edges)?.map((n) => n.id) ?? []
    expect(order[0]).toBe('s')
    expect(order).toHaveLength(3)
  })
})
