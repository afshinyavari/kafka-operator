import { describe, it, expect } from 'vitest'
import { computeOutputTypes, generateOutputRecord } from './outputTypes'
import { createEmptyProject } from '../model/project'

describe('computeOutputTypes', () => {
  it('builds an output type for a mapValues node, resolving field types', () => {
    const doc = createEmptyProject()
    doc.recordTypes = [
      {
        id: 'rtIn',
        name: 'Order',
        source: { kind: 'manual' },
        fields: [
          { name: 'amount', type: { kind: 'primitive', primitive: 'double' } },
        ],
      },
    ]
    doc.nodes = [
      {
        id: 'src',
        type: 'source',
        position: { x: 0, y: 0 },
        config: { valueRecordTypeId: 'rtIn' },
      },
      {
        id: 'mv',
        type: 'map-values',
        position: { x: 0, y: 0 },
        config: {
          label: 'Map Values',
          valueMapping: [
            { id: '1', outputField: 'total', expression: { kind: 'field', path: 'amount' } },
            { id: '2', outputField: 'currency', expression: { kind: 'literal', value: 'EUR' } },
          ],
        },
      },
    ]
    doc.edges = [
      { id: 'e', source: 'src', sourceHandle: 'out', target: 'mv', targetHandle: 'in' },
    ]
    const mv = computeOutputTypes(doc).get('mv')
    expect(mv?.className).toBe('MapValuesOutput')
    expect(mv?.fields).toEqual([
      { name: 'total', javaType: 'double' },
      { name: 'currency', javaType: 'String' },
    ])
  })

  it('emits no output type for an empty mapping', () => {
    const doc = createEmptyProject()
    doc.nodes = [
      {
        id: 'mv',
        type: 'map-values',
        position: { x: 0, y: 0 },
        config: { valueMapping: [] },
      },
    ]
    expect(computeOutputTypes(doc).size).toBe(0)
  })

  it('generateOutputRecord emits a Java record', () => {
    const java = generateOutputRecord({
      className: 'Foo',
      fields: [{ name: 'x', javaType: 'long' }],
    })
    expect(java).toContain('public record Foo(')
    expect(java).toContain('long x')
  })
})
