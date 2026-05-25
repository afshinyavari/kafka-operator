import { describe, it, expect } from 'vitest'
import {
  defaultFieldType,
  flattenFieldPaths,
  newField,
  newRecordType,
} from './recordTypes'
import type { RecordType } from './recordTypes'

describe('record types', () => {
  it('newRecordType / newField produce sane manual defaults', () => {
    const rt = newRecordType('Order')
    expect(rt.name).toBe('Order')
    expect(rt.source.kind).toBe('manual')
    expect(rt.fields).toEqual([])

    const field = newField('amount')
    expect(field.name).toBe('amount')
    expect(field.type).toEqual({ kind: 'primitive', primitive: 'string' })
  })

  it('defaultFieldType gives a usable shape per kind', () => {
    expect(defaultFieldType('array')).toEqual({
      kind: 'array',
      items: { kind: 'primitive', primitive: 'string' },
    })
    expect(defaultFieldType('record')).toEqual({ kind: 'record', fields: [] })
  })

  it('flattenFieldPaths emits dotted paths for nested records and arrays', () => {
    const rt: RecordType = {
      id: 'r1',
      name: 'Order',
      source: { kind: 'manual' },
      fields: [
        { name: 'id', type: { kind: 'primitive', primitive: 'string' } },
        {
          name: 'customer',
          type: {
            kind: 'record',
            fields: [
              { name: 'name', type: { kind: 'primitive', primitive: 'string' } },
            ],
          },
        },
        {
          name: 'tags',
          type: {
            kind: 'array',
            items: { kind: 'primitive', primitive: 'string' },
          },
        },
      ],
    }
    expect(flattenFieldPaths(rt)).toEqual([
      'id',
      'customer',
      'customer.name',
      'tags',
      'tags[]',
    ])
  })

  it('flattenFieldPaths resolves refs only when a resolver is supplied', () => {
    const address: RecordType = {
      id: 'addr',
      name: 'Address',
      source: { kind: 'manual' },
      fields: [{ name: 'zip', type: { kind: 'primitive', primitive: 'string' } }],
    }
    const person: RecordType = {
      id: 'person',
      name: 'Person',
      source: { kind: 'manual' },
      fields: [{ name: 'home', type: { kind: 'ref', recordTypeId: 'addr' } }],
    }
    expect(flattenFieldPaths(person)).toEqual(['home'])
    expect(
      flattenFieldPaths(person, (id) => (id === 'addr' ? address : undefined)),
    ).toEqual(['home', 'home.zip'])
  })
})
