import { describe, it, expect } from 'vitest'
import { parseAvroSchema } from './avro'

const orderAvro = {
  type: 'record',
  name: 'Order',
  fields: [
    { name: 'orderId', type: 'string' },
    { name: 'amount', type: 'double' },
    { name: 'note', type: ['null', 'string'] },
    {
      name: 'createdAt',
      type: { type: 'long', logicalType: 'timestamp-millis' },
    },
    {
      name: 'status',
      type: { type: 'enum', name: 'Status', symbols: ['NEW', 'PAID'] },
    },
    { name: 'tags', type: { type: 'array', items: 'string' } },
    {
      name: 'customer',
      type: {
        type: 'record',
        name: 'Customer',
        fields: [{ name: 'id', type: 'string' }],
      },
    },
    { name: 'meta', type: { type: 'map', values: 'string' } },
  ],
}

describe('parseAvroSchema', () => {
  it('parses the record name and primitive fields', () => {
    const result = parseAvroSchema(orderAvro)
    expect(result.name).toBe('Order')
    expect(result.fields.find((f) => f.name === 'orderId')?.type).toEqual({
      kind: 'primitive',
      primitive: 'string',
    })
    expect(result.fields.find((f) => f.name === 'amount')?.type).toEqual({
      kind: 'primitive',
      primitive: 'double',
    })
  })

  it('treats a ["null", X] union as a nullable field', () => {
    const note = parseAvroSchema(orderAvro).fields.find((f) => f.name === 'note')
    expect(note?.type).toEqual({ kind: 'primitive', primitive: 'string' })
    expect(note?.nullable).toBe(true)
  })

  it('maps logical types', () => {
    const createdAt = parseAvroSchema(orderAvro).fields.find(
      (f) => f.name === 'createdAt',
    )
    expect(createdAt?.type).toEqual({
      kind: 'primitive',
      primitive: 'timestamp',
    })
  })

  it('parses enums, arrays and nested records', () => {
    const fields = parseAvroSchema(orderAvro).fields
    expect(fields.find((f) => f.name === 'status')?.type).toEqual({
      kind: 'enum',
      symbols: ['NEW', 'PAID'],
    })
    expect(fields.find((f) => f.name === 'tags')?.type).toEqual({
      kind: 'array',
      items: { kind: 'primitive', primitive: 'string' },
    })
    expect(fields.find((f) => f.name === 'customer')?.type.kind).toBe('record')
  })

  it('degrades unsupported constructs (maps) to unknown with a warning', () => {
    const result = parseAvroSchema(orderAvro)
    expect(result.fields.find((f) => f.name === 'meta')?.type).toEqual({
      kind: 'unknown',
    })
    expect(result.warnings.length).toBeGreaterThan(0)
  })

  it('handles a non-record top-level schema gracefully', () => {
    const result = parseAvroSchema({ type: 'string' })
    expect(result.fields).toEqual([])
    expect(result.warnings.length).toBeGreaterThan(0)
  })
})
