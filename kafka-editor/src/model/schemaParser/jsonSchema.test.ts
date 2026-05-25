import { describe, it, expect } from 'vitest'
import { parseJsonSchema } from './jsonSchema'

const invoiceJson = {
  $schema: 'http://json-schema.org/draft-07/schema#',
  title: 'Invoice',
  type: 'object',
  properties: {
    invoiceId: { type: 'string' },
    amount: { type: 'number' },
    paid: { type: 'boolean' },
    dueDate: { type: 'string', format: 'date' },
    lines: { type: 'array', items: { type: 'string' } },
    customer: {
      type: 'object',
      properties: { id: { type: 'string' } },
    },
  },
  required: ['invoiceId', 'amount'],
}

describe('parseJsonSchema', () => {
  it('parses the title and property fields', () => {
    const result = parseJsonSchema(invoiceJson)
    expect(result.name).toBe('Invoice')
    expect(result.fields.map((f) => f.name)).toEqual([
      'invoiceId',
      'amount',
      'paid',
      'dueDate',
      'lines',
      'customer',
    ])
  })

  it('marks non-required properties as nullable', () => {
    const fields = parseJsonSchema(invoiceJson).fields
    expect(fields.find((f) => f.name === 'invoiceId')?.nullable).toBeUndefined()
    expect(fields.find((f) => f.name === 'paid')?.nullable).toBe(true)
  })

  it('maps primitive types and string formats', () => {
    const fields = parseJsonSchema(invoiceJson).fields
    expect(fields.find((f) => f.name === 'amount')?.type).toEqual({
      kind: 'primitive',
      primitive: 'double',
    })
    expect(fields.find((f) => f.name === 'dueDate')?.type).toEqual({
      kind: 'primitive',
      primitive: 'date',
    })
  })

  it('parses arrays and nested objects', () => {
    const fields = parseJsonSchema(invoiceJson).fields
    expect(fields.find((f) => f.name === 'lines')?.type).toEqual({
      kind: 'array',
      items: { kind: 'primitive', primitive: 'string' },
    })
    expect(fields.find((f) => f.name === 'customer')?.type.kind).toBe('record')
  })

  it('degrades $ref / oneOf to unknown with a warning', () => {
    const result = parseJsonSchema({
      type: 'object',
      properties: { x: { $ref: '#/definitions/Foo' } },
    })
    expect(result.fields[0].type).toEqual({ kind: 'unknown' })
    expect(result.warnings.length).toBeGreaterThan(0)
  })
})
