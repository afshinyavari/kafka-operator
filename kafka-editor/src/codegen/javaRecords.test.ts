import { describe, it, expect } from 'vitest'
import { generateRecordClasses } from './javaRecords'
import type { RecordType } from '../model/recordTypes'

describe('generateRecordClasses', () => {
  it('generates a record with primitive fields', () => {
    const rt: RecordType = {
      id: 'r1',
      name: 'Order',
      source: { kind: 'manual' },
      fields: [
        { name: 'id', type: { kind: 'primitive', primitive: 'string' } },
        { name: 'amount', type: { kind: 'primitive', primitive: 'double' } },
      ],
    }
    const classes = generateRecordClasses(rt, [rt])
    expect(classes).toHaveLength(1)
    expect(classes[0].className).toBe('Order')
    expect(classes[0].content).toContain('public record Order(')
    expect(classes[0].content).toContain('String id')
    expect(classes[0].content).toContain('double amount')
  })

  it('generates a nested class for an inline record field', () => {
    const rt: RecordType = {
      id: 'r1',
      name: 'Order',
      source: { kind: 'manual' },
      fields: [
        {
          name: 'customer',
          type: {
            kind: 'record',
            fields: [
              { name: 'id', type: { kind: 'primitive', primitive: 'string' } },
            ],
          },
        },
      ],
    }
    const classes = generateRecordClasses(rt, [rt])
    const names = classes.map((c) => c.className)
    expect(names).toContain('Order')
    expect(names).toContain('OrderCustomer')
    // The parent references the generated nested class.
    const order = classes.find((c) => c.className === 'Order')
    expect(order?.content).toContain('OrderCustomer customer')
  })
})
