import { describe, it, expect } from 'vitest'
import {
  describeAggregation,
  newAggregateField,
  opReadsField,
} from './aggregations'

describe('aggregation helpers', () => {
  it('newAggregateField defaults to a count with a unique id', () => {
    const a = newAggregateField()
    const b = newAggregateField()
    expect(a.id).not.toBe(b.id)
    expect(a.op).toBe('count')
    expect(a.name).toBe('')
  })

  it('opReadsField is true for field ops and false for count', () => {
    expect(opReadsField('count')).toBe(false)
    expect(opReadsField('sum')).toBe(true)
    expect(opReadsField('max')).toBe(true)
  })

  it('describeAggregation lists the accumulator field names', () => {
    expect(describeAggregation([])).toBe('no accumulator fields')
    expect(
      describeAggregation([
        { id: '1', name: 'total', op: 'sum', sourceField: 'amount' },
        { id: '2', name: 'n', op: 'count', sourceField: '' },
      ]),
    ).toBe('total, n')
  })
})
