import { describe, it, expect } from 'vitest'
import {
  aggregateBlock,
  joinBlock,
  mapValuesBlock,
  reduceExpression,
} from './javaOperators'

describe('javaOperators', () => {
  it('mapValuesBlock builds a HashMap result', () => {
    const java = mapValuesBlock('mapped', 'source', [
      { id: '1', outputField: 'total', expression: { kind: 'field', path: 'amount' } },
      { id: '2', outputField: 'cur', expression: { kind: 'literal', value: 'EUR' } },
    ])
    expect(java).toContain('source.mapValues(value -> {')
    expect(java).toContain('out.put("total", value.amount());')
    expect(java).toContain('out.put("cur", "EUR");')
    expect(java).toContain('return out;')
  })

  it('mapValuesBlock is the identity when empty', () => {
    expect(mapValuesBlock('m', 's', [])).toBe(
      '        var m = s.mapValues(value -> value);',
    )
  })

  it('joinBlock references left/right and the window arg', () => {
    const java = joinBlock('joined', 'a', 'b', 'WINDOW', [
      { id: '1', outputField: 'x', expression: { kind: 'field', path: 'left.amount' } },
      { id: '2', outputField: 'y', expression: { kind: 'field', path: 'right.name' } },
    ])
    expect(java).toContain('a.join(b, (left, right) -> {')
    expect(java).toContain('out.put("x", left.amount());')
    expect(java).toContain('out.put("y", right.name());')
    expect(java).toContain('WINDOW)')
  })

  it('aggregateBlock emits an initializer and aggregator', () => {
    const java = aggregateBlock('agg', 'grouped', [
      { id: '1', name: 'n', op: 'count', sourceField: '' },
      { id: '2', name: 'total', op: 'sum', sourceField: 'amount' },
    ])
    expect(java).toContain('grouped.aggregate(')
    expect(java).toContain('acc.put("n", 0L);')
    expect(java).toContain('acc.put("total", 0.0);')
    expect(java).toContain('aggregate.put("n", ((Number) aggregate.get("n")).longValue() + 1L);')
    expect(java).toContain('((Number) value.amount()).doubleValue()')
  })

  it('reduceExpression maps each strategy', () => {
    expect(reduceExpression('earliest')).toBe('value1')
    expect(reduceExpression('latest')).toBe('value2')
    expect(reduceExpression('sum')).toContain('doubleValue()')
  })

  it('mapValuesBlock constructs a named record when given an output type', () => {
    const java = mapValuesBlock(
      'mapped',
      'source',
      [
        { id: '1', outputField: 'total', expression: { kind: 'field', path: 'amount' } },
        { id: '2', outputField: 'cur', expression: { kind: 'literal', value: 'EUR' } },
      ],
      {
        className: 'OrderTotal',
        fields: [
          { name: 'total', javaType: 'double' },
          { name: 'cur', javaType: 'String' },
        ],
      },
    )
    expect(java).toBe(
      '        var mapped = source.mapValues(value -> new OrderTotal(value.amount(), "EUR"));',
    )
  })
})
