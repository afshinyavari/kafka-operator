import { describe, it, expect } from 'vitest'
import {
  expressionToJava,
  fieldAccess,
  javaLiteral,
  predicateToJava,
} from './javaExpressions'

describe('javaExpressions', () => {
  it('fieldAccess builds a record-accessor chain', () => {
    expect(fieldAccess('amount')).toBe('value.amount()')
    expect(fieldAccess('customer.id')).toBe('value.customer().id()')
    expect(fieldAccess('')).toBe('value')
  })

  it('javaLiteral quotes strings but leaves numbers and booleans bare', () => {
    expect(javaLiteral('100')).toBe('100')
    expect(javaLiteral('true')).toBe('true')
    expect(javaLiteral('EUR')).toBe('"EUR"')
  })

  it('predicateToJava is "true" for an empty predicate', () => {
    expect(predicateToJava({ combinator: 'AND', conditions: [] })).toBe('true')
  })

  it('predicateToJava compiles conditions joined by the combinator', () => {
    const java = predicateToJava({
      combinator: 'AND',
      conditions: [
        { id: '1', field: 'amount', operator: 'gt', value: '100' },
        { id: '2', field: 'status', operator: 'eq', value: 'NEW' },
      ],
    })
    expect(java).toBe(
      '(value.amount() > 100) && (java.util.Objects.equals(value.status(), "NEW"))',
    )
  })

  it('predicateToJava handles a valueless operator', () => {
    expect(
      predicateToJava({
        combinator: 'OR',
        conditions: [
          { id: '1', field: 'note', operator: 'isNull', value: '' },
        ],
      }),
    ).toBe('value.note() == null')
  })

  it('expressionToJava compiles field and literal expressions', () => {
    expect(expressionToJava({ kind: 'field', path: 'amount' })).toBe(
      'value.amount()',
    )
    expect(expressionToJava({ kind: 'literal', value: 'EUR' })).toBe('"EUR"')
  })
})
