import { describe, it, expect } from 'vitest'
import {
  describeExpression,
  describePredicate,
  emptyPredicate,
  isExpressionSet,
  isPredicate,
  isValueExpression,
  newCondition,
  newFieldExpression,
  newMappingEntry,
} from './types'

describe('predicate helpers', () => {
  it('describes an empty predicate as "all records"', () => {
    expect(describePredicate(emptyPredicate())).toBe('all records')
  })

  it('describes conditions joined by the combinator', () => {
    const predicate = {
      combinator: 'AND' as const,
      conditions: [
        { id: '1', field: 'amount', operator: 'gt' as const, value: '100' },
        { id: '2', field: 'status', operator: 'eq' as const, value: 'NEW' },
      ],
    }
    expect(describePredicate(predicate)).toBe('amount > 100 AND status == NEW')
  })

  it('omits the value for valueless operators', () => {
    const predicate = {
      combinator: 'OR' as const,
      conditions: [
        { id: '1', field: 'note', operator: 'isNull' as const, value: '' },
      ],
    }
    expect(describePredicate(predicate)).toBe('note is null')
  })

  it('isPredicate distinguishes structured predicates from legacy strings', () => {
    expect(isPredicate(emptyPredicate())).toBe(true)
    expect(isPredicate('amount > 100')).toBe(false)
    expect(isPredicate(undefined)).toBe(false)
  })

  it('newCondition has a unique id and sane defaults', () => {
    const a = newCondition()
    const b = newCondition()
    expect(a.id).not.toBe(b.id)
    expect(a.operator).toBe('eq')
  })
})

describe('value expression helpers', () => {
  it('newFieldExpression / newMappingEntry produce sane defaults', () => {
    expect(newFieldExpression()).toEqual({ kind: 'field', path: '' })
    const a = newMappingEntry()
    const b = newMappingEntry()
    expect(a.id).not.toBe(b.id)
    expect(a.outputField).toBe('')
    expect(a.expression).toEqual({ kind: 'field', path: '' })
  })

  it('isValueExpression recognizes field and literal expressions', () => {
    expect(isValueExpression({ kind: 'field', path: 'x' })).toBe(true)
    expect(isValueExpression({ kind: 'literal', value: 'y' })).toBe(true)
    expect(isValueExpression('x')).toBe(false)
    expect(isValueExpression(undefined)).toBe(false)
  })

  it('isExpressionSet detects whether an expression carries a value', () => {
    expect(isExpressionSet({ kind: 'field', path: '' })).toBe(false)
    expect(isExpressionSet({ kind: 'field', path: 'customer.id' })).toBe(true)
    expect(isExpressionSet({ kind: 'literal', value: '' })).toBe(false)
    expect(isExpressionSet({ kind: 'literal', value: 'EUR' })).toBe(true)
  })

  it('describeExpression renders fields and literals', () => {
    expect(describeExpression({ kind: 'field', path: 'amount' })).toBe('amount')
    expect(describeExpression({ kind: 'literal', value: 'EUR' })).toBe('"EUR"')
    expect(describeExpression({ kind: 'field', path: '' })).toBe('(field)')
  })
})
