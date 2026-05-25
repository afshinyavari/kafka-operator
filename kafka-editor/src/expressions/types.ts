import { nanoid } from 'nanoid'

/**
 * The structured expression model — boolean predicates (M3.1) and value
 * expressions / mappings (M3.4). These are stored in node config and, later,
 * compiled to Java by the Quarkus codegen.
 */

/* -------------------------------------------------------------------------- */
/* Predicates (filter / filterNot)                                            */
/* -------------------------------------------------------------------------- */

/** Comparison operators available in the predicate builder. */
export type ComparisonOperator =
  | 'eq'
  | 'neq'
  | 'gt'
  | 'gte'
  | 'lt'
  | 'lte'
  | 'contains'
  | 'isNull'
  | 'isNotNull'

/** A single field comparison. */
export interface Condition {
  id: string
  /** Field path on the record, e.g. 'amount' or 'customer.id'. */
  field: string
  operator: ComparisonOperator
  /** Comparison value; ignored for isNull / isNotNull. */
  value: string
}

export type Combinator = 'AND' | 'OR'

/** A boolean predicate — a flat list of conditions joined by one combinator. */
export interface Predicate {
  combinator: Combinator
  conditions: Condition[]
}

/** Operators that take no comparison value. */
export const VALUELESS_OPERATORS: ComparisonOperator[] = ['isNull', 'isNotNull']

/** Human-readable symbol for each operator. */
export const OPERATOR_LABEL: Record<ComparisonOperator, string> = {
  eq: '==',
  neq: '!=',
  gt: '>',
  gte: '>=',
  lt: '<',
  lte: '<=',
  contains: 'contains',
  isNull: 'is null',
  isNotNull: 'is not null',
}

export function emptyPredicate(): Predicate {
  return { combinator: 'AND', conditions: [] }
}

export function newCondition(): Condition {
  return { id: nanoid(), field: '', operator: 'eq', value: '' }
}

/** Runtime type guard — distinguishes a structured Predicate from legacy data. */
export function isPredicate(value: unknown): value is Predicate {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  return (
    (v.combinator === 'AND' || v.combinator === 'OR') &&
    Array.isArray(v.conditions)
  )
}

/** A short human-readable rendering of a predicate, for node summaries. */
export function describePredicate(predicate: Predicate): string {
  if (predicate.conditions.length === 0) return 'all records'
  return predicate.conditions
    .map((c) => {
      const field = c.field || 'field'
      if (VALUELESS_OPERATORS.includes(c.operator)) {
        return `${field} ${OPERATOR_LABEL[c.operator]}`
      }
      return `${field} ${OPERATOR_LABEL[c.operator]} ${c.value || '?'}`
    })
    .join(` ${predicate.combinator} `)
}

/* -------------------------------------------------------------------------- */
/* Value expressions (selectKey / map / mapValues)                            */
/* -------------------------------------------------------------------------- */

/** A value — either a reference to an input field, or a literal. */
export type ValueExpression =
  | { kind: 'field'; path: string }
  | { kind: 'literal'; value: string }

/** One output field of a value mapping: `outputField` ← `expression`. */
export interface ValueMappingEntry {
  id: string
  outputField: string
  expression: ValueExpression
}

export function newFieldExpression(): ValueExpression {
  return { kind: 'field', path: '' }
}

export function newMappingEntry(): ValueMappingEntry {
  return { id: nanoid(), outputField: '', expression: newFieldExpression() }
}

/** Runtime type guard for a `ValueExpression`. */
export function isValueExpression(value: unknown): value is ValueExpression {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  return v.kind === 'field' || v.kind === 'literal'
}

/** Whether an expression carries an actual value (a non-empty field / literal). */
export function isExpressionSet(expression: ValueExpression): boolean {
  return expression.kind === 'field'
    ? expression.path.length > 0
    : expression.value.length > 0
}

/** A short human-readable rendering of a value expression. */
export function describeExpression(expression: ValueExpression): string {
  return expression.kind === 'field'
    ? expression.path || '(field)'
    : `"${expression.value}"`
}
