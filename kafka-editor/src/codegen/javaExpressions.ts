import { VALUELESS_OPERATORS } from '../expressions/types'
import type { Condition, Predicate, ValueExpression } from '../expressions/types'

/**
 * Compiles the editor's structured logic (predicates, value expressions) to
 * Java source fragments for the generated Quarkus topology.
 */

/** A dotted field path → a Java record-accessor chain (`value.customer().id()`). */
export function fieldAccess(path: string, root = 'value'): string {
  if (!path) return root
  return (
    root +
    path
      .split('.')
      .filter((p) => p.length > 0)
      .map((p) => `.${p}()`)
      .join('')
  )
}

/** A literal string → a Java literal (number / boolean bare, otherwise quoted). */
export function javaLiteral(raw: string): string {
  if (raw === 'true' || raw === 'false') return raw
  if (raw.trim() !== '' && !Number.isNaN(Number(raw))) return raw
  return JSON.stringify(raw)
}

function conditionToJava(condition: Condition): string {
  const field = fieldAccess(condition.field)
  const value = javaLiteral(condition.value)
  switch (condition.operator) {
    case 'eq':
      return `java.util.Objects.equals(${field}, ${value})`
    case 'neq':
      return `!java.util.Objects.equals(${field}, ${value})`
    case 'gt':
      return `${field} > ${value}`
    case 'gte':
      return `${field} >= ${value}`
    case 'lt':
      return `${field} < ${value}`
    case 'lte':
      return `${field} <= ${value}`
    case 'contains':
      return `String.valueOf(${field}).contains(${value})`
    case 'isNull':
      return `${field} == null`
    case 'isNotNull':
      return `${field} != null`
  }
}

/** Compile a predicate to a Java boolean expression. */
export function predicateToJava(predicate: Predicate): string {
  if (predicate.conditions.length === 0) return 'true'
  const joiner = predicate.combinator === 'AND' ? ' && ' : ' || '
  return predicate.conditions
    .map((c) => {
      const java = conditionToJava(c)
      // Parenthesize valued comparisons when combined, for clarity.
      return predicate.conditions.length > 1 &&
        !VALUELESS_OPERATORS.includes(c.operator)
        ? `(${java})`
        : java
    })
    .join(joiner)
}

/** Compile a value expression to a Java expression (rooted at `value`). */
export function expressionToJava(expression: ValueExpression): string {
  return expression.kind === 'field'
    ? fieldAccess(expression.path)
    : javaLiteral(expression.value)
}

/**
 * Compile a value expression in a join context, where field paths are prefixed
 * `left.` / `right.` to pick the joined side (e.g. `left.amount` → `left.amount()`).
 */
export function joinExpressionToJava(expression: ValueExpression): string {
  if (expression.kind === 'literal') return javaLiteral(expression.value)
  const segments = expression.path.split('.').filter((s) => s.length > 0)
  const onRight = segments[0] === 'right'
  const rooted = segments[0] === 'left' || segments[0] === 'right'
  return fieldAccess(
    (rooted ? segments.slice(1) : segments).join('.'),
    onRight ? 'right' : 'left',
  )
}
