import { nanoid } from 'nanoid'

/**
 * Structured aggregation model for the `aggregate` node. An aggregation is a
 * list of accumulator fields, each computed by an operation over the grouped
 * input. The initializer and the accumulator type are implied by the ops, so
 * the user never writes a raw fold.
 */

export type AggregateOp = 'count' | 'sum' | 'min' | 'max' | 'first' | 'last'

export const AGGREGATE_OPS: { value: AggregateOp; label: string }[] = [
  { value: 'count', label: 'count' },
  { value: 'sum', label: 'sum of' },
  { value: 'min', label: 'min of' },
  { value: 'max', label: 'max of' },
  { value: 'first', label: 'first' },
  { value: 'last', label: 'last' },
]

/** Operations that read an input field; 'count' does not. */
export const FIELD_OPS: AggregateOp[] = ['sum', 'min', 'max', 'first', 'last']

export interface AggregateField {
  id: string
  /** Name of the accumulator field this produces. */
  name: string
  op: AggregateOp
  /** Input value field the op reads (ignored for 'count'). */
  sourceField: string
}

export function newAggregateField(): AggregateField {
  return { id: nanoid(), name: '', op: 'count', sourceField: '' }
}

/** Whether an op reads a source field. */
export function opReadsField(op: AggregateOp): boolean {
  return FIELD_OPS.includes(op)
}

/** A short human-readable rendering of an aggregation, for node summaries. */
export function describeAggregation(fields: AggregateField[]): string {
  if (fields.length === 0) return 'no accumulator fields'
  return fields.map((f) => f.name || '?').join(', ')
}
