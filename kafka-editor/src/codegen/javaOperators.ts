import type { ValueExpression, ValueMappingEntry } from '../expressions/types'
import type { AggregateField } from '../model/aggregations'
import type { OutputType } from './outputTypes'
import {
  expressionToJava,
  fieldAccess,
  joinExpressionToJava,
} from './javaExpressions'

/**
 * Multi-line operator-body generators. Value-reshaping operators construct a
 * named output record when one is known, falling back to a HashMap otherwise.
 */

const I = '        ' // statement, in the method body
const B = '            ' // lambda body, one level in
const B2 = '                ' // block inside a lambda

function named(entries: ValueMappingEntry[]): ValueMappingEntry[] {
  return entries.filter((e) => e.outputField)
}

/** Build the `HashMap`-construction lines for a value mapping (the fallback). */
function hashMapBody(
  entries: ValueMappingEntry[],
  compile: (e: ValueExpression) => string,
): string[] {
  const lines = [`${B}var out = new java.util.HashMap<String, Object>();`]
  for (const entry of entries) {
    lines.push(`${B}out.put("${entry.outputField}", ${compile(entry.expression)});`)
  }
  return lines
}

/** `mapValues` — construct the named output record (identity when empty). */
export function mapValuesBlock(
  varName: string,
  input: string,
  entries: ValueMappingEntry[],
  outputType?: OutputType,
): string {
  const fields = named(entries)
  if (fields.length === 0) {
    return `${I}var ${varName} = ${input}.mapValues(value -> value);`
  }
  if (outputType) {
    const args = fields.map((e) => expressionToJava(e.expression)).join(', ')
    return `${I}var ${varName} = ${input}.mapValues(value -> new ${outputType.className}(${args}));`
  }
  return [
    `${I}var ${varName} = ${input}.mapValues(value -> {`,
    ...hashMapBody(fields, expressionToJava),
    `${B}return out;`,
    `${I}});`,
  ].join('\n')
}

/** `map` — rebuild key and value. */
export function mapBlock(
  varName: string,
  input: string,
  keyExpr: string,
  entries: ValueMappingEntry[],
  outputType?: OutputType,
): string {
  const fields = named(entries)
  if (outputType && fields.length > 0) {
    const args = fields.map((e) => expressionToJava(e.expression)).join(', ')
    return [
      `${I}var ${varName} = ${input}.map((key, value) ->`,
      `${B}org.apache.kafka.streams.KeyValue.pair(${keyExpr}, new ${outputType.className}(${args})));`,
    ].join('\n')
  }
  const body =
    fields.length === 0
      ? [`${B}var out = value;`]
      : hashMapBody(fields, expressionToJava)
  return [
    `${I}var ${varName} = ${input}.map((key, value) -> {`,
    ...body,
    `${B}return org.apache.kafka.streams.KeyValue.pair(${keyExpr}, out);`,
    `${I}});`,
  ].join('\n')
}

/** A join with a structured value joiner; `windowArg` is the JoinWindows arg. */
export function joinBlock(
  varName: string,
  leftVar: string,
  rightVar: string,
  windowArg: string | null,
  entries: ValueMappingEntry[],
  outputType?: OutputType,
): string {
  const fields = named(entries)
  if (outputType && fields.length > 0) {
    const args = fields.map((e) => joinExpressionToJava(e.expression)).join(', ')
    const win = windowArg ? `, ${windowArg}` : ''
    return `${I}var ${varName} = ${leftVar}.join(${rightVar}, (left, right) -> new ${outputType.className}(${args})${win});`
  }
  const joiner =
    fields.length === 0
      ? [`${B}return left;`]
      : [...hashMapBody(fields, joinExpressionToJava), `${B}return out;`]
  const close = windowArg ? `},\n${B}${windowArg})` : `})`
  return [
    `${I}var ${varName} = ${leftVar}.join(${rightVar}, (left, right) -> {`,
    ...joiner,
    `${I}${close};`,
  ].join('\n')
}

/** Initial accumulator value per aggregate op. */
const AGG_INIT: Record<string, string> = {
  count: '0L',
  sum: '0.0',
  min: 'null',
  max: 'null',
  first: 'null',
  last: 'null',
}

function aggregateUpdate(field: AggregateField): string {
  const key = JSON.stringify(field.name)
  const src = fieldAccess(field.sourceField)
  switch (field.op) {
    case 'count':
      return `aggregate.put(${key}, ((Number) aggregate.get(${key})).longValue() + 1L);`
    case 'sum':
      return `aggregate.put(${key}, ((Number) aggregate.get(${key})).doubleValue() + ((Number) ${src}).doubleValue());`
    case 'min':
      return `aggregate.merge(${key}, ${src}, (a, b) -> ((Comparable<Object>) a).compareTo(b) <= 0 ? a : b);`
    case 'max':
      return `aggregate.merge(${key}, ${src}, (a, b) -> ((Comparable<Object>) a).compareTo(b) >= 0 ? a : b);`
    case 'first':
      return `aggregate.putIfAbsent(${key}, ${src});`
    case 'last':
      return `aggregate.put(${key}, ${src});`
    default:
      return `// TODO: unsupported op ${field.op}`
  }
}

/** `aggregate` — an initializer + aggregator over a HashMap accumulator. */
export function aggregateBlock(
  varName: string,
  input: string,
  fields: AggregateField[],
): string {
  const named = fields.filter((f) => f.name)
  return [
    `${I}var ${varName} = ${input}.aggregate(`,
    `${B}() -> {`,
    `${B2}var acc = new java.util.HashMap<String, Object>();`,
    ...named.map(
      (f) =>
        `${B2}acc.put(${JSON.stringify(f.name)}, ${AGG_INIT[f.op] ?? 'null'});`,
    ),
    `${B2}return acc;`,
    `${B}},`,
    `${B}(key, value, aggregate) -> {`,
    ...named.map((f) => `${B2}${aggregateUpdate(f)}`),
    `${B2}return aggregate;`,
    `${B}});`,
  ].join('\n')
}

/** `reduce` — a combine expression for the chosen strategy. */
export function reduceExpression(strategy: unknown): string {
  switch (strategy) {
    case 'earliest':
      return 'value1'
    case 'latest':
      return 'value2'
    case 'sum':
      return '((Number) value1).doubleValue() + ((Number) value2).doubleValue()'
    case 'min':
      return '((Comparable<Object>) value1).compareTo(value2) <= 0 ? value1 : value2'
    case 'max':
      return '((Comparable<Object>) value1).compareTo(value2) >= 0 ? value1 : value2'
    default:
      return 'value2'
  }
}
