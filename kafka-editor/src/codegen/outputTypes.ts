import type { ProjectDocument } from '../model/project'
import type { FieldType, RecordField, RecordType } from '../model/recordTypes'
import type { ValueExpression, ValueMappingEntry } from '../expressions/types'
import type { KafkaNode } from '../model/graph'
import { recordTypeIdForInput } from '../graph/recordTypeInference'
import { fieldTypeToJava, javaClassName, literalJavaType } from './javaTypes'

/**
 * Computes a named Java record type for each value-reshaping operator
 * (mapValues / map / join), so the generated topology flows typed values
 * instead of an untyped `Map<String, Object>`.
 */

export interface OutputField {
  name: string
  javaType: string
}

export interface OutputType {
  className: string
  fields: OutputField[]
}

const RESHAPERS = new Set([
  'map-values',
  'map',
  'stream-stream-join',
  'stream-table-join',
  'table-table-join',
])

/** Resolve a dotted field path within a record type to its field type. */
function resolveFieldType(
  recordType: RecordType,
  path: string,
  all: RecordType[],
): FieldType | undefined {
  const segments = path.split('.').filter((s) => s.length > 0)
  let fields: RecordField[] = recordType.fields
  let current: FieldType | undefined
  for (const segment of segments) {
    const field = fields.find((f) => f.name === segment)
    if (!field) return undefined
    current = field.type
    if (current.kind === 'record') {
      fields = current.fields
    } else if (current.kind === 'ref') {
      const refId = current.recordTypeId
      const ref = all.find((r) => r.id === refId)
      fields = ref ? ref.fields : []
    } else {
      fields = []
    }
  }
  return current
}

/** Compute the output record type for every reshaping node in the project. */
export function computeOutputTypes(
  doc: ProjectDocument,
): Map<string, OutputType> {
  const result = new Map<string, OutputType>()
  const usedNames = new Set<string>()

  // Adapt project nodes to the shape the inference pass expects.
  const kNodes = doc.nodes.map((n) => ({
    id: n.id,
    type: n.type,
    position: n.position,
    data: { config: n.config },
  })) as KafkaNode[]
  const kEdges = doc.edges as unknown as Parameters<
    typeof recordTypeIdForInput
  >[3]

  for (const node of doc.nodes) {
    if (!RESHAPERS.has(node.type)) continue
    const isJoin = node.type !== 'map-values' && node.type !== 'map'
    const configKey = isJoin ? 'valueJoiner' : 'valueMapping'
    const raw = node.config[configKey]
    const entries = (
      Array.isArray(raw) ? (raw as ValueMappingEntry[]) : []
    ).filter((e) => e.outputField)
    if (entries.length === 0) continue

    const leftId = recordTypeIdForInput(
      node.id,
      isJoin ? 'left' : 'in',
      kNodes,
      kEdges,
      doc.catalog,
    )
    const rightId = isJoin
      ? recordTypeIdForInput(node.id, 'right', kNodes, kEdges, doc.catalog)
      : undefined
    const leftRt = doc.recordTypes.find((rt) => rt.id === leftId)
    const rightRt = doc.recordTypes.find((rt) => rt.id === rightId)

    const javaTypeOf = (expr: ValueExpression): string => {
      if (expr.kind === 'literal') return literalJavaType(expr.value)
      if (isJoin) {
        const segments = expr.path.split('.')
        const onRight = segments[0] === 'right'
        const rooted = segments[0] === 'left' || segments[0] === 'right'
        const side = onRight ? rightRt : leftRt
        const rest = (rooted ? segments.slice(1) : segments).join('.')
        if (!side) return 'Object'
        const ft = resolveFieldType(side, rest, doc.recordTypes)
        return ft ? fieldTypeToJava(ft, doc.recordTypes) : 'Object'
      }
      if (!leftRt) return 'Object'
      const ft = resolveFieldType(leftRt, expr.path, doc.recordTypes)
      return ft ? fieldTypeToJava(ft, doc.recordTypes) : 'Object'
    }

    const fields: OutputField[] = entries.map((e) => ({
      name: e.outputField,
      javaType: javaTypeOf(e.expression),
    }))

    const label =
      typeof node.config.label === 'string' && node.config.label
        ? node.config.label
        : node.type
    const base = `${javaClassName(label)}Output`
    let className = base
    let suffix = 2
    while (usedNames.has(className)) className = `${base}${suffix++}`
    usedNames.add(className)

    result.set(node.id, { className, fields })
  }

  return result
}

/** Generate a Java record source file for an output type. */
export function generateOutputRecord(type: OutputType): string {
  const components = type.fields
    .map((f) => `        ${f.javaType} ${f.name}`)
    .join(',\n')
  const body = components ? `\n${components}\n` : ''
  return `package org.acme.kafka;

/** Generated value type produced by a topology operator. */
public record ${type.className}(${body}) {
}
`
}
