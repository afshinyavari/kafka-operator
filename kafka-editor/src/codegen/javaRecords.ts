import type { FieldType, RecordField, RecordType } from '../model/recordTypes'
import { boxed, fieldTypeToJava, javaClassName } from './javaTypes'

/**
 * Generates Java records for a record type, including a nested class for each
 * inline `record` field (named `<Parent><Field>`), so nested shapes are typed
 * rather than degraded to Object.
 */

export interface RecordClass {
  className: string
  content: string
}

function capitalize(s: string): string {
  return s.length > 0 ? s.charAt(0).toUpperCase() + s.slice(1) : s
}

/**
 * The Java type for a field type, emitting a class into `out` for each inline
 * nested record. `nestedName` is the class name to use should `type` be (or
 * contain) a record.
 */
function typeFor(
  nestedName: string,
  type: FieldType,
  recordTypes: RecordType[],
  out: RecordClass[],
): string {
  if (type.kind === 'record') {
    emitRecord(nestedName, type.fields, recordTypes, out)
    return nestedName
  }
  if (type.kind === 'array') {
    return `java.util.List<${boxed(
      typeFor(`${nestedName}Item`, type.items, recordTypes, out),
    )}>`
  }
  return fieldTypeToJava(type, recordTypes)
}

function emitRecord(
  className: string,
  fields: RecordField[],
  recordTypes: RecordType[],
  out: RecordClass[],
): void {
  if (out.some((r) => r.className === className)) return
  // Reserve the slot first so recursion is cycle-safe.
  const slot: RecordClass = { className, content: '' }
  out.push(slot)

  const components: string[] = []
  for (const field of fields) {
    if (!field.name) continue
    const javaType = typeFor(
      `${className}${capitalize(field.name)}`,
      field.type,
      recordTypes,
      out,
    )
    components.push(`        ${javaType} ${field.name}`)
  }
  const body =
    components.length > 0 ? `\n${components.join(',\n')}\n` : ''
  slot.content = `package org.acme.kafka;

/** Generated record type. */
public record ${className}(${body}) {
}
`
}

/** Generate the Java record(s) for a record type, nested records included. */
export function generateRecordClasses(
  recordType: RecordType,
  recordTypes: RecordType[],
): RecordClass[] {
  const out: RecordClass[] = []
  emitRecord(
    javaClassName(recordType.name),
    recordType.fields,
    recordTypes,
    out,
  )
  return out
}
