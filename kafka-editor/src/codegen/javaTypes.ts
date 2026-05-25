import type { FieldType, RecordType } from '../model/recordTypes'

/** Java type helpers shared by the project and output-type generators. */

/** Sanitize a name into a Java class identifier (PascalCase). */
export function javaClassName(name: string): string {
  const cleaned = name
    .replace(/[^a-zA-Z0-9]+/g, ' ')
    .trim()
    .split(' ')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join('')
  return cleaned || 'GeneratedType'
}

/** Box a Java primitive for use as a generic type argument. */
export function boxed(javaType: string): string {
  switch (javaType) {
    case 'int':
      return 'Integer'
    case 'long':
      return 'Long'
    case 'float':
      return 'Float'
    case 'double':
      return 'Double'
    case 'boolean':
      return 'Boolean'
    default:
      return javaType
  }
}

/** Map a record-type field type to a Java type. */
export function fieldTypeToJava(
  type: FieldType,
  recordTypes: RecordType[],
): string {
  switch (type.kind) {
    case 'primitive':
      switch (type.primitive) {
        case 'string':
          return 'String'
        case 'boolean':
          return 'boolean'
        case 'int':
          return 'int'
        case 'long':
          return 'long'
        case 'float':
          return 'float'
        case 'double':
          return 'double'
        case 'bytes':
          return 'byte[]'
        case 'date':
          return 'java.time.LocalDate'
        case 'timestamp':
          return 'java.time.Instant'
        case 'uuid':
          return 'java.util.UUID'
      }
      return 'Object'
    case 'array':
      return `java.util.List<${boxed(fieldTypeToJava(type.items, recordTypes))}>`
    case 'enum':
      return 'String'
    case 'ref': {
      const ref = recordTypes.find((rt) => rt.id === type.recordTypeId)
      return ref ? javaClassName(ref.name) : 'Object'
    }
    case 'record':
      // Inline nested records get their own generated class (see javaRecords).
      return 'Object'
    case 'unknown':
      return 'Object'
  }
}

/** Best-effort Java type for a literal string value. */
export function literalJavaType(raw: string): string {
  if (raw === 'true' || raw === 'false') return 'boolean'
  if (raw.trim() !== '' && !Number.isNaN(Number(raw))) return 'double'
  return 'String'
}
