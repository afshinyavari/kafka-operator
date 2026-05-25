import type { FieldType, PrimitiveType, RecordField } from '../recordTypes'
import type { ParsedSchema } from './index'

interface Resolved {
  type: FieldType
  nullable: boolean
}

/** Parse a JSON Schema document (already JSON-parsed) into the field model. */
export function parseJsonSchema(json: unknown): ParsedSchema {
  const warnings: string[] = []
  if (!isObject(json)) {
    return { name: 'Schema', fields: [], warnings: ['Schema is not an object.'] }
  }
  const name = typeof json.title === 'string' ? json.title : 'Record'
  return { name, fields: parseObjectFields(json, warnings), warnings }
}

function parseObjectFields(
  schema: Record<string, unknown>,
  warnings: string[],
): RecordField[] {
  const props = schema.properties
  if (!isObject(props)) return []
  const required = Array.isArray(schema.required) ? schema.required : []
  const fields: RecordField[] = []
  for (const [key, propSchema] of Object.entries(props)) {
    const resolved = resolveJsonType(propSchema, warnings, key)
    const optional = !required.includes(key) || resolved.nullable
    fields.push({
      name: key,
      type: resolved.type,
      nullable: optional || undefined,
      doc:
        isObject(propSchema) && typeof propSchema.description === 'string'
          ? propSchema.description
          : undefined,
    })
  }
  return fields
}

function resolveJsonType(
  s: unknown,
  warnings: string[],
  fieldName: string,
): Resolved {
  if (!isObject(s)) {
    return { type: { kind: 'unknown' }, nullable: false }
  }
  if (s.$ref || s.oneOf || s.anyOf || s.allOf) {
    warnings.push(
      `Field "${fieldName}": $ref / oneOf / anyOf is not supported.`,
    )
    return { type: { kind: 'unknown' }, nullable: false }
  }
  if (Array.isArray(s.enum)) {
    return { type: { kind: 'enum', symbols: s.enum.map(String) }, nullable: false }
  }

  // `type` may be a string or an array including "null".
  let typeName: unknown = s.type
  let nullable = false
  if (Array.isArray(typeName)) {
    nullable = typeName.includes('null')
    typeName = typeName.find((t) => t !== 'null')
  }

  switch (typeName) {
    case 'object':
      return {
        type: { kind: 'record', fields: parseObjectFields(s, warnings) },
        nullable,
      }
    case 'array':
      return {
        type: {
          kind: 'array',
          items: resolveJsonType(s.items, warnings, fieldName).type,
        },
        nullable,
      }
    case 'string':
      return {
        type: { kind: 'primitive', primitive: jsonStringFormat(s.format) },
        nullable,
      }
    case 'integer':
      return { type: { kind: 'primitive', primitive: 'long' }, nullable }
    case 'number':
      return { type: { kind: 'primitive', primitive: 'double' }, nullable }
    case 'boolean':
      return { type: { kind: 'primitive', primitive: 'boolean' }, nullable }
    default:
      warnings.push(`Field "${fieldName}": unrecognized JSON Schema type.`)
      return { type: { kind: 'unknown' }, nullable }
  }
}

function jsonStringFormat(format: unknown): PrimitiveType {
  if (format === 'date-time') return 'timestamp'
  if (format === 'date') return 'date'
  if (format === 'uuid') return 'uuid'
  return 'string'
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}
