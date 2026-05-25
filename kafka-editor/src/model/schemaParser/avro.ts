import type { FieldType, PrimitiveType, RecordField } from '../recordTypes'
import type { ParsedSchema } from './index'

/** Avro primitive type names → the editor's primitive types. */
const AVRO_PRIMITIVES: Record<string, PrimitiveType> = {
  string: 'string',
  boolean: 'boolean',
  int: 'int',
  long: 'long',
  float: 'float',
  double: 'double',
  bytes: 'bytes',
}

interface Resolved {
  type: FieldType
  nullable: boolean
}

/** Parse an Avro schema (already JSON-parsed) into the record-field model. */
export function parseAvroSchema(json: unknown): ParsedSchema {
  const warnings: string[] = []
  if (!isObject(json) || json.type !== 'record') {
    return {
      name: 'Schema',
      fields: [],
      warnings: ['Top-level Avro schema is not a record.'],
    }
  }
  const name = typeof json.name === 'string' ? json.name : 'Record'
  return { name, fields: parseRecordFields(json.fields, warnings), warnings }
}

function parseRecordFields(raw: unknown, warnings: string[]): RecordField[] {
  if (!Array.isArray(raw)) return []
  const fields: RecordField[] = []
  for (const f of raw) {
    if (!isObject(f) || typeof f.name !== 'string') continue
    const resolved = resolveAvroType(f.type, warnings, f.name)
    fields.push({
      name: f.name,
      type: resolved.type,
      nullable: resolved.nullable || undefined,
      doc: typeof f.doc === 'string' ? f.doc : undefined,
    })
  }
  return fields
}

function resolveAvroType(
  t: unknown,
  warnings: string[],
  fieldName: string,
): Resolved {
  // Union, e.g. ["null", "string"].
  if (Array.isArray(t)) {
    const nonNull = t.filter((m) => m !== 'null')
    const nullable = nonNull.length !== t.length
    if (nonNull.length === 1) {
      return {
        type: resolveAvroType(nonNull[0], warnings, fieldName).type,
        nullable,
      }
    }
    warnings.push(`Field "${fieldName}": multi-type union is not supported.`)
    return { type: { kind: 'unknown' }, nullable }
  }

  // A bare string — a primitive, or a named-type reference.
  if (typeof t === 'string') {
    const prim = AVRO_PRIMITIVES[t]
    if (prim) return { type: { kind: 'primitive', primitive: prim }, nullable: false }
    warnings.push(`Field "${fieldName}": named type "${t}" is not resolved.`)
    return { type: { kind: 'unknown' }, nullable: false }
  }

  // A complex object type.
  if (isObject(t)) {
    if (typeof t.logicalType === 'string') {
      return {
        type: {
          kind: 'primitive',
          primitive: logicalToPrimitive(t.logicalType, t.type),
        },
        nullable: false,
      }
    }
    const kind = t.type
    if (kind === 'record') {
      return {
        type: { kind: 'record', fields: parseRecordFields(t.fields, warnings) },
        nullable: false,
      }
    }
    if (kind === 'array') {
      return {
        type: {
          kind: 'array',
          items: resolveAvroType(t.items, warnings, fieldName).type,
        },
        nullable: false,
      }
    }
    if (kind === 'enum' && Array.isArray(t.symbols)) {
      return {
        type: { kind: 'enum', symbols: t.symbols.map(String) },
        nullable: false,
      }
    }
    if (kind === 'fixed') {
      return { type: { kind: 'primitive', primitive: 'bytes' }, nullable: false }
    }
    if (kind === 'map') {
      warnings.push(`Field "${fieldName}": Avro maps are not supported.`)
      return { type: { kind: 'unknown' }, nullable: false }
    }
    if (typeof kind === 'string' && AVRO_PRIMITIVES[kind]) {
      return {
        type: { kind: 'primitive', primitive: AVRO_PRIMITIVES[kind] },
        nullable: false,
      }
    }
  }

  warnings.push(`Field "${fieldName}": unrecognized Avro type.`)
  return { type: { kind: 'unknown' }, nullable: false }
}

function logicalToPrimitive(logical: string, underlying: unknown): PrimitiveType {
  if (logical === 'date') return 'date'
  if (logical.startsWith('timestamp')) return 'timestamp'
  if (logical === 'uuid') return 'uuid'
  if (typeof underlying === 'string' && AVRO_PRIMITIVES[underlying]) {
    return AVRO_PRIMITIVES[underlying]
  }
  return 'string'
}

function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}
