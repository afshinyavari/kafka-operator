import { nanoid } from 'nanoid'

/**
 * The record-type model — named descriptions of Kafka record value shapes.
 * Record types are defined manually (M3.2) or imported from Apicurio Registry
 * (M3.3). This module is pure TS — consumed by the graph type-inference pass
 * and, later, by Quarkus codegen.
 */

/** Primitive scalar field types. */
export type PrimitiveType =
  | 'string'
  | 'boolean'
  | 'int'
  | 'long'
  | 'float'
  | 'double'
  | 'bytes'
  | 'date'
  | 'timestamp'
  | 'uuid'

export const PRIMITIVE_TYPES: PrimitiveType[] = [
  'string',
  'boolean',
  'int',
  'long',
  'float',
  'double',
  'bytes',
  'date',
  'timestamp',
  'uuid',
]

/** The type of a single field — a discriminated union. */
export type FieldType =
  | { kind: 'primitive'; primitive: PrimitiveType }
  | { kind: 'enum'; symbols: string[] }
  | { kind: 'array'; items: FieldType }
  | { kind: 'record'; fields: RecordField[] }
  | { kind: 'ref'; recordTypeId: string }
  | { kind: 'unknown' }

export type FieldKind = FieldType['kind']

export const FIELD_KINDS: FieldKind[] = [
  'primitive',
  'array',
  'record',
  'enum',
  'ref',
  'unknown',
]

export interface RecordField {
  name: string
  type: FieldType
  nullable?: boolean
  doc?: string
}

/** Schema formats supported by the Apicurio importer. */
export type SchemaFormat = 'AVRO' | 'JSON'

/** Provenance of a record type. */
export type RecordTypeSource =
  | { kind: 'manual' }
  | {
      kind: 'apicurio'
      groupId: string
      artifactId: string
      format: SchemaFormat
      /** ISO-8601 timestamp of the import. */
      importedAt: string
      /** The raw schema text as fetched — kept for refresh and codegen. */
      rawSchema: string
    }

export interface RecordType {
  id: string
  name: string
  fields: RecordField[]
  source: RecordTypeSource
}

export function newRecordType(name = 'NewRecord'): RecordType {
  return { id: nanoid(), name, fields: [], source: { kind: 'manual' } }
}

export function newField(name = ''): RecordField {
  return { name, type: { kind: 'primitive', primitive: 'string' } }
}

/** Default `FieldType` for a freshly-chosen field kind. */
export function defaultFieldType(kind: FieldKind): FieldType {
  switch (kind) {
    case 'primitive':
      return { kind: 'primitive', primitive: 'string' }
    case 'array':
      return { kind: 'array', items: { kind: 'primitive', primitive: 'string' } }
    case 'record':
      return { kind: 'record', fields: [] }
    case 'enum':
      return { kind: 'enum', symbols: [] }
    case 'ref':
      return { kind: 'ref', recordTypeId: '' }
    case 'unknown':
      return { kind: 'unknown' }
  }
}

/**
 * Flatten a record type into dotted field paths (`customer.id`, `items[]`),
 * for the predicate builder's field combobox. `resolveRef` resolves `ref`
 * fields to other record types; omit it to stop at refs.
 */
export function flattenFieldPaths(
  recordType: RecordType,
  resolveRef?: (id: string) => RecordType | undefined,
): string[] {
  const paths: string[] = []
  const visiting = new Set<string>()

  function walkFields(fields: RecordField[], prefix: string): void {
    for (const field of fields) {
      if (!field.name) continue
      walkType(field.type, prefix ? `${prefix}.${field.name}` : field.name)
    }
  }

  function walkType(type: FieldType, path: string): void {
    switch (type.kind) {
      case 'primitive':
      case 'enum':
      case 'unknown':
        paths.push(path)
        break
      case 'array':
        paths.push(path)
        walkType(type.items, `${path}[]`)
        break
      case 'record':
        paths.push(path)
        walkFields(type.fields, path)
        break
      case 'ref': {
        paths.push(path)
        const target = resolveRef?.(type.recordTypeId)
        if (target && !visiting.has(target.id)) {
          visiting.add(target.id)
          walkFields(target.fields, path)
          visiting.delete(target.id)
        }
        break
      }
    }
  }

  walkFields(recordType.fields, '')
  return paths
}
