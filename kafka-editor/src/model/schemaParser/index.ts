import type { RecordField, SchemaFormat } from '../recordTypes'
import { parseAvroSchema } from './avro'
import { parseJsonSchema } from './jsonSchema'

/**
 * Schema parsing — converts an Avro or JSON Schema document into the editor's
 * `RecordField` model. Unsupported constructs degrade to `{ kind: 'unknown' }`
 * with a recorded warning rather than throwing.
 */

export interface ParsedSchema {
  name: string
  fields: RecordField[]
  warnings: string[]
}

/** Parse raw schema text of the given format into a record-field model. */
export function parseSchema(
  format: SchemaFormat,
  rawText: string,
): ParsedSchema {
  let json: unknown
  try {
    json = JSON.parse(rawText)
  } catch {
    return {
      name: 'Schema',
      fields: [],
      warnings: ['Schema text is not valid JSON.'],
    }
  }
  return format === 'AVRO' ? parseAvroSchema(json) : parseJsonSchema(json)
}
