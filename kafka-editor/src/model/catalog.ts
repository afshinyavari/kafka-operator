import { nanoid } from 'nanoid'

/**
 * The catalog — Kafka topics defined once and referenced by id, so a topic
 * name, its serialization format, and its value type aren't duplicated (and
 * drifting) across nodes. A topic is the single source of truth for its value
 * record type; source/sink nodes inherit it, with an optional per-node
 * override (see {@link effectiveValueRecordTypeId}).
 */

/** Serialization format for a topic's key or value. */
export type SerdeKind =
  | 'string'
  | 'long'
  | 'integer'
  | 'double'
  | 'bytes'
  | 'json'
  | 'avro'

export const SERDE_KINDS: SerdeKind[] = [
  'string',
  'long',
  'integer',
  'double',
  'bytes',
  'json',
  'avro',
]

export interface TopicDef {
  id: string
  name: string
  partitions?: number
  /** Key serialization format (default 'string'). */
  keyFormat?: SerdeKind
  /** Value serialization format (default 'json'). */
  valueFormat?: SerdeKind
  /** The record type describing the value — for json / avro formats. */
  valueRecordTypeId?: string
}

export interface Catalog {
  topics: TopicDef[]
}

export function emptyCatalog(): Catalog {
  return { topics: [] }
}

export function newTopic(name = 'new-topic'): TopicDef {
  return { id: nanoid(), name, keyFormat: 'string', valueFormat: 'json' }
}

/** Whether a format carries a structured payload (and may link a record type). */
export function formatCarriesRecordType(format: SerdeKind | undefined): boolean {
  return format === 'json' || format === 'avro'
}

/**
 * A source/sink node's effective value record type: its own per-node override
 * if set, otherwise the value type of the catalog topic it references.
 */
export function effectiveValueRecordTypeId(
  config: Record<string, unknown>,
  catalog: Catalog,
): string | undefined {
  const override = config.valueRecordTypeId
  if (typeof override === 'string' && override.length > 0) {
    return override
  }
  const topicId = config.topicId
  if (typeof topicId !== 'string' || topicId.length === 0) {
    return undefined
  }
  return catalog.topics.find((t) => t.id === topicId)?.valueRecordTypeId
}
