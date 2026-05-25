import { nanoid } from 'nanoid'
import { CURRENT_SCHEMA_VERSION } from './project'
import type { ProjectDocument } from './project'

/**
 * Forward migration chain for saved project documents. Each migration upgrades
 * a raw document from version N to N+1, so older saved files keep working.
 */

type RawDoc = Record<string, unknown>
type Migration = (doc: RawDoc) => RawDoc

/** Migrations keyed by the version they upgrade *from*. */
const migrations: Record<number, Migration> = {
  // v1 -> v2: record-type definitions were introduced.
  1: (doc) => ({ ...doc, schemaVersion: 2, recordTypes: [] }),

  // v2 -> v3: the topic/SerDe catalog was introduced. Free-text topic names
  // on source/sink nodes become catalog topics, referenced by `topicId`.
  2: (doc) => {
    const topics: { id: string; name: string }[] = []
    const byName = new Map<string, string>()
    const nodes = Array.isArray(doc.nodes) ? doc.nodes : []
    for (const node of nodes) {
      const config = node?.config
      if (config && typeof config.topic === 'string' && config.topic) {
        let id = byName.get(config.topic)
        if (id === undefined) {
          id = nanoid()
          byName.set(config.topic, id)
          topics.push({ id, name: config.topic })
        }
        config.topicId = id
        delete config.topic
      }
    }
    return { ...doc, schemaVersion: 3, catalog: { topics, serdes: [] } }
  },

  // v3 -> v4: the project environment (Kafka cluster + schema registry).
  3: (doc) => ({
    ...doc,
    schemaVersion: 4,
    environment: {
      bootstrapServers: 'localhost:9092',
      schemaRegistryUrl: 'http://localhost:8085',
    },
  }),

  // v4 -> v5: topics carry their key/value format and value record type
  // directly; the standalone SerDe entities are folded into the topics.
  4: (doc) => {
    const catalog =
      doc.catalog && typeof doc.catalog === 'object'
        ? (doc.catalog as Record<string, unknown>)
        : {}
    const serdes = Array.isArray(catalog.serdes) ? catalog.serdes : []
    const serdeById = new Map(serdes.map((s) => [s.id, s]))
    const rawTopics = Array.isArray(catalog.topics) ? catalog.topics : []
    const topics = rawTopics.map((topic) => {
      const valueSerde = serdeById.get(topic.valueSerdeId)
      const keySerde = serdeById.get(topic.keySerdeId)
      return {
        id: topic.id,
        name: topic.name,
        partitions: topic.partitions,
        keyFormat: keySerde?.kind ?? 'string',
        valueFormat: valueSerde?.kind ?? 'json',
        valueRecordTypeId: valueSerde?.recordTypeId,
      }
    })
    return { ...doc, schemaVersion: 5, catalog: { topics } }
  },
}

/**
 * Upgrade a raw, parsed document to the current schema version. Throws if a
 * required migration step is missing or the document is from the future.
 */
export function migrateProject(raw: RawDoc): ProjectDocument {
  let doc = raw
  let version = typeof raw.schemaVersion === 'number' ? raw.schemaVersion : 0

  while (version < CURRENT_SCHEMA_VERSION) {
    const migrate = migrations[version]
    if (!migrate) {
      throw new Error(`No migration registered from schema version ${version}`)
    }
    doc = migrate(doc)
    version += 1
  }

  if (version !== CURRENT_SCHEMA_VERSION) {
    throw new Error(
      `Document schema version ${version} is newer than supported version ${CURRENT_SCHEMA_VERSION}`,
    )
  }

  return doc as unknown as ProjectDocument
}
