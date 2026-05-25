import { describe, it, expect } from 'vitest'
import { createEmptyProject, CURRENT_SCHEMA_VERSION } from './project'
import { fromDocument, toDocument } from './serialize'
import type { ProjectExtras } from './serialize'
import type { KafkaEdge, KafkaNode } from './graph'

const extras: ProjectExtras = {
  recordTypes: [
    {
      id: 'rt1',
      name: 'Order',
      source: { kind: 'manual' },
      fields: [{ name: 'id', type: { kind: 'primitive', primitive: 'string' } }],
    },
  ],
  catalog: {
    topics: [
      { id: 't1', name: 'orders', valueFormat: 'avro', valueRecordTypeId: 'rt1' },
    ],
  },
  environment: {
    bootstrapServers: 'localhost:9092',
    schemaRegistryUrl: 'http://localhost:8085',
  },
}

describe('serialize', () => {
  it('round-trips nodes, edges, record types and the catalog', () => {
    const base = createEmptyProject()
    const nodes: KafkaNode[] = [
      {
        id: 'n1',
        type: 'source',
        position: { x: 10, y: 20 },
        data: { config: { label: 'Orders' } },
      },
    ]
    const edges: KafkaEdge[] = []
    const viewport = { x: 5, y: 6, zoom: 1.5 }

    const doc = toDocument(base, nodes, edges, viewport, extras)
    expect(doc.schemaVersion).toBe(CURRENT_SCHEMA_VERSION)
    expect(doc.recordTypes).toEqual(extras.recordTypes)
    expect(doc.catalog).toEqual(extras.catalog)

    const restored = fromDocument(doc)
    expect(restored.nodes).toHaveLength(1)
    expect(restored.recordTypes).toEqual(extras.recordTypes)
    expect(restored.catalog).toEqual(extras.catalog)
    expect(restored.environment).toEqual(extras.environment)
  })

  it('is stable across a second round-trip', () => {
    const base = createEmptyProject()
    const viewport = { x: 0, y: 0, zoom: 1 }
    const doc = toDocument(base, [], [], viewport, extras)
    const restored = fromDocument(doc)
    const doc2 = toDocument(base, restored.nodes, restored.edges, viewport, {
      recordTypes: restored.recordTypes,
      catalog: restored.catalog,
      environment: restored.environment,
    })
    expect(doc2.catalog).toEqual(doc.catalog)
    expect(doc2.recordTypes).toEqual(doc.recordTypes)
  })

  it('survives JSON serialization', () => {
    const base = createEmptyProject()
    const doc = toDocument(base, [], [], { x: 0, y: 0, zoom: 1 }, extras)
    expect(JSON.parse(JSON.stringify(doc))).toEqual(doc)
  })
})
