import { describe, it, expect } from 'vitest'
import { migrateProject } from './migrations'
import { CURRENT_SCHEMA_VERSION } from './project'

describe('migrateProject', () => {
  it('upgrades a v1 document through to the current version', () => {
    const v1 = {
      schemaVersion: 1,
      id: 'p1',
      meta: { name: 'Old', createdAt: 'x', updatedAt: 'x' },
      nodes: [],
      edges: [],
      viewport: { x: 0, y: 0, zoom: 1 },
    }
    const migrated = migrateProject(v1)
    expect(migrated.schemaVersion).toBe(CURRENT_SCHEMA_VERSION)
    expect(migrated.recordTypes).toEqual([])
    expect(migrated.catalog).toEqual({ topics: [] })
    expect(migrated.environment.bootstrapServers).toBe('localhost:9092')
    expect(migrated.id).toBe('p1')
  })

  it('v2 -> v3 converts free-text topic names into catalog topics', () => {
    const v2 = {
      schemaVersion: 2,
      id: 'p2',
      meta: { name: 'M', createdAt: 'x', updatedAt: 'x' },
      edges: [],
      viewport: { x: 0, y: 0, zoom: 1 },
      recordTypes: [],
      nodes: [
        { id: 'n1', type: 'source', position: { x: 0, y: 0 }, config: { topic: 'orders' } },
        { id: 'n2', type: 'sink', position: { x: 0, y: 0 }, config: { topic: 'orders' } },
        { id: 'n3', type: 'filter', position: { x: 0, y: 0 }, config: {} },
      ],
    }
    const migrated = migrateProject(v2)
    expect(migrated.schemaVersion).toBe(CURRENT_SCHEMA_VERSION)
    // The two 'orders' references collapse to one catalog topic.
    expect(migrated.catalog.topics).toHaveLength(1)
    expect(migrated.catalog.topics[0].name).toBe('orders')
    const topicId = migrated.catalog.topics[0].id
    expect(migrated.nodes[0].config.topicId).toBe(topicId)
    expect(migrated.nodes[1].config.topicId).toBe(topicId)
    expect(migrated.nodes[0].config.topic).toBeUndefined()
  })

  it('throws for a document newer than the supported version', () => {
    expect(() => migrateProject({ schemaVersion: 999 })).toThrow()
  })
})
