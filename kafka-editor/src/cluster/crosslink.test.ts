import { describe, expect, it } from 'vitest'
import type { Catalog } from '../model/catalog'
import { catalogHasTopic, liveTopicToDef } from './crosslink'

describe('liveTopicToDef', () => {
  it('builds a TopicDef with the name, partitions and an id', () => {
    const def = liveTopicToDef('orders', 6)
    expect(def.name).toBe('orders')
    expect(def.partitions).toBe(6)
    expect(def.id).toBeTruthy()
    expect(def.keyFormat).toBe('string')
    expect(def.valueFormat).toBe('json')
  })

  it('never produces fewer than one partition', () => {
    expect(liveTopicToDef('t', 0).partitions).toBe(1)
  })
})

describe('catalogHasTopic', () => {
  const catalog: Catalog = {
    topics: [
      { id: '1', name: 'orders' },
      { id: '2', name: 'events' },
    ],
  }

  it('detects an existing topic by name', () => {
    expect(catalogHasTopic(catalog, 'orders')).toBe(true)
  })

  it('returns false for an unknown topic', () => {
    expect(catalogHasTopic(catalog, 'missing')).toBe(false)
  })
})
