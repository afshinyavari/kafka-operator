import { describe, it, expect } from 'vitest'
import { generateConnectorConfigs } from './connectorConfig'
import { createEmptyProject } from '../model/project'

describe('generateConnectorConfigs', () => {
  it('emits nothing when there are no connector nodes', () => {
    expect(generateConnectorConfigs(createEmptyProject())).toEqual([])
  })

  it('emits a connector-config JSON per connector node', () => {
    const doc = createEmptyProject()
    doc.catalog.topics = [{ id: 't1', name: 'orders' }]
    doc.nodes = [
      {
        id: 'n1',
        type: 'connect-sink',
        position: { x: 0, y: 0 },
        config: {
          label: 'Orders Sink',
          topicId: 't1',
          connectorClass: 'io.confluent.connect.jdbc.JdbcSinkConnector',
          connectorConfig: [{ id: 'c1', key: 'tasks.max', value: '3' }],
        },
      },
    ]
    const files = generateConnectorConfigs(doc)
    expect(files).toHaveLength(1)
    expect(files[0].path).toBe('connectors/orders-sink.json')

    const parsed = JSON.parse(files[0].content)
    expect(parsed.name).toBe('orders-sink')
    expect(parsed.config['connector.class']).toBe(
      'io.confluent.connect.jdbc.JdbcSinkConnector',
    )
    expect(parsed.config.topics).toBe('orders')
    // A user-supplied entry overrides the default.
    expect(parsed.config['tasks.max']).toBe('3')
  })

  it('uses `topic` for source connectors and dedupes names', () => {
    const doc = createEmptyProject()
    doc.catalog.topics = [{ id: 't1', name: 'events' }]
    const sourceNode = {
      type: 'connect-source',
      position: { x: 0, y: 0 },
      config: { label: 'Feed', topicId: 't1', connectorClass: 'X' },
    }
    doc.nodes = [
      { id: 'a', ...sourceNode },
      { id: 'b', ...sourceNode },
    ]
    const files = generateConnectorConfigs(doc)
    expect(files.map((f) => f.path)).toEqual([
      'connectors/feed.json',
      'connectors/feed-2.json',
    ])
    expect(JSON.parse(files[0].content).config.topic).toBe('events')
  })
})
