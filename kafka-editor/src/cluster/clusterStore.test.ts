import { afterEach, describe, expect, it } from 'vitest'
import { toConnection, useClusterStore } from './clusterStore'
import type { SavedCluster } from './clusterStore'

const blank = {
  name: 'A',
  bootstrapServers: 'a:9092',
  schemaRegistryUrl: '',
  connectUrl: '',
}

afterEach(() => {
  useClusterStore.setState({ clusters: [], activeId: null })
})

describe('clusterStore', () => {
  it('the first added cluster becomes active', () => {
    const id = useClusterStore.getState().addCluster(blank)
    const state = useClusterStore.getState()
    expect(state.clusters).toHaveLength(1)
    expect(state.activeId).toBe(id)
  })

  it('a later cluster does not steal the active selection', () => {
    const first = useClusterStore.getState().addCluster(blank)
    useClusterStore.getState().addCluster({ ...blank, name: 'B' })
    expect(useClusterStore.getState().activeId).toBe(first)
  })

  it('updateCluster patches only the targeted cluster', () => {
    const id = useClusterStore.getState().addCluster(blank)
    useClusterStore.getState().addCluster({ ...blank, name: 'B' })
    useClusterStore.getState().updateCluster(id, { name: 'Renamed' })
    const clusters = useClusterStore.getState().clusters
    expect(clusters.find((c) => c.id === id)?.name).toBe('Renamed')
    expect(clusters.find((c) => c.id !== id)?.name).toBe('B')
  })

  it('removing the active cluster reactivates the next one', () => {
    const a = useClusterStore.getState().addCluster(blank)
    const b = useClusterStore.getState().addCluster({ ...blank, name: 'B' })
    useClusterStore.getState().removeCluster(a)
    expect(useClusterStore.getState().activeId).toBe(b)
  })

  it('removing the last cluster clears the active selection', () => {
    const a = useClusterStore.getState().addCluster(blank)
    useClusterStore.getState().removeCluster(a)
    expect(useClusterStore.getState().activeId).toBeNull()
  })

  it('toConnection extracts the connection fields', () => {
    const cluster: SavedCluster = {
      id: 'x',
      name: 'A',
      bootstrapServers: 'a:9092',
      schemaRegistryUrl: 'http://r',
      connectUrl: 'http://c',
    }
    expect(toConnection(cluster)).toEqual({
      bootstrapServers: 'a:9092',
      schemaRegistryUrl: 'http://r',
      connectUrl: 'http://c',
    })
  })
})
