import { create } from 'zustand'
import { nanoid } from 'nanoid'

/**
 * Saved Kafka cluster connections — a local-only list, modeled on envStore.
 * Deliberately kept out of the project document (and its export): these hold
 * broker hosts and, later, credentials. Persisted to localStorage.
 *
 * This is separate from the project's own `environment` (edited in Settings),
 * which is what a *Run* targets. The Cluster explorer uses this list instead.
 */

export interface SavedCluster {
  id: string
  name: string
  bootstrapServers: string
  schemaRegistryUrl: string
  connectUrl: string
}

/** The connection fields an admin API call needs. */
export interface ClusterConnection {
  bootstrapServers: string
  schemaRegistryUrl: string
  connectUrl: string
}

const STORAGE_KEY = 'kafka-editor-clusters'

interface Persisted {
  clusters: SavedCluster[]
  activeId: string | null
}

function load(): Persisted {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return { clusters: [], activeId: null }
    const parsed = JSON.parse(raw) as Partial<Persisted>
    return {
      clusters: parsed.clusters ?? [],
      activeId: parsed.activeId ?? null,
    }
  } catch {
    return { clusters: [], activeId: null }
  }
}

function persist(clusters: SavedCluster[], activeId: string | null): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify({ clusters, activeId }))
  } catch {
    // Storage may be unavailable — non-fatal.
  }
}

interface ClusterState {
  clusters: SavedCluster[]
  activeId: string | null
  /** Add a cluster; the first one added becomes active. Returns its id. */
  addCluster: (input: Omit<SavedCluster, 'id'>) => string
  updateCluster: (id: string, patch: Partial<Omit<SavedCluster, 'id'>>) => void
  removeCluster: (id: string) => void
  setActiveCluster: (id: string | null) => void
}

const initial = load()

export const useClusterStore = create<ClusterState>((set) => ({
  clusters: initial.clusters,
  activeId: initial.activeId,

  addCluster: (input) => {
    const cluster: SavedCluster = { id: nanoid(), ...input }
    set((s) => {
      const clusters = [...s.clusters, cluster]
      const activeId = s.activeId ?? cluster.id
      persist(clusters, activeId)
      return { clusters, activeId }
    })
    return cluster.id
  },

  updateCluster: (id, patch) =>
    set((s) => {
      const clusters = s.clusters.map((c) =>
        c.id === id ? { ...c, ...patch } : c,
      )
      persist(clusters, s.activeId)
      return { clusters }
    }),

  removeCluster: (id) =>
    set((s) => {
      const clusters = s.clusters.filter((c) => c.id !== id)
      const activeId =
        s.activeId === id ? (clusters[0]?.id ?? null) : s.activeId
      persist(clusters, activeId)
      return { clusters, activeId }
    }),

  setActiveCluster: (id) =>
    set((s) => {
      persist(s.clusters, id)
      return { activeId: id }
    }),
}))

/** The connection fields for a saved cluster. */
export function toConnection(cluster: SavedCluster): ClusterConnection {
  return {
    bootstrapServers: cluster.bootstrapServers,
    schemaRegistryUrl: cluster.schemaRegistryUrl,
    connectUrl: cluster.connectUrl,
  }
}

/** Reactive hook for the currently-active saved cluster (or null). */
export function useActiveCluster(): SavedCluster | null {
  return useClusterStore(
    (s) => s.clusters.find((c) => c.id === s.activeId) ?? null,
  )
}
