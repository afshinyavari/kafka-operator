import { create } from 'zustand'

/** The two top-level modes of the app. */
export type AppView = 'editor' | 'cluster'

/** Sections of the Cluster (management) view. */
export type ClusterSection =
  | 'overview'
  | 'topics'
  | 'groups'
  | 'schemas'
  | 'acls'
  | 'connect'

/**
 * A cross-link target — where the Cluster view should focus when it is opened
 * from elsewhere (e.g. a canvas source node jumping to its live topic).
 */
export type ClusterFocus =
  | { kind: 'overview' }
  | { kind: 'topics' }
  | { kind: 'topic'; name: string }
  | { kind: 'groups' }
  | { kind: 'group'; id: string }

interface ViewState {
  view: AppView
  clusterSection: ClusterSection
  /** The topic open in the Topics section, or null for the topic list. */
  selectedTopic: string | null
  /** True once the Cluster view has been opened — gates its lazy mount. */
  clusterEverOpened: boolean
  setView: (view: AppView) => void
  setClusterSection: (section: ClusterSection) => void
  setSelectedTopic: (topic: string | null) => void
  /** Switch to the Cluster view and focus a specific section. */
  openClusterAt: (focus: ClusterFocus) => void
}

/**
 * Top-level navigation state. Holding the cluster section/topic here (rather
 * than as ClusterView local state) lets cross-links navigate the Cluster view
 * with a plain store write — no effects, no pending-focus handshake.
 */
export const useViewStore = create<ViewState>((set) => ({
  view: 'editor',
  clusterSection: 'overview',
  selectedTopic: null,
  clusterEverOpened: false,

  setView: (view) =>
    set((s) => ({
      view,
      clusterEverOpened: s.clusterEverOpened || view === 'cluster',
    })),

  setClusterSection: (section) =>
    set({ clusterSection: section, selectedTopic: null }),

  setSelectedTopic: (topic) => set({ selectedTopic: topic }),

  openClusterAt: (focus) => {
    const base = { view: 'cluster' as const, clusterEverOpened: true }
    switch (focus.kind) {
      case 'overview':
        set({ ...base, clusterSection: 'overview', selectedTopic: null })
        break
      case 'topics':
        set({ ...base, clusterSection: 'topics', selectedTopic: null })
        break
      case 'topic':
        set({ ...base, clusterSection: 'topics', selectedTopic: focus.name })
        break
      case 'groups':
      case 'group':
        set({ ...base, clusterSection: 'groups', selectedTopic: null })
        break
    }
  },
}))
