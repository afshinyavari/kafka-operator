import { useEffect } from 'react'
import { create } from 'zustand'
import { apiBase } from '../api/baseUrl'
import { useClusterStore } from '../cluster/clusterStore'

/**
 * Operator-mode config — fetched once on app load from /api/config (public,
 * no auth needed). When `operatorMode === true` the backend is deployed by
 * the kafka-operator: it ignores any SPA-supplied bootstrap and routes every
 * Kafka call through the injected proxy URL via SASL_SSL + OAUTHBEARER.
 *
 * The standalone cluster-manager UI (Add Cluster / Manage Clusters) is then
 * meaningless and is hidden — the SPA auto-creates a single "Managed cluster"
 * entry so the rest of the cluster-aware UI continues to work unchanged.
 */
export interface OperatorConfig {
  operatorMode: boolean
}

interface OperatorConfigState {
  config: OperatorConfig | null
  setConfig: (c: OperatorConfig) => void
}

export const useOperatorConfigStore = create<OperatorConfigState>((set) => ({
  config: null,
  setConfig: (config) => set({ config }),
}))

/** Returns the loaded operator config, or null while it's still in-flight. */
export function useOperatorConfig(): OperatorConfig | null {
  return useOperatorConfigStore((s) => s.config)
}

/** Convenience: `true` only after the fetch succeeded AND operatorMode is true. */
export function useOperatorMode(): boolean {
  return useOperatorConfigStore((s) => s.config?.operatorMode === true)
}

const MANAGED_CLUSTER_NAME = 'Managed cluster'

/**
 * Fetches /api/config exactly once and, in operator mode, ensures the cluster
 * store has at least one entry so the rest of the UI has something to
 * activate. The bootstrap value is a placeholder — the operator-deployed
 * backend overrides it from `KAFKA_EDITOR_BOOTSTRAP_SERVERS` server-side.
 */
export function useBootstrapOperatorMode(): void {
  useEffect(() => {
    let cancelled = false
    fetch(apiBase() + '/api/config', { credentials: 'include' })
      .then((r) => (r.ok ? (r.json() as Promise<OperatorConfig>) : null))
      .then((cfg) => {
        if (cancelled || !cfg) return
        useOperatorConfigStore.getState().setConfig(cfg)

        if (cfg.operatorMode) {
          const { clusters, addCluster } = useClusterStore.getState()
          if (clusters.length === 0) {
            addCluster({
              name: MANAGED_CLUSTER_NAME,
              bootstrapServers: 'managed',
              schemaRegistryUrl: '',
              connectUrl: '',
            })
          }
        }
      })
      .catch(() => {
        // Network error / unauthenticated /api/config — fall through to
        // standalone mode; the cluster-manager UI stays visible.
      })
    return () => {
      cancelled = true
    }
  }, [])
}
