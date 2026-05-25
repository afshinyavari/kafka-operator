import { useState } from 'react'
import { Server } from 'lucide-react'
import { useClusterStore } from './clusterStore'
import { ClusterManagerModal } from './ClusterManagerModal'
import { useOperatorMode } from '../operator/useOperatorConfig'

/** Active-cluster selector + "Manage clusters", shown in the header.
 *  In operator mode the cluster is operator-managed (one entry, fixed name),
 *  so the selector becomes a read-only label and "Manage clusters" is hidden. */
export function ClusterTopBar() {
  const clusters = useClusterStore((s) => s.clusters)
  const activeId = useClusterStore((s) => s.activeId)
  const setActiveCluster = useClusterStore((s) => s.setActiveCluster)
  const [showManager, setShowManager] = useState(false)
  const operatorMode = useOperatorMode()

  if (operatorMode) {
    const active =
      clusters.find((c) => c.id === activeId) ?? clusters[0] ?? null
    return (
      <span
        className="flex items-center gap-1.5 rounded border border-slate-300 bg-slate-50 px-2 py-1 text-xs text-slate-700"
        title="Cluster managed by the kafka-operator"
      >
        <Server className="h-3.5 w-3.5" />
        {active?.name ?? 'Managed cluster'}
      </span>
    )
  }

  return (
    <>
      <select
        value={activeId ?? ''}
        onChange={(e) => setActiveCluster(e.target.value || null)}
        disabled={clusters.length === 0}
        className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 outline-none focus:border-slate-500 disabled:text-slate-400"
        aria-label="Active cluster"
      >
        {clusters.length === 0 && <option value="">No clusters</option>}
        {clusters.map((c) => (
          <option key={c.id} value={c.id}>
            {c.name || c.bootstrapServers || 'Unnamed cluster'}
          </option>
        ))}
      </select>
      <button
        type="button"
        onClick={() => setShowManager(true)}
        className="flex items-center gap-1.5 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
      >
        <Server className="h-3.5 w-3.5" />
        Manage clusters
      </button>
      {showManager && (
        <ClusterManagerModal onClose={() => setShowManager(false)} />
      )}
    </>
  )
}
