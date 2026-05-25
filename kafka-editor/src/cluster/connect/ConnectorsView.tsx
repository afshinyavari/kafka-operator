import { useState } from 'react'
import { RefreshCw } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { listConnectors } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'
import { EmptyState } from '../../components/EmptyState'
import { ConnectorDetail } from './ConnectorDetail'
import { stateColor } from './connectorState'

interface ConnectorsViewProps {
  cluster: SavedCluster
}

/** Kafka Connect connectors — list and manage. */
export function ConnectorsView({ cluster }: ConnectorsViewProps) {
  const conn = toConnection(cluster)
  const [reload, setReload] = useState(0)
  const [selected, setSelected] = useState<string | null>(null)
  const { data, loading, error } = useAdminQuery(
    `${cluster.id}:connectors:${reload}`,
    () => listConnectors(conn),
  )

  const refresh = () => setReload((n) => n + 1)

  if (data && data.capability !== 'supported') {
    return (
      <EmptyState
        title="Kafka Connect not configured"
        hint="Add a Kafka Connect URL to this cluster (Manage clusters) to list and manage connectors."
      />
    )
  }

  const connectors = data?.data ?? {}
  const names = Object.keys(connectors).sort()
  const selectedEntry = selected ? connectors[selected] : undefined

  if (selected && selectedEntry) {
    return (
      <ConnectorDetail
        conn={conn}
        name={selected}
        entry={selectedEntry}
        onBack={() => setSelected(null)}
        onChanged={refresh}
      />
    )
  }

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
        <h2 className="text-sm font-semibold text-slate-800">
          Connectors
          {data && (
            <span className="ml-1 text-xs font-normal text-slate-400">
              ({names.length})
            </span>
          )}
        </h2>
        <button
          type="button"
          onClick={refresh}
          className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </button>
      </div>

      <div className="flex-1 overflow-y-auto">
        {error && <ErrorBanner message={error} onRetry={refresh} />}
        {loading && !data && (
          <p className="p-4 text-xs text-slate-400">Loading…</p>
        )}
        {data && names.length === 0 && (
          <p className="p-4 text-xs text-slate-400">No connectors.</p>
        )}
        {names.length > 0 && (
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 bg-white">
              <tr className="border-b border-slate-200 text-left text-slate-500">
                <th className="py-1.5 pr-3 pl-4 font-medium">Name</th>
                <th className="py-1.5 pr-3 font-medium">Type</th>
                <th className="py-1.5 pr-3 font-medium">State</th>
                <th className="py-1.5 pr-4 font-medium">Tasks</th>
              </tr>
            </thead>
            <tbody>
              {names.map((name) => {
                const entry = connectors[name]
                const state = entry.status?.connector?.state ?? 'UNKNOWN'
                const tasks = entry.status?.tasks ?? []
                const failed = tasks.filter(
                  (t) => t.state === 'FAILED',
                ).length
                return (
                  <tr
                    key={name}
                    onClick={() => setSelected(name)}
                    className="cursor-pointer border-b border-slate-100 hover:bg-slate-50"
                  >
                    <td className="py-1.5 pr-3 pl-4 font-mono text-slate-800">
                      {name}
                    </td>
                    <td className="py-1.5 pr-3 text-slate-500">
                      {entry.info?.type ?? entry.status?.type ?? '—'}
                    </td>
                    <td className="py-1.5 pr-3">
                      <span
                        className={`rounded px-1.5 py-0.5 text-[10px] ${stateColor(
                          state,
                        )}`}
                      >
                        {state.toLowerCase()}
                      </span>
                    </td>
                    <td className="py-1.5 pr-4">
                      {tasks.length}
                      {failed > 0 && (
                        <span className="ml-1 text-rose-600">
                          ({failed} failed)
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
