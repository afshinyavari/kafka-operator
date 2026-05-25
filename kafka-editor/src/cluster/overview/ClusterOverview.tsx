import { RefreshCw } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { getClusterOverview } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'

interface ClusterOverviewProps {
  cluster: SavedCluster
}

/** Dashboard: cluster id, controller, brokers, aggregate counts. */
export function ClusterOverview({ cluster }: ClusterOverviewProps) {
  const conn = toConnection(cluster)
  const { data, loading, error, refetch } = useAdminQuery(
    `${cluster.id}:overview`,
    () => getClusterOverview(conn),
  )

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
        <h2 className="text-sm font-semibold text-slate-800">
          Cluster overview
        </h2>
        <button
          type="button"
          onClick={refetch}
          className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh
        </button>
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {error && <ErrorBanner message={error} onRetry={refetch} />}
        {loading && !data && (
          <p className="text-xs text-slate-400">Loading…</p>
        )}
        {data && (
          <div className="flex flex-col gap-4">
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <Stat label="Cluster id" value={data.clusterId || '—'} mono />
              <Stat label="Brokers" value={String(data.brokers.length)} />
              <Stat label="Topics" value={String(data.topicCount)} />
              <Stat label="Partitions" value={String(data.partitionCount)} />
            </div>

            <div>
              <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
                Brokers
              </h3>
              <table className="w-full border-collapse text-xs">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-slate-500">
                    <th className="py-1.5 pr-3 font-medium">ID</th>
                    <th className="py-1.5 pr-3 font-medium">Host</th>
                    <th className="py-1.5 pr-3 font-medium">Port</th>
                    <th className="py-1.5 pr-3 font-medium">Rack</th>
                    <th className="py-1.5 font-medium">Role</th>
                  </tr>
                </thead>
                <tbody>
                  {data.brokers.map((b) => (
                    <tr key={b.id} className="border-b border-slate-100">
                      <td className="py-1.5 pr-3 font-mono">{b.id}</td>
                      <td className="py-1.5 pr-3 font-mono">{b.host}</td>
                      <td className="py-1.5 pr-3 font-mono">{b.port}</td>
                      <td className="py-1.5 pr-3">{b.rack ?? '—'}</td>
                      <td className="py-1.5">
                        {data.controller && b.id === data.controller.id ? (
                          <span className="rounded bg-slate-800 px-1.5 py-0.5 text-[10px] text-white">
                            controller
                          </span>
                        ) : (
                          <span className="text-slate-400">broker</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}

function Stat({
  label,
  value,
  mono,
}: {
  label: string
  value: string
  mono?: boolean
}) {
  return (
    <div className="rounded border border-slate-200 p-2.5">
      <p className="text-[10px] tracking-wide text-slate-400 uppercase">
        {label}
      </p>
      <p
        className={`mt-0.5 truncate text-sm text-slate-800 ${
          mono ? 'font-mono text-xs' : 'font-semibold'
        }`}
        title={value}
      >
        {value}
      </p>
    </div>
  )
}
