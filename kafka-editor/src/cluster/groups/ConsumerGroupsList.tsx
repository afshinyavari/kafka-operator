import { RefreshCw } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { listConsumerGroups } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'

interface ConsumerGroupsListProps {
  cluster: SavedCluster
  onSelectGroup: (groupId: string) => void
}

const STATE_COLOR: Record<string, string> = {
  STABLE: 'bg-emerald-100 text-emerald-700',
  EMPTY: 'bg-slate-100 text-slate-500',
  DEAD: 'bg-slate-100 text-slate-400',
  PREPARING_REBALANCE: 'bg-amber-100 text-amber-700',
  COMPLETING_REBALANCE: 'bg-amber-100 text-amber-700',
}

/** The cluster's consumer groups. */
export function ConsumerGroupsList({
  cluster,
  onSelectGroup,
}: ConsumerGroupsListProps) {
  const conn = toConnection(cluster)
  const { data, loading, error, refetch } = useAdminQuery(
    `${cluster.id}:groups`,
    () => listConsumerGroups(conn),
  )

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
        <h2 className="text-sm font-semibold text-slate-800">
          Consumer groups
          {data && (
            <span className="ml-1 text-xs font-normal text-slate-400">
              ({data.length})
            </span>
          )}
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

      <div className="flex-1 overflow-y-auto">
        {error && <ErrorBanner message={error} onRetry={refetch} />}
        {loading && !data && (
          <p className="p-4 text-xs text-slate-400">Loading…</p>
        )}
        {data && data.length === 0 && (
          <p className="p-4 text-xs text-slate-400">No consumer groups.</p>
        )}
        {data && data.length > 0 && (
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 bg-white">
              <tr className="border-b border-slate-200 text-left text-slate-500">
                <th className="py-1.5 pr-3 pl-4 font-medium">Group</th>
                <th className="py-1.5 pr-3 font-medium">State</th>
                <th className="py-1.5 pr-3 font-medium">Members</th>
                <th className="py-1.5 pr-4 font-medium">Coordinator</th>
              </tr>
            </thead>
            <tbody>
              {data.map((g) => (
                <tr
                  key={g.groupId}
                  onClick={() => onSelectGroup(g.groupId)}
                  className="cursor-pointer border-b border-slate-100 hover:bg-slate-50"
                >
                  <td className="py-1.5 pr-3 pl-4 font-mono text-slate-800">
                    {g.groupId}
                  </td>
                  <td className="py-1.5 pr-3">
                    <span
                      className={`rounded px-1.5 py-0.5 text-[10px] ${
                        STATE_COLOR[g.state] ?? 'bg-slate-100 text-slate-500'
                      }`}
                    >
                      {g.state.toLowerCase()}
                    </span>
                  </td>
                  <td className="py-1.5 pr-3">{g.members}</td>
                  <td className="py-1.5 pr-4 font-mono text-slate-500">
                    {g.coordinator || '—'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  )
}
