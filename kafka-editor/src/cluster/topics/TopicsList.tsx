import { useMemo, useState } from 'react'
import { Plus, RefreshCw, Search } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { listTopics } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'
import { CreateTopicModal } from './CreateTopicModal'

interface TopicsListProps {
  cluster: SavedCluster
  onSelectTopic: (name: string) => void
}

/** The cluster's topic list, with a name filter. */
export function TopicsList({ cluster, onSelectTopic }: TopicsListProps) {
  const conn = toConnection(cluster)
  const { data, loading, error, refetch } = useAdminQuery(
    `${cluster.id}:topics`,
    () => listTopics(conn),
  )
  const [filter, setFilter] = useState('')
  const [showInternal, setShowInternal] = useState(false)
  const [showCreate, setShowCreate] = useState(false)

  const rows = useMemo(() => {
    const needle = filter.trim().toLowerCase()
    return (data ?? [])
      .filter((t) => showInternal || !t.internal)
      .filter((t) => !needle || t.name.toLowerCase().includes(needle))
  }, [data, filter, showInternal])

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-2 border-b border-slate-200 px-4 py-2">
        <h2 className="text-sm font-semibold text-slate-800">
          Topics
          {data && (
            <span className="ml-1 text-xs font-normal text-slate-400">
              ({rows.length})
            </span>
          )}
        </h2>
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-1 rounded border border-slate-300 px-1.5">
            <Search className="h-3 w-3 text-slate-400" />
            <input
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
              placeholder="Filter…"
              className="w-36 py-1 text-xs text-slate-700 outline-none"
            />
          </div>
          <label className="flex items-center gap-1 text-xs text-slate-500">
            <input
              type="checkbox"
              checked={showInternal}
              onChange={(e) => setShowInternal(e.target.checked)}
            />
            Internal
          </label>
          <button
            type="button"
            onClick={refetch}
            className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
          >
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh
          </button>
          <button
            type="button"
            onClick={() => setShowCreate(true)}
            className="flex items-center gap-1 rounded border border-emerald-700 bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700"
          >
            <Plus className="h-3.5 w-3.5" />
            Create topic
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {error && <ErrorBanner message={error} onRetry={refetch} />}
        {loading && !data && (
          <p className="p-4 text-xs text-slate-400">Loading…</p>
        )}
        {data && rows.length === 0 && (
          <p className="p-4 text-xs text-slate-400">No matching topics.</p>
        )}
        {rows.length > 0 && (
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 bg-white">
              <tr className="border-b border-slate-200 text-left text-slate-500">
                <th className="py-1.5 pl-4 pr-3 font-medium">Name</th>
                <th className="py-1.5 pr-3 font-medium">Partitions</th>
                <th className="py-1.5 pr-3 font-medium">Replication</th>
                <th className="py-1.5 pr-4 font-medium" />
              </tr>
            </thead>
            <tbody>
              {rows.map((t) => (
                <tr
                  key={t.name}
                  onClick={() => onSelectTopic(t.name)}
                  className="cursor-pointer border-b border-slate-100 hover:bg-slate-50"
                >
                  <td className="py-1.5 pl-4 pr-3 font-mono text-slate-800">
                    {t.name}
                    {t.internal && (
                      <span className="ml-1.5 rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-400">
                        internal
                      </span>
                    )}
                  </td>
                  <td className="py-1.5 pr-3">{t.partitionCount}</td>
                  <td className="py-1.5 pr-3">{t.replicationFactor}</td>
                  <td className="py-1.5 pr-4 text-right text-slate-400">›</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {showCreate && (
        <CreateTopicModal
          conn={conn}
          onClose={() => setShowCreate(false)}
          onCreated={refetch}
        />
      )}
    </div>
  )
}
