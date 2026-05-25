import { useMemo, useState } from 'react'
import { ArrowLeft, RefreshCw, RotateCcw, Trash2 } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { deleteConsumerGroup, getGroupOffsets } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'
import { Modal } from '../../components/Modal'
import { ResetOffsetsModal } from './ResetOffsetsModal'
import { formatLag, lagByTopic } from './lag'

interface GroupDetailProps {
  cluster: SavedCluster
  groupId: string
  onBack: () => void
}

const actionBtn =
  'flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100'

/** Consumer-group detail — per-partition lag, reset and delete. */
export function GroupDetail({ cluster, groupId, onBack }: GroupDetailProps) {
  const conn = toConnection(cluster)
  const { data, loading, error, refetch } = useAdminQuery(
    `${cluster.id}:group:${groupId}`,
    () => getGroupOffsets(conn, groupId),
  )
  const [modal, setModal] = useState<'reset' | 'delete' | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)

  const topics = useMemo(
    () => [...new Set((data?.partitions ?? []).map((p) => p.topic))].sort(),
    [data],
  )
  const topicLag = useMemo(
    () => lagByTopic(data?.partitions ?? []),
    [data],
  )

  const doDelete = async () => {
    setDeleting(true)
    setDeleteError(null)
    try {
      await deleteConsumerGroup(conn, groupId)
      onBack()
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : String(e))
      setDeleting(false)
    }
  }

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-2 border-b border-slate-200 px-4 py-2">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onBack}
            aria-label="Back"
            className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
          </button>
          <h2 className="font-mono text-sm font-semibold text-slate-800">
            {groupId}
          </h2>
          {data && (
            <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] text-slate-500">
              {data.state.toLowerCase()}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setModal('reset')}
            className={actionBtn}
          >
            <RotateCcw className="h-3.5 w-3.5" />
            Reset offsets
          </button>
          <button
            type="button"
            onClick={() => setModal('delete')}
            className="flex items-center gap-1 rounded border border-rose-300 px-2 py-1 text-xs text-rose-600 hover:bg-rose-50"
          >
            <Trash2 className="h-3.5 w-3.5" />
            Delete
          </button>
          <button type="button" onClick={refetch} className={actionBtn}>
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {error && <ErrorBanner message={error} onRetry={refetch} />}
        {loading && !data && (
          <p className="text-xs text-slate-400">Loading…</p>
        )}
        {data && (
          <div className="flex flex-col gap-5">
            <div className="rounded border border-slate-200 p-2.5">
              <p className="text-[10px] tracking-wide text-slate-400 uppercase">
                Total lag
              </p>
              <p className="mt-0.5 text-lg font-semibold text-slate-800">
                {data.totalLag.toLocaleString()}
              </p>
            </div>

            {topicLag.length > 0 && (
              <section>
                <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
                  Lag by topic
                </h3>
                <table className="w-full border-collapse text-xs">
                  <thead>
                    <tr className="border-b border-slate-200 text-left text-slate-500">
                      <th className="py-1.5 pr-3 font-medium">Topic</th>
                      <th className="py-1.5 pr-3 font-medium">Partitions</th>
                      <th className="py-1.5 font-medium">Lag</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topicLag.map((t) => (
                      <tr key={t.topic} className="border-b border-slate-100">
                        <td className="py-1.5 pr-3 font-mono text-slate-700">
                          {t.topic}
                        </td>
                        <td className="py-1.5 pr-3">{t.partitions}</td>
                        <td className="py-1.5 font-mono">
                          {t.lag.toLocaleString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>
            )}

            <section>
              <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
                Partitions ({data.partitions.length})
              </h3>
              {data.partitions.length === 0 ? (
                <p className="text-xs text-slate-400">
                  This group has no committed offsets.
                </p>
              ) : (
                <table className="w-full border-collapse text-xs">
                  <thead>
                    <tr className="border-b border-slate-200 text-left text-slate-500">
                      <th className="py-1.5 pr-3 font-medium">Topic</th>
                      <th className="py-1.5 pr-3 font-medium">Partition</th>
                      <th className="py-1.5 pr-3 font-medium">Committed</th>
                      <th className="py-1.5 pr-3 font-medium">End</th>
                      <th className="py-1.5 font-medium">Lag</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.partitions.map((p) => (
                      <tr
                        key={`${p.topic}-${p.partition}`}
                        className="border-b border-slate-100"
                      >
                        <td className="py-1.5 pr-3 font-mono text-slate-700">
                          {p.topic}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">
                          {p.partition}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">
                          {p.committedOffset ?? '—'}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">
                          {p.endOffset}
                        </td>
                        <td className="py-1.5 font-mono">
                          {formatLag(p.lag)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </section>
          </div>
        )}
      </div>

      {modal === 'reset' && (
        <ResetOffsetsModal
          conn={conn}
          groupId={groupId}
          topics={topics}
          onClose={() => setModal(null)}
          onDone={refetch}
        />
      )}
      {modal === 'delete' && (
        <Modal
          title="Delete consumer group"
          onClose={() => setModal(null)}
          footer={
            <>
              <button
                type="button"
                onClick={() => setModal(null)}
                className="rounded border border-slate-300 px-3 py-1 text-xs text-slate-600 hover:bg-slate-100"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={doDelete}
                disabled={deleting}
                className="rounded border border-rose-700 bg-rose-600 px-3 py-1 text-xs font-medium text-white hover:bg-rose-700 disabled:opacity-50"
              >
                {deleting ? 'Deleting…' : 'Delete group'}
              </button>
            </>
          }
        >
          {deleteError && <ErrorBanner message={deleteError} />}
          <p className="text-xs text-slate-600">
            Delete consumer group{' '}
            <span className="font-mono">{groupId}</span>? Its committed offsets
            will be lost.
          </p>
        </Modal>
      )}
    </div>
  )
}
