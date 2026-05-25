import { useMemo, useState } from 'react'
import {
  ArrowLeft,
  Check,
  Inbox,
  Library,
  Plus,
  RefreshCw,
  Settings2,
  Trash2,
} from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { getTopic } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { useEditorStore } from '../../state/store'
import { catalogHasTopic, liveTopicToDef } from '../crosslink'
import { ErrorBanner } from '../../components/ErrorBanner'
import { TopicMetricsPanel } from './TopicMetricsPanel'
import { TopicConfigEditor } from './TopicConfigEditor'
import { AddPartitionsModal } from './AddPartitionsModal'
import { DeleteTopicModal } from './DeleteTopicModal'

interface TopicDetailProps {
  cluster: SavedCluster
  topic: string
  onBack: () => void
  onBrowseMessages: () => void
}

type OpenModal = 'config' | 'partitions' | 'delete' | null

const actionBtn =
  'flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100'

/** Topic detail — metrics, partitions, configuration, and lifecycle actions. */
export function TopicDetail({
  cluster,
  topic,
  onBack,
  onBrowseMessages,
}: TopicDetailProps) {
  const conn = toConnection(cluster)
  const { data, loading, error, refetch } = useAdminQuery(
    `${cluster.id}:topic:${topic}`,
    () => getTopic(conn, topic),
  )
  const [showDefaults, setShowDefaults] = useState(false)
  const [modal, setModal] = useState<OpenModal>(null)
  // Bumped after a mutation so the metrics panel remounts and refetches.
  const [reload, setReload] = useState(0)

  // Cross-link: bring this live topic into the editor's design-time catalog.
  const addTopic = useEditorStore((s) => s.addTopic)
  const editorCatalog = useEditorStore((s) => s.catalog)
  const [justAdded, setJustAdded] = useState(false)
  const inCatalog = justAdded || catalogHasTopic(editorCatalog, topic)

  const addToCatalog = () => {
    if (!catalogHasTopic(editorCatalog, topic)) {
      addTopic(liveTopicToDef(topic, data?.partitions.length ?? 1))
    }
    setJustAdded(true)
  }

  const configs = useMemo(() => {
    const all = data?.configs ?? []
    return showDefaults ? all : all.filter((c) => !c.isDefault)
  }, [data, showDefaults])

  const refreshAll = () => {
    refetch()
    setReload((r) => r + 1)
  }

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-2 border-b border-slate-200 px-4 py-2">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onBack}
            aria-label="Back to topics"
            className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
          </button>
          <h2 className="font-mono text-sm font-semibold text-slate-800">
            {topic}
          </h2>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onBrowseMessages}
            className="flex items-center gap-1 rounded border border-slate-800 bg-slate-800 px-2 py-1 text-xs font-medium text-white hover:bg-slate-700"
          >
            <Inbox className="h-3.5 w-3.5" />
            Messages
          </button>
          <button
            type="button"
            onClick={addToCatalog}
            disabled={inCatalog}
            title="Add this topic to the editor's design-time catalog"
            className={actionBtn}
          >
            {inCatalog ? (
              <Check className="h-3.5 w-3.5 text-emerald-600" />
            ) : (
              <Library className="h-3.5 w-3.5" />
            )}
            {inCatalog ? 'In catalog' : 'Add to catalog'}
          </button>
          <button type="button" onClick={() => setModal('config')} className={actionBtn}>
            <Settings2 className="h-3.5 w-3.5" />
            Configure
          </button>
          <button
            type="button"
            onClick={() => setModal('partitions')}
            className={actionBtn}
          >
            <Plus className="h-3.5 w-3.5" />
            Add partitions
          </button>
          <button
            type="button"
            onClick={() => setModal('delete')}
            className="flex items-center gap-1 rounded border border-rose-300 px-2 py-1 text-xs text-rose-600 hover:bg-rose-50"
          >
            <Trash2 className="h-3.5 w-3.5" />
            Delete
          </button>
          <button type="button" onClick={refreshAll} className={actionBtn}>
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
            <section>
              <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
                Metrics
              </h3>
              <TopicMetricsPanel
                key={reload}
                cluster={cluster}
                topic={topic}
              />
            </section>

            <section>
              <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
                Partitions ({data.partitions.length})
              </h3>
              <table className="w-full border-collapse text-xs">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-slate-500">
                    <th className="py-1.5 pr-3 font-medium">Partition</th>
                    <th className="py-1.5 pr-3 font-medium">Leader</th>
                    <th className="py-1.5 pr-3 font-medium">Replicas</th>
                    <th className="py-1.5 pr-3 font-medium">ISR</th>
                    <th className="py-1.5 font-medium">Health</th>
                  </tr>
                </thead>
                <tbody>
                  {data.partitions.map((p) => {
                    const underReplicated = p.isr.length < p.replicas.length
                    return (
                      <tr
                        key={p.partition}
                        className="border-b border-slate-100"
                      >
                        <td className="py-1.5 pr-3 font-mono">
                          {p.partition}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">
                          {p.leader < 0 ? '—' : p.leader}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">
                          {p.replicas.join(', ')}
                        </td>
                        <td className="py-1.5 pr-3 font-mono">
                          {p.isr.join(', ')}
                        </td>
                        <td className="py-1.5">
                          {p.leader < 0 ? (
                            <span className="text-rose-600">no leader</span>
                          ) : underReplicated ? (
                            <span className="text-amber-600">
                              under-replicated
                            </span>
                          ) : (
                            <span className="text-emerald-600">ok</span>
                          )}
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </section>

            <section>
              <div className="mb-1.5 flex items-center justify-between">
                <h3 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
                  Configuration ({configs.length})
                </h3>
                <label className="flex items-center gap-1 text-xs text-slate-500">
                  <input
                    type="checkbox"
                    checked={showDefaults}
                    onChange={(e) => setShowDefaults(e.target.checked)}
                  />
                  Show defaults
                </label>
              </div>
              {configs.length === 0 ? (
                <p className="text-xs text-slate-400">
                  {data.configs.length === 0
                    ? 'No configuration is visible.'
                    : 'No non-default overrides.'}
                </p>
              ) : (
                <table className="w-full border-collapse text-xs">
                  <thead>
                    <tr className="border-b border-slate-200 text-left text-slate-500">
                      <th className="py-1.5 pr-3 font-medium">Key</th>
                      <th className="py-1.5 pr-3 font-medium">Value</th>
                      <th className="py-1.5 font-medium">Source</th>
                    </tr>
                  </thead>
                  <tbody>
                    {configs.map((c) => (
                      <tr key={c.name} className="border-b border-slate-100">
                        <td className="py-1.5 pr-3 font-mono text-slate-700">
                          {c.name}
                        </td>
                        <td className="py-1.5 pr-3 font-mono break-all text-slate-600">
                          {c.sensitive ? '••••••' : (c.value ?? '')}
                        </td>
                        <td className="py-1.5 text-slate-400">
                          {c.isDefault ? 'default' : 'overridden'}
                          {c.readOnly && ' · read-only'}
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

      {modal === 'config' && data && (
        <TopicConfigEditor
          conn={conn}
          topic={topic}
          configs={data.configs}
          onClose={() => setModal(null)}
          onApplied={refreshAll}
        />
      )}
      {modal === 'partitions' && data && (
        <AddPartitionsModal
          conn={conn}
          topic={topic}
          currentCount={data.partitions.length}
          onClose={() => setModal(null)}
          onDone={refreshAll}
        />
      )}
      {modal === 'delete' && (
        <DeleteTopicModal
          conn={conn}
          topic={topic}
          onClose={() => setModal(null)}
          onDeleted={onBack}
        />
      )}
    </div>
  )
}
