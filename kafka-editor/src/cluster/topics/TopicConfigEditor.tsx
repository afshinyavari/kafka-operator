import { useMemo, useState } from 'react'
import { nanoid } from 'nanoid'
import { Plus, RotateCcw, Trash2 } from 'lucide-react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { updateTopicConfig } from '../../api/adminClient'
import type { ConfigKv } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'
import { computeConfigDiff } from './topicConfig'
import type { ConfigRow } from './topicConfig'

interface TopicConfigEditorProps {
  conn: ClusterConnection
  topic: string
  configs: ConfigKv[]
  onClose: () => void
  onApplied: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 font-mono text-xs text-slate-800 outline-none focus:border-slate-500'

/** Edit a topic's non-default config; sends only the changed entries. */
export function TopicConfigEditor({
  conn,
  topic,
  configs,
  onClose,
  onApplied,
}: TopicConfigEditorProps) {
  const [rows, setRows] = useState<ConfigRow[]>(() =>
    configs
      .filter((c) => !c.isDefault && !c.readOnly && !c.sensitive)
      .map((c) => ({
        id: nanoid(),
        name: c.name,
        value: c.value ?? '',
        original: c.value ?? '',
      })),
  )
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const diff = useMemo(() => computeConfigDiff(rows), [rows])
  const changeCount = Object.keys(diff).length

  const patch = (id: string, p: Partial<ConfigRow>) =>
    setRows((rs) => rs.map((r) => (r.id === id ? { ...r, ...p } : r)))

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await updateTopicConfig(conn, topic, diff)
      onApplied()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="Edit topic configuration"
      onClose={onClose}
      width="w-[560px]"
      footer={
        <>
          <span className="mr-auto self-center text-[11px] text-slate-400">
            {changeCount === 0
              ? 'No changes'
              : `${changeCount} change${changeCount === 1 ? '' : 's'}`}
          </span>
          <button
            type="button"
            onClick={onClose}
            className="rounded border border-slate-300 px-3 py-1 text-xs text-slate-600 hover:bg-slate-100"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={submit}
            disabled={submitting || changeCount === 0}
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Applying…' : 'Apply changes'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      {rows.length === 0 && (
        <p className="text-xs text-slate-400">
          No overrides yet. Add a config key below.
        </p>
      )}
      {rows.map((row) => {
        const changed = row.value !== row.original
        return (
          <div key={row.id} className="flex items-center gap-1.5">
            <input
              className={`${inputCls} flex-1 ${row.original ? 'bg-slate-50' : ''}`}
              value={row.name}
              placeholder="config.key"
              readOnly={row.original !== ''}
              onChange={(e) => patch(row.id, { name: e.target.value })}
            />
            <input
              className={`${inputCls} flex-1 ${
                changed ? 'border-emerald-400' : ''
              }`}
              value={row.value}
              placeholder="(default)"
              onChange={(e) => patch(row.id, { value: e.target.value })}
            />
            {row.original !== '' ? (
              <button
                type="button"
                onClick={() => patch(row.id, { value: '' })}
                title="Reset to default"
                className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
              >
                <RotateCcw className="h-3.5 w-3.5" />
              </button>
            ) : (
              <button
                type="button"
                onClick={() =>
                  setRows((rs) => rs.filter((r) => r.id !== row.id))
                }
                title="Remove"
                className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
              >
                <Trash2 className="h-3.5 w-3.5" />
              </button>
            )}
          </div>
        )
      })}
      <button
        type="button"
        onClick={() =>
          setRows((rs) => [
            ...rs,
            { id: nanoid(), name: '', value: '', original: '' },
          ])
        }
        className="flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
      >
        <Plus className="h-3.5 w-3.5" />
        Add config key
      </button>
      <p className="text-[11px] text-slate-400">
        Clearing a value resets that key to the broker default.
      </p>
    </Modal>
  )
}
