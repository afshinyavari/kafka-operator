import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { createTopic } from '../../api/adminClient'
import { parseConfigText } from './topicConfig'
import type { ClusterConnection } from '../clusterStore'

interface CreateTopicModalProps {
  conn: ClusterConnection
  onClose: () => void
  onCreated: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

/** Form for creating a new topic. */
export function CreateTopicModal({
  conn,
  onClose,
  onCreated,
}: CreateTopicModalProps) {
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState(1)
  const [replicationFactor, setReplicationFactor] = useState(1)
  const [configText, setConfigText] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await createTopic(conn, {
        name: name.trim(),
        partitions,
        replicationFactor,
        configs: parseConfigText(configText),
      })
      onCreated()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="Create topic"
      onClose={onClose}
      footer={
        <>
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
            disabled={submitting || name.trim().length === 0}
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Creating…' : 'Create'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Name
        <input
          className={`${inputCls} font-mono`}
          value={name}
          placeholder="orders.v1"
          autoFocus
          onChange={(e) => setName(e.target.value)}
        />
      </label>
      <div className="flex gap-2">
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Partitions
          <input
            type="number"
            min={1}
            className={inputCls}
            value={partitions}
            onChange={(e) =>
              setPartitions(Math.max(1, Number(e.target.value) || 1))
            }
          />
        </label>
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Replication factor
          <input
            type="number"
            min={1}
            className={inputCls}
            value={replicationFactor}
            onChange={(e) =>
              setReplicationFactor(Math.max(1, Number(e.target.value) || 1))
            }
          />
        </label>
      </div>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Advanced configs (optional)
        <textarea
          className={`${inputCls} h-20 font-mono`}
          value={configText}
          placeholder={'retention.ms=604800000\ncleanup.policy=compact'}
          onChange={(e) => setConfigText(e.target.value)}
        />
        <span className="text-[11px] text-slate-400">
          One <code>key=value</code> per line.
        </span>
      </label>
    </Modal>
  )
}
