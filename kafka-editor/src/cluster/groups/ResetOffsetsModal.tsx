import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { resetGroupOffsets } from '../../api/adminClient'
import type { ResetOffsetsInput } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'

interface ResetOffsetsModalProps {
  conn: ClusterConnection
  groupId: string
  /** Topics the group has committed offsets on (for the picker). */
  topics: string[]
  onClose: () => void
  onDone: () => void
}

const ctrl =
  'w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

type Target = ResetOffsetsInput['target']

/** Reset a consumer group's committed offsets for a topic. */
export function ResetOffsetsModal({
  conn,
  groupId,
  topics,
  onClose,
  onDone,
}: ResetOffsetsModalProps) {
  const [topic, setTopic] = useState(topics[0] ?? '')
  const [target, setTarget] = useState<Target>('EARLIEST')
  const [offset, setOffset] = useState('0')
  const [timestamp, setTimestamp] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await resetGroupOffsets(conn, groupId, {
        topic: topic.trim(),
        target,
        offset: target === 'OFFSET' ? Number(offset) || 0 : undefined,
        timestamp:
          target === 'TIMESTAMP' && timestamp
            ? new Date(timestamp).getTime()
            : undefined,
      })
      onDone()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="Reset offsets"
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
            disabled={submitting || topic.trim().length === 0}
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Resetting…' : 'Reset offsets'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <p className="text-xs text-slate-600">
        Resets every partition of the chosen topic for group{' '}
        <span className="font-mono">{groupId}</span>.
      </p>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Topic
        {topics.length > 0 ? (
          <select
            className={ctrl}
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
          >
            {topics.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        ) : (
          <input
            className={`${ctrl} font-mono`}
            value={topic}
            onChange={(e) => setTopic(e.target.value)}
          />
        )}
      </label>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Reset to
        <select
          className={ctrl}
          value={target}
          onChange={(e) => setTarget(e.target.value as Target)}
        >
          <option value="EARLIEST">Earliest</option>
          <option value="LATEST">Latest</option>
          <option value="OFFSET">Specific offset</option>
          <option value="TIMESTAMP">Timestamp</option>
        </select>
      </label>
      {target === 'OFFSET' && (
        <label className="flex flex-col gap-1 text-xs text-slate-600">
          Offset
          <input
            type="number"
            min={0}
            className={ctrl}
            value={offset}
            onChange={(e) => setOffset(e.target.value)}
          />
        </label>
      )}
      {target === 'TIMESTAMP' && (
        <label className="flex flex-col gap-1 text-xs text-slate-600">
          Timestamp
          <input
            type="datetime-local"
            className={ctrl}
            value={timestamp}
            onChange={(e) => setTimestamp(e.target.value)}
          />
        </label>
      )}
    </Modal>
  )
}
