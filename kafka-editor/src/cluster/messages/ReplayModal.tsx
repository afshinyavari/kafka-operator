import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { replayMessage } from '../../api/adminClient'
import type { RenderedRecord } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'

interface ReplayModalProps {
  conn: ClusterConnection
  sourceTopic: string
  record: RenderedRecord
  onClose: () => void
  onReplayed: () => void
}

/** Re-produce an existing record (raw bytes + headers) to a chosen topic. */
export function ReplayModal({
  conn,
  sourceTopic,
  record,
  onClose,
  onReplayed,
}: ReplayModalProps) {
  const [targetTopic, setTargetTopic] = useState(sourceTopic)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await replayMessage(conn, {
        sourceTopic,
        partition: record.partition,
        offset: record.offset,
        targetTopic: targetTopic.trim(),
      })
      onReplayed()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="Replay message"
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
            disabled={submitting || targetTopic.trim().length === 0}
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Replaying…' : 'Replay'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <p className="text-xs text-slate-600">
        Re-produces the record at{' '}
        <span className="font-mono">
          {sourceTopic}-{record.partition}@{record.offset}
        </span>{' '}
        with its original bytes and headers preserved.
      </p>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Target topic
        <input
          className="w-full rounded border border-slate-300 px-2 py-1 font-mono text-xs text-slate-800 outline-none focus:border-slate-500"
          value={targetTopic}
          onChange={(e) => setTargetTopic(e.target.value)}
        />
      </label>
    </Modal>
  )
}
