import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { addPartitions } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'

interface AddPartitionsModalProps {
  conn: ClusterConnection
  topic: string
  currentCount: number
  onClose: () => void
  onDone: () => void
}

/** Increase a topic's partition count (Kafka only allows increases). */
export function AddPartitionsModal({
  conn,
  topic,
  currentCount,
  onClose,
  onDone,
}: AddPartitionsModalProps) {
  const [total, setTotal] = useState(currentCount + 1)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await addPartitions(conn, topic, total)
      onDone()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="Add partitions"
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
            disabled={submitting || total <= currentCount}
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Applying…' : 'Add partitions'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <p className="text-xs text-slate-600">
        <span className="font-mono">{topic}</span> currently has{' '}
        <strong>{currentCount}</strong> partition
        {currentCount === 1 ? '' : 's'}.
      </p>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        New total partition count
        <input
          type="number"
          min={currentCount + 1}
          className="w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500"
          value={total}
          onChange={(e) => setTotal(Number(e.target.value) || 0)}
        />
      </label>
      <p className="rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-[11px] text-amber-700">
        Adding partitions changes how keys map to partitions — keyed ordering
        guarantees only hold for messages produced afterwards.
      </p>
    </Modal>
  )
}
