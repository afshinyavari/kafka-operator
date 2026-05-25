import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { deleteTopic } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'

interface DeleteTopicModalProps {
  conn: ClusterConnection
  topic: string
  onClose: () => void
  onDeleted: () => void
}

/** Type-to-confirm deletion of a topic. */
export function DeleteTopicModal({
  conn,
  topic,
  onClose,
  onDeleted,
}: DeleteTopicModalProps) {
  const [confirm, setConfirm] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await deleteTopic(conn, topic)
      onDeleted()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="Delete topic"
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
            disabled={submitting || confirm !== topic}
            className="rounded border border-rose-700 bg-rose-600 px-3 py-1 text-xs font-medium text-white hover:bg-rose-700 disabled:opacity-50"
          >
            {submitting ? 'Deleting…' : 'Delete topic'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <p className="text-xs text-slate-600">
        This permanently deletes <span className="font-mono">{topic}</span> and
        all of its data. Type the topic name to confirm.
      </p>
      <input
        className="w-full rounded border border-slate-300 px-2 py-1 font-mono text-xs text-slate-800 outline-none focus:border-rose-500"
        value={confirm}
        placeholder={topic}
        autoFocus
        onChange={(e) => setConfirm(e.target.value)}
      />
    </Modal>
  )
}
