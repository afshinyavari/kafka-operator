import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { createArtifact } from '../../api/apicurioClient'

interface CreateSchemaModalProps {
  registryUrl: string
  onClose: () => void
  onCreated: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

const TYPES = ['AVRO', 'JSON', 'PROTOBUF']

/** Create a new schema-registry artifact. */
export function CreateSchemaModal({
  registryUrl,
  onClose,
  onCreated,
}: CreateSchemaModalProps) {
  const [artifactId, setArtifactId] = useState('')
  const [type, setType] = useState('AVRO')
  const [content, setContent] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await createArtifact(
        registryUrl,
        'default',
        artifactId.trim(),
        type,
        content,
      )
      onCreated()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="New schema"
      onClose={onClose}
      width="w-[520px]"
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
            disabled={
              submitting ||
              artifactId.trim().length === 0 ||
              content.trim().length === 0
            }
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Creating…' : 'Create'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <div className="flex gap-2">
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Artifact id
          <input
            className={`${inputCls} font-mono`}
            value={artifactId}
            placeholder="orders-value"
            autoFocus
            onChange={(e) => setArtifactId(e.target.value)}
          />
        </label>
        <label className="flex w-28 flex-col gap-1 text-xs text-slate-600">
          Type
          <select
            className={inputCls}
            value={type}
            onChange={(e) => setType(e.target.value)}
          >
            {TYPES.map((t) => (
              <option key={t} value={t}>
                {t}
              </option>
            ))}
          </select>
        </label>
      </div>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Schema
        <textarea
          className={`${inputCls} h-48 font-mono`}
          value={content}
          onChange={(e) => setContent(e.target.value)}
          placeholder={'{ "type": "record", "name": "Order", "fields": [] }'}
        />
      </label>
    </Modal>
  )
}
