import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { produceAvro, produceMessage } from '../../api/adminClient'
import {
  getArtifactContent,
  getArtifactMeta,
  searchArtifacts,
} from '../../api/apicurioClient'
import type { RegistryArtifact } from '../../api/apicurioClient'
import type { ClusterConnection } from '../clusterStore'
import { useAdminQuery } from '../clusterSelectors'
import { parseConfigText } from '../topics/topicConfig'

interface ProduceModalProps {
  conn: ClusterConnection
  topic: string
  onClose: () => void
  onProduced: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'
const sendBtn =
  'rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50'
const closeBtn =
  'rounded border border-slate-300 px-3 py-1 text-xs text-slate-600 hover:bg-slate-100'

/** Produce a record — raw text/JSON, or schema-aware Avro. */
export function ProduceModal({
  conn,
  topic,
  onClose,
  onProduced,
}: ProduceModalProps) {
  const [mode, setMode] = useState<'raw' | 'schema'>('raw')

  return (
    <Modal title={`Produce to ${topic}`} onClose={onClose} width="w-[560px]">
      <div className="flex self-start overflow-hidden rounded border border-slate-300 text-xs">
        {(['raw', 'schema'] as const).map((m) => (
          <button
            key={m}
            type="button"
            onClick={() => setMode(m)}
            className={
              mode === m
                ? 'bg-slate-800 px-3 py-1 text-white'
                : 'px-3 py-1 text-slate-600 hover:bg-slate-100'
            }
          >
            {m === 'raw' ? 'Raw' : 'Schema (Avro)'}
          </button>
        ))}
      </div>
      {mode === 'raw' ? (
        <RawProduceForm
          conn={conn}
          topic={topic}
          onClose={onClose}
          onProduced={onProduced}
        />
      ) : (
        <SchemaProduceForm
          conn={conn}
          topic={topic}
          onClose={onClose}
          onProduced={onProduced}
        />
      )}
    </Modal>
  )
}

function RawProduceForm({ conn, topic, onClose, onProduced }: ProduceModalProps) {
  const [key, setKey] = useState('')
  const [value, setValue] = useState('')
  const [valueType, setValueType] = useState<'string' | 'json'>('string')
  const [tombstone, setTombstone] = useState(false)
  const [headerText, setHeaderText] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [sent, setSent] = useState<string | null>(null)

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    setSent(null)
    try {
      const result = await produceMessage(conn, {
        topic,
        key: key || undefined,
        value: tombstone ? undefined : value,
        valueType,
        tombstone,
        headers: parseConfigText(headerText),
      })
      setSent(`Sent to partition ${result.partition}, offset ${result.offset}.`)
      onProduced()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <>
      {error && <ErrorBanner message={error} />}
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Key (optional)
        <input
          className={`${inputCls} font-mono`}
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
      </label>
      <label className="flex items-center gap-1.5 text-xs text-slate-600">
        <input
          type="checkbox"
          checked={tombstone}
          onChange={(e) => setTombstone(e.target.checked)}
        />
        Send a tombstone (null value)
      </label>
      {!tombstone && (
        <>
          <div className="flex items-center gap-3 text-xs text-slate-600">
            <span>Value type:</span>
            {(['string', 'json'] as const).map((t) => (
              <label key={t} className="flex items-center gap-1">
                <input
                  type="radio"
                  checked={valueType === t}
                  onChange={() => setValueType(t)}
                />
                {t}
              </label>
            ))}
          </div>
          <label className="flex flex-col gap-1 text-xs text-slate-600">
            Value
            <textarea
              className={`${inputCls} h-24 font-mono`}
              value={value}
              onChange={(e) => setValue(e.target.value)}
            />
          </label>
        </>
      )}
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Headers (optional)
        <textarea
          className={`${inputCls} h-14 font-mono`}
          value={headerText}
          onChange={(e) => setHeaderText(e.target.value)}
          placeholder="source=ui"
        />
      </label>
      <FormActions
        sent={sent}
        submitting={submitting}
        onClose={onClose}
        onSubmit={submit}
      />
    </>
  )
}

function SchemaProduceForm({
  conn,
  topic,
  onClose,
  onProduced,
}: ProduceModalProps) {
  const [artifact, setArtifact] = useState<RegistryArtifact | null>(null)
  const [key, setKey] = useState('')
  const [jsonValue, setJsonValue] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [sent, setSent] = useState<string | null>(null)

  const artifacts = useAdminQuery('produce-artifacts', () =>
    searchArtifacts(conn.schemaRegistryUrl),
  )
  const schema = useAdminQuery(
    `produce-schema:${artifact?.artifactId ?? ''}`,
    () =>
      artifact
        ? Promise.all([
            getArtifactContent(
              conn.schemaRegistryUrl,
              artifact.groupId,
              artifact.artifactId,
            ),
            getArtifactMeta(
              conn.schemaRegistryUrl,
              artifact.groupId,
              artifact.artifactId,
            ),
          ])
        : Promise.resolve(null),
  )

  const submit = async () => {
    if (!schema.data) return
    setSubmitting(true)
    setError(null)
    setSent(null)
    try {
      const [content, meta] = schema.data
      const result = await produceAvro(conn, {
        topic,
        key: key || undefined,
        jsonValue,
        schema: content,
        globalId: meta.globalId,
      })
      setSent(`Sent to partition ${result.partition}, offset ${result.offset}.`)
      onProduced()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSubmitting(false)
    }
  }

  if (!conn.schemaRegistryUrl.trim()) {
    return (
      <p className="text-xs text-slate-400">
        No schema registry is configured for this cluster.
      </p>
    )
  }

  return (
    <>
      {error && <ErrorBanner message={error} />}
      {artifacts.error && <ErrorBanner message={artifacts.error} />}
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Schema
        <select
          className={inputCls}
          value={artifact?.artifactId ?? ''}
          onChange={(e) =>
            setArtifact(
              artifacts.data?.find(
                (a) => a.artifactId === e.target.value,
              ) ?? null,
            )
          }
        >
          <option value="">Select an artifact…</option>
          {artifacts.data?.map((a) => (
            <option key={a.artifactId} value={a.artifactId}>
              {a.artifactId} ({a.type})
            </option>
          ))}
        </select>
      </label>
      {schema.data && (
        <details className="text-xs text-slate-500">
          <summary className="cursor-pointer">View schema</summary>
          <pre className="mt-1 max-h-40 overflow-auto rounded border border-slate-200 bg-slate-50 p-2 font-mono text-[11px] whitespace-pre-wrap">
            {schema.data[0]}
          </pre>
        </details>
      )}
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Key (optional)
        <input
          className={`${inputCls} font-mono`}
          value={key}
          onChange={(e) => setKey(e.target.value)}
        />
      </label>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Value (Avro JSON encoding)
        <textarea
          className={`${inputCls} h-32 font-mono`}
          value={jsonValue}
          onChange={(e) => setJsonValue(e.target.value)}
          placeholder={'{ "field": "value" }'}
        />
      </label>
      <FormActions
        sent={sent}
        submitting={submitting}
        disabled={!schema.data}
        onClose={onClose}
        onSubmit={submit}
      />
    </>
  )
}

function FormActions({
  sent,
  submitting,
  disabled,
  onClose,
  onSubmit,
}: {
  sent: string | null
  submitting: boolean
  disabled?: boolean
  onClose: () => void
  onSubmit: () => void
}) {
  return (
    <div className="flex items-center gap-2 border-t border-slate-200 pt-3">
      {sent && (
        <span className="mr-auto text-[11px] text-emerald-600">{sent}</span>
      )}
      <button
        type="button"
        onClick={onClose}
        className={`${sent ? '' : 'ml-auto'} ${closeBtn}`}
      >
        Close
      </button>
      <button
        type="button"
        onClick={onSubmit}
        disabled={submitting || disabled}
        className={sendBtn}
      >
        {submitting ? 'Sending…' : 'Send'}
      </button>
    </div>
  )
}
