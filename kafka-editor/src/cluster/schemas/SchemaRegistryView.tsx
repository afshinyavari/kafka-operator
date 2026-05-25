import { useState } from 'react'
import { Plus, RefreshCw, Trash2 } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'
import { EmptyState } from '../../components/EmptyState'
import {
  deleteArtifact,
  getArtifactContent,
  getArtifactVersions,
  searchArtifacts,
  updateArtifact,
} from '../../api/apicurioClient'
import type { RegistryArtifact } from '../../api/apicurioClient'
import { CreateSchemaModal } from './CreateSchemaModal'

interface SchemaRegistryViewProps {
  cluster: SavedCluster
}

/** Browse and manage schema-registry artifacts for the active cluster. */
export function SchemaRegistryView({ cluster }: SchemaRegistryViewProps) {
  const registryUrl = cluster.schemaRegistryUrl
  const [reload, setReload] = useState(0)
  const [selected, setSelected] = useState<RegistryArtifact | null>(null)
  const [showCreate, setShowCreate] = useState(false)

  const list = useAdminQuery(
    `schemas:${cluster.id}:${reload}`,
    () => searchArtifacts(registryUrl),
  )

  if (!registryUrl.trim()) {
    return (
      <EmptyState
        title="No schema registry configured"
        hint="Add a Schema Registry URL to this cluster (Manage clusters) to browse and manage schemas."
      />
    )
  }

  return (
    <div className="flex flex-1 overflow-hidden">
      <div className="flex w-72 shrink-0 flex-col border-r border-slate-200">
        <div className="flex items-center justify-between border-b border-slate-200 px-3 py-2">
          <h2 className="text-xs font-semibold text-slate-700">
            Schemas
            {list.data && (
              <span className="ml-1 font-normal text-slate-400">
                ({list.data.length})
              </span>
            )}
          </h2>
          <div className="flex gap-1">
            <button
              type="button"
              onClick={() => setReload((n) => n + 1)}
              aria-label="Refresh"
              className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
            >
              <RefreshCw className="h-3 w-3" />
            </button>
            <button
              type="button"
              onClick={() => setShowCreate(true)}
              className="flex items-center gap-1 rounded border border-emerald-700 bg-emerald-600 px-1.5 py-0.5 text-[11px] font-medium text-white hover:bg-emerald-700"
            >
              <Plus className="h-3 w-3" />
              New
            </button>
          </div>
        </div>
        <div className="flex-1 overflow-y-auto">
          {list.error && (
            <ErrorBanner
              message={list.error}
              onRetry={() => setReload((n) => n + 1)}
            />
          )}
          {list.loading && !list.data && (
            <p className="p-3 text-xs text-slate-400">Loading…</p>
          )}
          {list.data?.length === 0 && (
            <p className="p-3 text-xs text-slate-400">No schemas.</p>
          )}
          {list.data?.map((a) => (
            <button
              key={`${a.groupId}/${a.artifactId}`}
              type="button"
              onClick={() => setSelected(a)}
              className={`flex w-full flex-col items-start gap-0.5 border-b border-slate-100 px-3 py-1.5 text-left hover:bg-slate-50 ${
                selected?.artifactId === a.artifactId ? 'bg-slate-100' : ''
              }`}
            >
              <span className="font-mono text-xs text-slate-800">
                {a.artifactId}
              </span>
              <span className="text-[10px] text-slate-400">{a.type}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="flex flex-1 flex-col overflow-hidden">
        {selected ? (
          <SchemaDetail
            key={`${selected.groupId}/${selected.artifactId}`}
            registryUrl={registryUrl}
            artifact={selected}
            onDeleted={() => {
              setSelected(null)
              setReload((n) => n + 1)
            }}
          />
        ) : (
          <EmptyState
            title="Select a schema"
            hint="Pick an artifact to view its content and versions."
          />
        )}
      </div>

      {showCreate && (
        <CreateSchemaModal
          registryUrl={registryUrl}
          onClose={() => setShowCreate(false)}
          onCreated={() => setReload((n) => n + 1)}
        />
      )}
    </div>
  )
}

interface SchemaDetailProps {
  registryUrl: string
  artifact: RegistryArtifact
  onDeleted: () => void
}

function SchemaDetail({ registryUrl, artifact, onDeleted }: SchemaDetailProps) {
  const [reload, setReload] = useState(0)
  const [editing, setEditing] = useState(false)
  const [draft, setDraft] = useState('')
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const detail = useAdminQuery(
    `schema:${artifact.groupId}/${artifact.artifactId}:${reload}`,
    () =>
      Promise.all([
        getArtifactContent(registryUrl, artifact.groupId, artifact.artifactId),
        getArtifactVersions(registryUrl, artifact.groupId, artifact.artifactId),
      ]),
  )
  const [content, versions] = detail.data ?? ['', []]

  const saveVersion = async () => {
    setSaving(true)
    setError(null)
    try {
      await updateArtifact(
        registryUrl,
        artifact.groupId,
        artifact.artifactId,
        draft,
      )
      setEditing(false)
      setReload((n) => n + 1)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  const remove = async () => {
    if (!window.confirm(`Delete schema "${artifact.artifactId}"?`)) return
    try {
      await deleteArtifact(
        registryUrl,
        artifact.groupId,
        artifact.artifactId,
      )
      onDeleted()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  return (
    <>
      <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
        <h2 className="font-mono text-sm font-semibold text-slate-800">
          {artifact.artifactId}
          <span className="ml-1.5 rounded bg-slate-100 px-1 py-0.5 text-[10px] font-normal text-slate-500">
            {artifact.type}
          </span>
        </h2>
        <div className="flex gap-2">
          {editing ? (
            <>
              <button
                type="button"
                onClick={() => setEditing(false)}
                className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={saveVersion}
                disabled={saving}
                className="rounded border border-emerald-700 bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {saving ? 'Saving…' : 'Save new version'}
              </button>
            </>
          ) : (
            <>
              <button
                type="button"
                onClick={() => {
                  setDraft(content)
                  setEditing(true)
                }}
                className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
              >
                New version
              </button>
              <button
                type="button"
                onClick={remove}
                className="flex items-center gap-1 rounded border border-rose-300 px-2 py-1 text-xs text-rose-600 hover:bg-rose-50"
              >
                <Trash2 className="h-3.5 w-3.5" />
                Delete
              </button>
            </>
          )}
        </div>
      </div>
      <div className="flex-1 overflow-y-auto p-4">
        {error && <ErrorBanner message={error} />}
        {detail.error && <ErrorBanner message={detail.error} />}
        {detail.loading && !detail.data && (
          <p className="text-xs text-slate-400">Loading…</p>
        )}
        {detail.data && (
          <div className="flex flex-col gap-3">
            <p className="text-[11px] text-slate-400">
              Versions: {versions.join(', ') || '—'}
            </p>
            {editing ? (
              <textarea
                className="h-96 w-full rounded border border-slate-300 p-2 font-mono text-xs text-slate-800 outline-none focus:border-slate-500"
                value={draft}
                onChange={(e) => setDraft(e.target.value)}
              />
            ) : (
              <pre className="overflow-auto rounded border border-slate-200 bg-slate-50 p-3 font-mono text-[11px] whitespace-pre-wrap text-slate-700">
                {content}
              </pre>
            )}
          </div>
        )}
      </div>
    </>
  )
}
