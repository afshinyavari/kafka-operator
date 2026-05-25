import { useEffect, useState } from 'react'
import { nanoid } from 'nanoid'
import { Download, RefreshCw, Search } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { getArtifactContent, searchArtifacts } from '../api/apicurioClient'
import type { RegistryArtifact } from '../api/apicurioClient'
import { parseSchema } from '../model/schemaParser'
import type { SchemaFormat } from '../model/recordTypes'

/** Apicurio artifact types the importer can parse. */
const SUPPORTED: Record<string, SchemaFormat> = { AVRO: 'AVRO', JSON: 'JSON' }

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500'

/** Record Types modal tab: search the schema registry and import schemas. */
export function ApicurioBrowser() {
  const addRecordType = useEditorStore((s) => s.addRecordType)
  const registryUrl = useEditorStore((s) => s.environment.schemaRegistryUrl)
  const [query, setQuery] = useState('')
  const [artifacts, setArtifacts] = useState<RegistryArtifact[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [message, setMessage] = useState('')

  const load = async (name: string) => {
    setLoading(true)
    setError('')
    try {
      setArtifacts(await searchArtifacts(registryUrl, name))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setArtifacts([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    if (registryUrl.trim()) {
      // eslint-disable-next-line react-hooks/set-state-in-effect -- initial load on open
      void load('')
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [registryUrl])

  const importArtifact = async (artifact: RegistryArtifact) => {
    const format = SUPPORTED[artifact.type]
    if (!format) return
    setError('')
    setMessage('')
    try {
      const raw = await getArtifactContent(
        registryUrl,
        artifact.groupId,
        artifact.artifactId,
      )
      const parsed = parseSchema(format, raw)
      addRecordType({
        id: nanoid(),
        name: parsed.name || artifact.artifactId,
        fields: parsed.fields,
        source: {
          kind: 'apicurio',
          groupId: artifact.groupId,
          artifactId: artifact.artifactId,
          format,
          importedAt: new Date().toISOString(),
          rawSchema: raw,
        },
      })
      const warn = parsed.warnings.length
        ? ` (${parsed.warnings.length} warning${parsed.warnings.length === 1 ? '' : 's'})`
        : ''
      setMessage(
        `Imported “${parsed.name}” with ${parsed.fields.length} field${
          parsed.fields.length === 1 ? '' : 's'
        }${warn}. See it under My Types.`,
      )
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  if (!registryUrl.trim()) {
    return (
      <p className="py-6 text-center text-sm text-slate-400">
        No schema registry configured. Set a Registry URL in{' '}
        <span className="font-medium text-slate-600">Settings</span> to browse
        and import schemas.
      </p>
    )
  }

  return (
    <div className="flex flex-col gap-2">
      <p className="text-[11px] text-slate-400">
        Import Avro or JSON Schema artifacts from{' '}
        <span className="font-mono">{registryUrl}</span> as record types.
      </p>

      <div className="flex gap-1">
        <input
          className={inputCls}
          value={query}
          placeholder="Search artifacts…"
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') void load(query)
          }}
        />
        <button
          type="button"
          onClick={() => void load(query)}
          className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
        >
          <Search className="h-3.5 w-3.5" />
          Search
        </button>
        <button
          type="button"
          onClick={() => void load(query)}
          aria-label="Refresh"
          className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
        >
          <RefreshCw className="h-3.5 w-3.5" />
        </button>
      </div>

      {loading && <p className="text-[11px] text-slate-400">Loading…</p>}
      {error && (
        <p className="rounded bg-rose-50 px-2 py-1 text-[11px] text-rose-600">
          {error}
        </p>
      )}
      {message && (
        <p className="rounded bg-emerald-50 px-2 py-1 text-[11px] text-emerald-700">
          {message}
        </p>
      )}
      {!loading && !error && artifacts.length === 0 && (
        <p className="py-4 text-center text-sm text-slate-400">
          No artifacts found.
        </p>
      )}

      {artifacts.map((artifact) => {
        const supported = Boolean(SUPPORTED[artifact.type])
        return (
          <div
            key={`${artifact.groupId}/${artifact.artifactId}`}
            className="flex items-center justify-between rounded border border-slate-200 px-3 py-2"
          >
            <div className="min-w-0">
              <div className="truncate text-sm font-medium text-slate-800">
                {artifact.name || artifact.artifactId}
              </div>
              <div className="text-[11px] text-slate-400">
                {artifact.groupId} · {artifact.type}
              </div>
            </div>
            <button
              type="button"
              disabled={!supported}
              onClick={() => void importArtifact(artifact)}
              className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100 disabled:cursor-not-allowed disabled:opacity-40"
            >
              <Download className="h-3.5 w-3.5" />
              {supported ? 'Import' : 'Unsupported'}
            </button>
          </div>
        )
      })}
    </div>
  )
}
