import { useState } from 'react'
import { ArrowLeft, Pause, Play, RotateCw, Trash2 } from 'lucide-react'
import {
  deleteConnector,
  pauseConnector,
  restartConnector,
  resumeConnector,
} from '../../api/adminClient'
import type { ConnectorEntry } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'
import { ErrorBanner } from '../../components/ErrorBanner'
import { stateColor } from './connectorState'

interface ConnectorDetailProps {
  conn: ClusterConnection
  name: string
  entry: ConnectorEntry
  onBack: () => void
  onChanged: () => void
}

const actionBtn =
  'flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100 disabled:opacity-50'

/** One connector — status, tasks, config and lifecycle actions. */
export function ConnectorDetail({
  conn,
  name,
  entry,
  onBack,
  onChanged,
}: ConnectorDetailProps) {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const status = entry.status
  const config = entry.info?.config ?? {}
  const tasks = status?.tasks ?? []

  const run = async (action: () => Promise<unknown>, back = false) => {
    setBusy(true)
    setError(null)
    try {
      await action()
      if (back) {
        onBack()
      } else {
        onChanged()
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-2 border-b border-slate-200 px-4 py-2">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onBack}
            aria-label="Back"
            className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
          </button>
          <h2 className="font-mono text-sm font-semibold text-slate-800">
            {name}
          </h2>
          {status?.connector?.state && (
            <span
              className={`rounded px-1.5 py-0.5 text-[10px] ${stateColor(
                status.connector.state,
              )}`}
            >
              {status.connector.state.toLowerCase()}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            disabled={busy}
            onClick={() => run(() => restartConnector(conn, name))}
            className={actionBtn}
          >
            <RotateCw className="h-3.5 w-3.5" />
            Restart
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => run(() => pauseConnector(conn, name))}
            className={actionBtn}
          >
            <Pause className="h-3.5 w-3.5" />
            Pause
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => run(() => resumeConnector(conn, name))}
            className={actionBtn}
          >
            <Play className="h-3.5 w-3.5" />
            Resume
          </button>
          <button
            type="button"
            disabled={busy}
            onClick={() => {
              if (window.confirm(`Delete connector "${name}"?`)) {
                void run(() => deleteConnector(conn, name), true)
              }
            }}
            className="flex items-center gap-1 rounded border border-rose-300 px-2 py-1 text-xs text-rose-600 hover:bg-rose-50 disabled:opacity-50"
          >
            <Trash2 className="h-3.5 w-3.5" />
            Delete
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4">
        {error && <ErrorBanner message={error} />}
        <section className="mb-5">
          <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
            Tasks ({tasks.length})
          </h3>
          {tasks.length === 0 ? (
            <p className="text-xs text-slate-400">No tasks.</p>
          ) : (
            <table className="w-full border-collapse text-xs">
              <thead>
                <tr className="border-b border-slate-200 text-left text-slate-500">
                  <th className="py-1.5 pr-3 font-medium">Task</th>
                  <th className="py-1.5 pr-3 font-medium">State</th>
                  <th className="py-1.5 font-medium">Worker / trace</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((t) => (
                  <tr key={t.id} className="border-b border-slate-100">
                    <td className="py-1.5 pr-3 font-mono">{t.id}</td>
                    <td className="py-1.5 pr-3">
                      <span
                        className={`rounded px-1.5 py-0.5 text-[10px] ${stateColor(
                          t.state ?? '',
                        )}`}
                      >
                        {(t.state ?? 'unknown').toLowerCase()}
                      </span>
                    </td>
                    <td className="py-1.5 font-mono break-all text-slate-500">
                      {t.trace
                        ? t.trace.split('\n')[0]
                        : (status?.connector?.worker_id ?? '')}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>

        <section>
          <h3 className="mb-1.5 text-xs font-semibold tracking-wide text-slate-500 uppercase">
            Config
          </h3>
          <table className="w-full border-collapse text-xs">
            <tbody>
              {Object.entries(config).map(([k, v]) => (
                <tr key={k} className="border-b border-slate-100">
                  <td className="py-1 pr-3 font-mono text-slate-600">{k}</td>
                  <td className="py-1 font-mono break-all text-slate-500">
                    {v}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  )
}
