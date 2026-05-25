import { useEffect, useState } from 'react'
import { Play, Square, X } from 'lucide-react'
import type { ProjectDocument } from '../model/project'
import { useEditorStore } from '../state/store'
import { useRunStore } from './runStore'
import type { RunMode } from './runStore'

interface RunDialogProps {
  buildDocument: () => ProjectDocument
  onClose: () => void
}

const inputCls =
  'rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500'

/** Dialog to start a test run or a live run of the topology. */
export function RunDialog({ buildDocument, onClose }: RunDialogProps) {
  const environment = useEditorStore((s) => s.environment)
  const status = useRunStore((s) => s.status)
  const runMode = useRunStore((s) => s.mode)
  const error = useRunStore((s) => s.error)
  const metrics = useRunStore((s) => s.metrics)
  const runTest = useRunStore((s) => s.runTest)
  const startLive = useRunStore((s) => s.startLive)
  const stopLive = useRunStore((s) => s.stopLive)

  const liveActive = runMode === 'live' && status === 'running'

  const [mode, setMode] = useState<RunMode>(liveActive ? 'live' : 'test')
  const [records, setRecords] = useState(20)
  const [generateInput, setGenerateInput] = useState(true)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const busy = status === 'running'
  const hasBroker = environment.bootstrapServers.trim().length > 0
  const nodesProcessed = Object.keys(metrics).length

  const start = () => {
    if (mode === 'test') {
      runTest(buildDocument(), records)
    } else {
      startLive(buildDocument(), generateInput)
    }
  }

  const modeButton = (value: RunMode, label: string) => (
    <button
      type="button"
      disabled={liveActive}
      onClick={() => setMode(value)}
      className={`flex-1 rounded border px-2 py-1 text-xs font-medium disabled:opacity-50 ${
        mode === value
          ? 'border-slate-700 bg-slate-700 text-white'
          : 'border-slate-300 text-slate-600 hover:bg-slate-100'
      }`}
    >
      {label}
    </button>
  )

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-6"
      onClick={onClose}
    >
      <div
        className="flex w-[460px] flex-col rounded-lg bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <h2 className="text-sm font-semibold text-slate-800">Run Topology</h2>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-4 w-4" />
          </button>
        </header>

        <div className="flex flex-col gap-3 p-4">
          <div className="flex gap-1">
            {modeButton('test', 'Test')}
            {modeButton('live', 'Live cluster')}
          </div>

          {mode === 'test' ? (
            <>
              <p className="text-[11px] text-slate-400">
                Runs the topology in-memory on the backend via Kafka Streams'
                TopologyTestDriver — generated sample records are fed into each
                source. No broker needed.
              </p>
              <label className="flex items-center justify-between gap-2 text-xs text-slate-600">
                <span>Records per source</span>
                <input
                  type="number"
                  min={1}
                  max={10000}
                  value={records}
                  onChange={(e) => setRecords(Number(e.target.value) || 1)}
                  className={`${inputCls} w-24`}
                />
              </label>
            </>
          ) : (
            <>
              <p className="text-[11px] text-slate-400">
                Runs a real Kafka Streams application against the cluster;
                per-node metrics stream live onto the canvas. Source records
                are decoded via the schema registry.
              </p>
              <div className="rounded border border-slate-200 bg-slate-50 px-2 py-1.5 text-[11px] text-slate-600">
                <div>
                  <span className="text-slate-400">Kafka:</span>{' '}
                  {environment.bootstrapServers || '(not set)'}
                </div>
                <div>
                  <span className="text-slate-400">Schema Registry:</span>{' '}
                  {environment.schemaRegistryUrl || '(none)'}
                </div>
                <div className="mt-0.5 text-slate-400">
                  Configure these in Settings.
                </div>
              </div>
              <label className="flex items-center gap-2 text-xs text-slate-600">
                <input
                  type="checkbox"
                  checked={generateInput}
                  onChange={(e) => setGenerateInput(e.target.checked)}
                  disabled={liveActive}
                  className="h-3.5 w-3.5"
                />
                <span>Feed generated test data into the source topics</span>
              </label>
            </>
          )}

          {liveActive ? (
            <button
              type="button"
              onClick={stopLive}
              className="flex items-center justify-center gap-1.5 rounded border border-rose-600 bg-rose-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-rose-700"
            >
              <Square className="h-4 w-4" />
              Stop run
            </button>
          ) : (
            <button
              type="button"
              onClick={start}
              disabled={busy || (mode === 'live' && !hasBroker)}
              className="flex items-center justify-center gap-1.5 rounded border border-emerald-700 bg-emerald-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
            >
              <Play className="h-4 w-4" />
              {busy ? 'Starting…' : 'Run'}
            </button>
          )}

          {mode === 'live' && !hasBroker && !liveActive && (
            <p className="rounded bg-amber-50 px-2 py-1.5 text-[11px] text-amber-700">
              Set the Kafka bootstrap servers in Settings to run live.
            </p>
          )}
          {liveActive && (
            <p className="rounded bg-emerald-50 px-2 py-1.5 text-[11px] text-emerald-700">
              ● Live — metrics are streaming onto the canvas.
            </p>
          )}
          {status === 'done' && (
            <p className="rounded bg-emerald-50 px-2 py-1.5 text-[11px] text-emerald-700">
              {runMode === 'live'
                ? 'Run stopped. The final counts remain on the canvas.'
                : `Run complete — ${nodesProcessed} node${
                    nodesProcessed === 1 ? '' : 's'
                  } processed records. See the counts on the canvas.`}
            </p>
          )}
          {status === 'error' && (
            <p className="rounded bg-rose-50 px-2 py-1.5 text-[11px] text-rose-600">
              {error}
            </p>
          )}
        </div>
      </div>
    </div>
  )
}
