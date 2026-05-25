import { useEffect } from 'react'
import { Plus, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { useEnvVarStore } from '../run/envStore'

interface SettingsModalProps {
  onClose: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500'

/** Project environment settings — Kafka cluster, schema registry, env vars. */
export function SettingsModal({ onClose }: SettingsModalProps) {
  const environment = useEditorStore((s) => s.environment)
  const setEnvironment = useEditorStore((s) => s.setEnvironment)
  const envVars = useEnvVarStore((s) => s.envVars)
  const addEnvVar = useEnvVarStore((s) => s.addEnvVar)
  const updateEnvVar = useEnvVarStore((s) => s.updateEnvVar)
  const removeEnvVar = useEnvVarStore((s) => s.removeEnvVar)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-6"
      onClick={onClose}
    >
      <div
        className="flex max-h-[80vh] w-[480px] flex-col rounded-lg bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <h2 className="text-sm font-semibold text-slate-800">Settings</h2>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-4 w-4" />
          </button>
        </header>

        <div className="flex flex-col gap-5 overflow-y-auto p-4">
          <section className="flex flex-col gap-1.5">
            <h3 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
              Kafka cluster
            </h3>
            <label className="flex flex-col gap-1 text-xs text-slate-600">
              <span>Bootstrap servers</span>
              <input
                className={inputCls}
                value={environment.bootstrapServers}
                placeholder="localhost:9092"
                onChange={(e) =>
                  setEnvironment({
                    ...environment,
                    bootstrapServers: e.target.value,
                  })
                }
              />
            </label>
          </section>

          <section className="flex flex-col gap-1.5">
            <h3 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
              Schema Registry
            </h3>
            <label className="flex flex-col gap-1 text-xs text-slate-600">
              <span>Registry URL</span>
              <input
                className={inputCls}
                value={environment.schemaRegistryUrl}
                placeholder="http://localhost:8085"
                onChange={(e) =>
                  setEnvironment({
                    ...environment,
                    schemaRegistryUrl: e.target.value,
                  })
                }
              />
            </label>
            <p className="text-[11px] text-slate-400">
              Used by live runs (Avro deserialization) and the “Browse
              Apicurio” schema importer.
            </p>
          </section>

          <section className="flex flex-col gap-1.5">
            <h3 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
              Environment variables
            </h3>
            <p className="text-[11px] text-slate-400">
              Local to this browser only — never saved with the project or
              exported. Sent with each run as extra Kafka / registry client
              config.
            </p>
            {envVars.length === 0 && (
              <span className="text-[11px] text-slate-400">
                No variables defined.
              </span>
            )}
            {envVars.map((variable) => (
              <div key={variable.id} className="flex items-center gap-1">
                <input
                  className={`${inputCls} font-mono`}
                  value={variable.key}
                  placeholder="KEY"
                  onChange={(e) =>
                    updateEnvVar(variable.id, { key: e.target.value })
                  }
                />
                <input
                  className={`${inputCls} font-mono`}
                  value={variable.value}
                  placeholder="value"
                  onChange={(e) =>
                    updateEnvVar(variable.id, { value: e.target.value })
                  }
                />
                <button
                  type="button"
                  onClick={() => removeEnvVar(variable.id)}
                  aria-label="Remove variable"
                  className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
                >
                  <X className="h-3 w-3" />
                </button>
              </div>
            ))}
            <button
              type="button"
              onClick={addEnvVar}
              className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
            >
              <Plus className="h-3.5 w-3.5" />
              Add variable
            </button>
          </section>
        </div>
      </div>
    </div>
  )
}
