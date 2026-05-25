import { Plus, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { newConfigEntry } from '../model/connectors'
import type { ConnectorConfigEntry } from '../model/connectors'

const fieldCls =
  'min-w-0 flex-1 rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

interface KeyValueEditorProps {
  nodeId: string
  configKey: string
  label: string
  entries: ConnectorConfigEntry[]
}

/** Inspector editor for an arbitrary key/value map (Kafka Connect config). */
export function KeyValueEditor({
  nodeId,
  configKey,
  label,
  entries,
}: KeyValueEditorProps) {
  const updateNodeConfig = useEditorStore((s) => s.updateNodeConfig)
  const commit = (next: ConnectorConfigEntry[]) =>
    updateNodeConfig(nodeId, { [configKey]: next })

  const update = (id: string, patch: Partial<ConnectorConfigEntry>) =>
    commit(entries.map((e) => (e.id === id ? { ...e, ...patch } : e)))
  const remove = (id: string) => commit(entries.filter((e) => e.id !== id))
  const add = () => commit([...entries, newConfigEntry()])

  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-600">{label}</span>

      {entries.length === 0 && (
        <span className="text-[11px] text-slate-400">No properties yet.</span>
      )}

      {entries.map((entry) => (
        <div key={entry.id} className="flex items-center gap-1">
          <input
            className={fieldCls}
            value={entry.key}
            placeholder="property"
            onChange={(e) => update(entry.id, { key: e.target.value })}
          />
          <input
            className={fieldCls}
            value={entry.value}
            placeholder="value"
            onChange={(e) => update(entry.id, { value: e.target.value })}
          />
          <button
            type="button"
            onClick={() => remove(entry.id)}
            aria-label="Remove property"
            className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-3 w-3" />
          </button>
        </div>
      ))}

      <button
        type="button"
        onClick={add}
        className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
      >
        <Plus className="h-3.5 w-3.5" />
        Add property
      </button>
    </div>
  )
}
