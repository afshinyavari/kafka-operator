import { Plus, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { newMappingEntry } from '../expressions/types'
import type { ValueExpression, ValueMappingEntry } from '../expressions/types'
import { ExpressionBuilder } from './ExpressionBuilder'

interface ValueMappingBuilderProps {
  nodeId: string
  configKey: string
  label: string
  entries: ValueMappingEntry[]
  fieldSuggestions: string[]
}

/** Inspector editor for a value mapping — the output value's fields. */
export function ValueMappingBuilder({
  nodeId,
  configKey,
  label,
  entries,
  fieldSuggestions,
}: ValueMappingBuilderProps) {
  const updateNodeConfig = useEditorStore((s) => s.updateNodeConfig)
  const commit = (next: ValueMappingEntry[]) =>
    updateNodeConfig(nodeId, { [configKey]: next })

  const updateEntry = (id: string, patch: Partial<ValueMappingEntry>) =>
    commit(entries.map((e) => (e.id === id ? { ...e, ...patch } : e)))
  const removeEntry = (id: string) =>
    commit(entries.filter((e) => e.id !== id))
  const addEntry = () => commit([...entries, newMappingEntry()])

  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-600">{label}</span>

      {entries.length === 0 && (
        <span className="text-[11px] text-slate-400">
          No output fields defined yet.
        </span>
      )}

      {entries.map((entry) => (
        <div
          key={entry.id}
          className="flex flex-col gap-1 rounded border border-slate-200 bg-slate-50 p-2"
        >
          <div className="flex items-center gap-1">
            <input
              className="min-w-0 flex-1 rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500"
              value={entry.outputField}
              placeholder="output field"
              onChange={(e) =>
                updateEntry(entry.id, { outputField: e.target.value })
              }
            />
            <span className="text-[10px] text-slate-400">←</span>
            <button
              type="button"
              onClick={() => removeEntry(entry.id)}
              aria-label="Remove output field"
              className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
          <ExpressionBuilder
            expression={entry.expression}
            fieldSuggestions={fieldSuggestions}
            onChange={(expr: ValueExpression) =>
              updateEntry(entry.id, { expression: expr })
            }
          />
        </div>
      ))}

      <button
        type="button"
        onClick={addEntry}
        className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
      >
        <Plus className="h-3.5 w-3.5" />
        Add output field
      </button>
    </div>
  )
}
