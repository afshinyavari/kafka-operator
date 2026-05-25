import { useId } from 'react'
import { Plus, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import {
  AGGREGATE_OPS,
  newAggregateField,
  opReadsField,
} from '../model/aggregations'
import type { AggregateField, AggregateOp } from '../model/aggregations'

const fieldCls =
  'rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

interface AggregationBuilderProps {
  nodeId: string
  configKey: string
  fields: AggregateField[]
  fieldSuggestions: string[]
}

/** Inspector editor for an aggregate node's accumulator (a list of ops). */
export function AggregationBuilder({
  nodeId,
  configKey,
  fields,
  fieldSuggestions,
}: AggregationBuilderProps) {
  const updateNodeConfig = useEditorStore((s) => s.updateNodeConfig)
  const listId = useId()
  const commit = (next: AggregateField[]) =>
    updateNodeConfig(nodeId, { [configKey]: next })

  const update = (id: string, patch: Partial<AggregateField>) =>
    commit(fields.map((f) => (f.id === id ? { ...f, ...patch } : f)))
  const remove = (id: string) => commit(fields.filter((f) => f.id !== id))
  const add = () => commit([...fields, newAggregateField()])

  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-600">Accumulator</span>

      {fieldSuggestions.length > 0 && (
        <datalist id={listId}>
          {fieldSuggestions.map((path) => (
            <option key={path} value={path} />
          ))}
        </datalist>
      )}

      {fields.length === 0 && (
        <span className="text-[11px] text-slate-400">
          No accumulator fields yet.
        </span>
      )}

      {fields.map((field) => (
        <div
          key={field.id}
          className="flex flex-col gap-1 rounded border border-slate-200 bg-slate-50 p-2"
        >
          <div className="flex items-center gap-1">
            <input
              className={`${fieldCls} min-w-0 flex-1`}
              value={field.name}
              placeholder="accumulator field"
              onChange={(e) => update(field.id, { name: e.target.value })}
            />
            <select
              className={fieldCls}
              value={field.op}
              onChange={(e) =>
                update(field.id, { op: e.target.value as AggregateOp })
              }
            >
              {AGGREGATE_OPS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => remove(field.id)}
              aria-label="Remove accumulator field"
              className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
          {opReadsField(field.op) && (
            <input
              className={fieldCls}
              list={listId}
              value={field.sourceField}
              placeholder="input field"
              onChange={(e) =>
                update(field.id, { sourceField: e.target.value })
              }
            />
          )}
        </div>
      ))}

      <button
        type="button"
        onClick={add}
        className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
      >
        <Plus className="h-3.5 w-3.5" />
        Add accumulator field
      </button>
    </div>
  )
}
