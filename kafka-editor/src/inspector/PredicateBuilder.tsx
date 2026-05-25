import { useId } from 'react'
import { Plus, X } from 'lucide-react'
import { newCondition, VALUELESS_OPERATORS } from '../expressions/types'
import type {
  Combinator,
  ComparisonOperator,
  Condition,
  Predicate,
} from '../expressions/types'

const OPERATORS: { value: ComparisonOperator; label: string }[] = [
  { value: 'eq', label: '== equals' },
  { value: 'neq', label: '!= not equals' },
  { value: 'gt', label: '> greater than' },
  { value: 'gte', label: '>= at least' },
  { value: 'lt', label: '< less than' },
  { value: 'lte', label: '<= at most' },
  { value: 'contains', label: 'contains' },
  { value: 'isNull', label: 'is null' },
  { value: 'isNotNull', label: 'is not null' },
]

const fieldClass =
  'w-full rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

interface PredicateBuilderProps {
  predicate: Predicate
  /** Field paths offered as autocomplete (from the inferred input record type). */
  fieldSuggestions?: string[]
  onChange: (predicate: Predicate) => void
}

/** Inspector editor for a structured boolean predicate. */
export function PredicateBuilder({
  predicate,
  fieldSuggestions = [],
  onChange,
}: PredicateBuilderProps) {
  const listId = useId()

  const updateCondition = (id: string, patch: Partial<Condition>) =>
    onChange({
      ...predicate,
      conditions: predicate.conditions.map((c) =>
        c.id === id ? { ...c, ...patch } : c,
      ),
    })
  const removeCondition = (id: string) =>
    onChange({
      ...predicate,
      conditions: predicate.conditions.filter((c) => c.id !== id),
    })
  const addCondition = () =>
    onChange({
      ...predicate,
      conditions: [...predicate.conditions, newCondition()],
    })
  const setCombinator = (combinator: Combinator) =>
    onChange({ ...predicate, combinator })

  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-600">Predicate</span>

      {fieldSuggestions.length > 0 && (
        <datalist id={listId}>
          {fieldSuggestions.map((path) => (
            <option key={path} value={path} />
          ))}
        </datalist>
      )}

      {predicate.conditions.length === 0 && (
        <span className="text-[11px] text-slate-400">
          No conditions — matches every record.
        </span>
      )}

      {predicate.conditions.map((cond, index) => (
        <div key={cond.id} className="flex flex-col gap-1">
          {index > 0 && (
            <div className="flex gap-1">
              {(['AND', 'OR'] as Combinator[]).map((c) => (
                <button
                  key={c}
                  type="button"
                  onClick={() => setCombinator(c)}
                  className={`rounded px-2 py-0.5 text-[10px] font-semibold ${
                    predicate.combinator === c
                      ? 'bg-slate-700 text-white'
                      : 'bg-slate-100 text-slate-500 hover:bg-slate-200'
                  }`}
                >
                  {c}
                </button>
              ))}
            </div>
          )}
          <div className="flex items-center gap-1">
            <input
              value={cond.field}
              placeholder="field"
              list={listId}
              onChange={(e) =>
                updateCondition(cond.id, { field: e.target.value })
              }
              className={fieldClass}
            />
            <select
              value={cond.operator}
              onChange={(e) =>
                updateCondition(cond.id, {
                  operator: e.target.value as ComparisonOperator,
                })
              }
              className={fieldClass}
            >
              {OPERATORS.map((o) => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
            <button
              type="button"
              onClick={() => removeCondition(cond.id)}
              aria-label="Remove condition"
              className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
          {!VALUELESS_OPERATORS.includes(cond.operator) && (
            <input
              value={cond.value}
              placeholder="value"
              onChange={(e) =>
                updateCondition(cond.id, { value: e.target.value })
              }
              className={fieldClass}
            />
          )}
        </div>
      ))}

      <button
        type="button"
        onClick={addCondition}
        className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
      >
        <Plus className="h-3.5 w-3.5" />
        Add condition
      </button>
    </div>
  )
}
