import { useId } from 'react'
import type { ValueExpression } from '../expressions/types'

const inputCls =
  'rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

interface ExpressionBuilderProps {
  expression: ValueExpression
  /** Input field paths offered as autocomplete for `field` expressions. */
  fieldSuggestions: string[]
  onChange: (expression: ValueExpression) => void
}

/** Edits a single value expression — a field reference or a literal. */
export function ExpressionBuilder({
  expression,
  fieldSuggestions,
  onChange,
}: ExpressionBuilderProps) {
  const listId = useId()

  return (
    <div className="flex items-center gap-1">
      <select
        className={inputCls}
        value={expression.kind}
        onChange={(e) =>
          onChange(
            e.target.value === 'literal'
              ? { kind: 'literal', value: '' }
              : { kind: 'field', path: '' },
          )
        }
      >
        <option value="field">field</option>
        <option value="literal">literal</option>
      </select>
      {expression.kind === 'field' ? (
        <>
          <input
            className={`${inputCls} min-w-0 flex-1`}
            list={listId}
            value={expression.path}
            placeholder="input field"
            onChange={(e) => onChange({ kind: 'field', path: e.target.value })}
          />
          {fieldSuggestions.length > 0 && (
            <datalist id={listId}>
              {fieldSuggestions.map((path) => (
                <option key={path} value={path} />
              ))}
            </datalist>
          )}
        </>
      ) : (
        <input
          className={`${inputCls} min-w-0 flex-1`}
          value={expression.value}
          placeholder="literal value"
          onChange={(e) => onChange({ kind: 'literal', value: e.target.value })}
        />
      )}
    </div>
  )
}
