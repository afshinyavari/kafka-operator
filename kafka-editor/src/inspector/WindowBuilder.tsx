import { WINDOW_TYPES } from '../model/windows'
import type { WindowConfig, WindowType } from '../model/windows'

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500'

interface WindowBuilderProps {
  window: WindowConfig
  onChange: (window: WindowConfig) => void
}

/** Inspector editor for an aggregation's windowing configuration. */
export function WindowBuilder({ window, onChange }: WindowBuilderProps) {
  const optionalMs = (raw: string): number | undefined =>
    raw === '' ? undefined : Number(raw)

  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-600">Window</span>
      <select
        className={inputCls}
        value={window.type}
        onChange={(e) =>
          onChange({ ...window, type: e.target.value as WindowType })
        }
      >
        {WINDOW_TYPES.map((w) => (
          <option key={w.value} value={w.value}>
            {w.label}
          </option>
        ))}
      </select>

      {window.type !== 'none' && (
        <>
          <label className="flex flex-col gap-1">
            <span className="text-[11px] text-slate-500">
              {window.type === 'session'
                ? 'Inactivity gap (ms)'
                : 'Window size (ms)'}
            </span>
            <input
              type="number"
              className={inputCls}
              value={window.sizeMs}
              onChange={(e) =>
                onChange({ ...window, sizeMs: Number(e.target.value) || 0 })
              }
            />
          </label>

          {window.type === 'hopping' && (
            <label className="flex flex-col gap-1">
              <span className="text-[11px] text-slate-500">Advance by (ms)</span>
              <input
                type="number"
                className={inputCls}
                value={window.advanceMs ?? ''}
                placeholder="hop interval"
                onChange={(e) =>
                  onChange({ ...window, advanceMs: optionalMs(e.target.value) })
                }
              />
            </label>
          )}

          <label className="flex flex-col gap-1">
            <span className="text-[11px] text-slate-500">Grace period (ms)</span>
            <input
              type="number"
              className={inputCls}
              value={window.graceMs ?? ''}
              placeholder="allowed lateness"
              onChange={(e) =>
                onChange({ ...window, graceMs: optionalMs(e.target.value) })
              }
            />
          </label>
        </>
      )}
    </div>
  )
}
