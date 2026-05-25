import { Repeat, X } from 'lucide-react'
import type { RenderedRecord } from '../../api/adminClient'
import { formatTimestamp } from './formatMessage'

interface MessageDetailProps {
  record: RenderedRecord
  onReplay: () => void
  onClose: () => void
}

/** The side panel showing one record in full. */
export function MessageDetail({
  record,
  onReplay,
  onClose,
}: MessageDetailProps) {
  const valueText =
    record.value.strategy === 'TOMBSTONE'
      ? '∅ tombstone (null value)'
      : (record.value.text ?? '')
  const warnings = [...record.key.warnings, ...record.value.warnings]

  return (
    <aside className="flex w-96 shrink-0 flex-col border-l border-slate-200">
      <div className="flex items-center justify-between border-b border-slate-200 px-3 py-2">
        <h3 className="text-xs font-semibold text-slate-700">
          Partition {record.partition} · offset {record.offset}
        </h3>
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={onReplay}
            className="flex items-center gap-1 rounded border border-slate-300 px-1.5 py-0.5 text-[11px] text-slate-600 hover:bg-slate-100"
          >
            <Repeat className="h-3 w-3" />
            Replay
          </button>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-3.5 w-3.5" />
          </button>
        </div>
      </div>
      <div className="flex flex-1 flex-col gap-3 overflow-y-auto p-3 text-xs">
        <Field label="Timestamp" value={formatTimestamp(record.timestamp)} />
        <Block
          label={`Key (${record.key.strategy.toLowerCase()})`}
          text={record.key.text ?? '(none)'}
        />
        <Block
          label={`Value (${record.value.strategy.toLowerCase()})`}
          text={valueText}
        />
        <div>
          <p className="mb-1 text-[10px] tracking-wide text-slate-400 uppercase">
            Headers ({record.headers.length})
          </p>
          {record.headers.length === 0 ? (
            <p className="text-slate-400">No headers.</p>
          ) : (
            <table className="w-full border-collapse">
              <tbody>
                {record.headers.map((h, i) => (
                  <tr key={`${h.key}-${i}`} className="border-b border-slate-100">
                    <td className="py-1 pr-2 font-mono text-slate-600">
                      {h.key}
                    </td>
                    <td className="py-1 font-mono break-all text-slate-500">
                      {h.value ?? ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        {warnings.length > 0 && (
          <div className="rounded border border-amber-200 bg-amber-50 px-2 py-1 text-[11px] text-amber-700">
            {warnings.join(' · ')}
          </div>
        )}
      </div>
    </aside>
  )
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-[10px] tracking-wide text-slate-400 uppercase">
        {label}
      </p>
      <p className="font-mono text-slate-700">{value}</p>
    </div>
  )
}

function Block({ label, text }: { label: string; text: string }) {
  return (
    <div>
      <p className="mb-1 text-[10px] tracking-wide text-slate-400 uppercase">
        {label}
      </p>
      <pre className="max-h-64 overflow-auto rounded border border-slate-200 bg-slate-50 p-2 font-mono text-[11px] break-all whitespace-pre-wrap text-slate-700">
        {text}
      </pre>
    </div>
  )
}
