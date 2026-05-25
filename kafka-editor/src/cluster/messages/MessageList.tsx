import type { RenderedRecord } from '../../api/adminClient'
import {
  formatTimestamp,
  isTombstone,
  previewText,
  recordKey,
} from './formatMessage'

interface MessageListProps {
  records: RenderedRecord[]
  selectedKey: string | null
  onSelect: (record: RenderedRecord) => void
}

const STRATEGY_COLOR: Record<string, string> = {
  AVRO: 'bg-violet-100 text-violet-700',
  JSON: 'bg-sky-100 text-sky-700',
  STRING: 'bg-slate-100 text-slate-600',
  BINARY: 'bg-amber-100 text-amber-700',
  TOMBSTONE: 'bg-slate-100 text-slate-400',
  EMPTY: 'bg-slate-100 text-slate-400',
}

/** The browsed/tailed message rows. */
export function MessageList({
  records,
  selectedKey,
  onSelect,
}: MessageListProps) {
  if (records.length === 0) {
    return <p className="p-4 text-xs text-slate-400">No messages.</p>
  }
  return (
    <table className="w-full border-collapse text-xs">
      <thead className="sticky top-0 bg-white">
        <tr className="border-b border-slate-200 text-left text-slate-500">
          <th className="py-1.5 pr-2 pl-3 font-medium">P</th>
          <th className="py-1.5 pr-2 font-medium">Offset</th>
          <th className="py-1.5 pr-2 font-medium">Timestamp</th>
          <th className="py-1.5 pr-2 font-medium">Key</th>
          <th className="py-1.5 pr-2 font-medium">Value</th>
          <th className="py-1.5 pr-3 font-medium">Type</th>
        </tr>
      </thead>
      <tbody>
        {records.map((record) => {
          const key = recordKey(record)
          const tomb = isTombstone(record)
          return (
            <tr
              key={key}
              onClick={() => onSelect(record)}
              className={`cursor-pointer border-b border-slate-100 hover:bg-slate-50 ${
                selectedKey === key ? 'bg-slate-100' : ''
              }`}
            >
              <td className="py-1.5 pr-2 pl-3 font-mono">{record.partition}</td>
              <td className="py-1.5 pr-2 font-mono">{record.offset}</td>
              <td className="py-1.5 pr-2 font-mono text-slate-500">
                {formatTimestamp(record.timestamp)}
              </td>
              <td className="max-w-[160px] truncate py-1.5 pr-2 font-mono text-slate-600">
                {previewText(record.key, 40)}
              </td>
              <td
                className={`max-w-[420px] truncate py-1.5 pr-2 font-mono ${
                  tomb ? 'text-slate-400 italic' : 'text-slate-700'
                }`}
              >
                {previewText(record.value)}
              </td>
              <td className="py-1.5 pr-3">
                <span
                  className={`rounded px-1 py-0.5 text-[10px] ${
                    STRATEGY_COLOR[record.value.strategy] ??
                    'bg-slate-100 text-slate-600'
                  }`}
                >
                  {record.value.strategy.toLowerCase()}
                </span>
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}
