import { useEffect, useMemo, useState } from 'react'
import {
  ArrowLeft,
  Download,
  RefreshCw,
  Search,
  Send,
} from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import {
  browseMessages,
  getMessagePartitions,
  tailMessages,
} from '../../api/adminClient'
import type { RenderedRecord, SeekMode } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'
import { MessageList } from './MessageList'
import { MessageDetail } from './MessageDetail'
import { ProduceModal } from './ProduceModal'
import { ReplayModal } from './ReplayModal'
import { recordKey, recordMatchesFilter } from './formatMessage'
import type { FilterField } from './formatMessage'
import { downloadText, toCsv, toJson } from './exportMessages'

interface MessageBrowserProps {
  cluster: SavedCluster
  topic: string
  onBack: () => void
}

const TAIL_CAP = 1000
const DISPLAY_CAP = 500
const ctrl =
  'rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 outline-none focus:border-slate-500'

/** Browse, tail, filter, produce, replay and export a topic's messages. */
export function MessageBrowser({ cluster, topic, onBack }: MessageBrowserProps) {
  const conn = toConnection(cluster)
  const [partition, setPartition] = useState(0)
  const [seek, setSeek] = useState<SeekMode>('LATEST')
  const [offsetInput, setOffsetInput] = useState('0')
  const [timestampInput, setTimestampInput] = useState('')
  const [size, setSize] = useState(50)
  const [loadNonce, setLoadNonce] = useState(0)
  const [tailOn, setTailOn] = useState(false)
  const [tailRecords, setTailRecords] = useState<RenderedRecord[]>([])
  const [filter, setFilter] = useState('')
  const [field, setField] = useState<FilterField>('all')
  const [selected, setSelected] = useState<RenderedRecord | null>(null)
  const [showProduce, setShowProduce] = useState(false)
  const [replayRecord, setReplayRecord] = useState<RenderedRecord | null>(null)

  const partitionsQuery = useAdminQuery(
    `${cluster.id}:partitions:${topic}`,
    () => getMessagePartitions(conn, topic),
  )
  const partitions = partitionsQuery.data ?? [0]

  const page = useAdminQuery(
    `browse:${cluster.id}:${topic}:${partition}:${loadNonce}`,
    () =>
      browseMessages(conn, {
        topic,
        partition,
        seek,
        offset: seek === 'OFFSET' ? Number(offsetInput) || 0 : undefined,
        timestamp:
          seek === 'TIMESTAMP' && timestampInput
            ? new Date(timestampInput).getTime()
            : undefined,
        size,
      }),
  )

  // Live tail subscription. The buffer is cleared by the toggle handler, so the
  // effect itself never writes state synchronously.
  useEffect(() => {
    if (!tailOn) return
    return tailMessages(conn, topic, [partition], (record) => {
      setTailRecords((prev) => {
        const next = [record, ...prev]
        return next.length > TAIL_CAP ? next.slice(0, TAIL_CAP) : next
      })
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tailOn, cluster.id, topic, partition])

  const loadedRows = page.data?.rows
  const totalLoaded = tailOn ? tailRecords.length : (loadedRows?.length ?? 0)
  const displayed = useMemo(() => {
    const base = tailOn ? tailRecords : (loadedRows ?? [])
    return base
      .filter((r) => recordMatchesFilter(r, filter, field))
      .slice(0, DISPLAY_CAP)
  }, [tailOn, tailRecords, loadedRows, filter, field])

  const toggleTail = () => {
    setTailRecords([])
    setSelected(null)
    setTailOn((on) => !on)
  }

  const exportAs = (format: 'json' | 'csv') => {
    const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')
    if (format === 'json') {
      downloadText(`${topic}-${stamp}.json`, toJson(displayed), 'application/json')
    } else {
      downloadText(`${topic}-${stamp}.csv`, toCsv(displayed), 'text/csv')
    }
  }

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between gap-2 border-b border-slate-200 px-4 py-2">
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={onBack}
            aria-label="Back"
            className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
          >
            <ArrowLeft className="h-3.5 w-3.5" />
          </button>
          <h2 className="font-mono text-sm font-semibold text-slate-800">
            {topic}
          </h2>
          <span className="text-[11px] text-slate-400">messages</span>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => exportAs('json')}
            disabled={displayed.length === 0}
            className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100 disabled:opacity-50"
          >
            <Download className="h-3.5 w-3.5" />
            JSON
          </button>
          <button
            type="button"
            onClick={() => exportAs('csv')}
            disabled={displayed.length === 0}
            className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100 disabled:opacity-50"
          >
            <Download className="h-3.5 w-3.5" />
            CSV
          </button>
          <button
            type="button"
            onClick={() => setShowProduce(true)}
            className="flex items-center gap-1 rounded border border-emerald-700 bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700"
          >
            <Send className="h-3.5 w-3.5" />
            Produce
          </button>
        </div>
      </div>

      {/* Browse controls */}
      <div className="flex flex-wrap items-center gap-2 border-b border-slate-200 px-4 py-2">
        <label className="flex items-center gap-1 text-xs text-slate-500">
          Partition
          <select
            className={ctrl}
            value={partition}
            onChange={(e) => setPartition(Number(e.target.value))}
          >
            {partitions.map((p) => (
              <option key={p} value={p}>
                {p}
              </option>
            ))}
          </select>
        </label>
        <label className="flex items-center gap-1 text-xs text-slate-500">
          From
          <select
            className={ctrl}
            value={seek}
            disabled={tailOn}
            onChange={(e) => setSeek(e.target.value as SeekMode)}
          >
            <option value="LATEST">Latest</option>
            <option value="OFFSET">Offset</option>
            <option value="TIMESTAMP">Timestamp</option>
          </select>
        </label>
        {seek === 'OFFSET' && !tailOn && (
          <input
            className={`${ctrl} w-24`}
            type="number"
            min={0}
            value={offsetInput}
            onChange={(e) => setOffsetInput(e.target.value)}
          />
        )}
        {seek === 'TIMESTAMP' && !tailOn && (
          <input
            className={ctrl}
            type="datetime-local"
            value={timestampInput}
            onChange={(e) => setTimestampInput(e.target.value)}
          />
        )}
        <label className="flex items-center gap-1 text-xs text-slate-500">
          Size
          <input
            className={`${ctrl} w-16`}
            type="number"
            min={1}
            max={500}
            value={size}
            disabled={tailOn}
            onChange={(e) =>
              setSize(Math.min(500, Math.max(1, Number(e.target.value) || 1)))
            }
          />
        </label>
        <button
          type="button"
          onClick={() => {
            setSelected(null)
            setLoadNonce((n) => n + 1)
          }}
          disabled={tailOn}
          className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100 disabled:opacity-50"
        >
          <RefreshCw className="h-3.5 w-3.5" />
          Load
        </button>
        <label className="flex items-center gap-1 text-xs text-slate-600">
          <input type="checkbox" checked={tailOn} onChange={toggleTail} />
          Live tail
          {tailOn && (
            <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-emerald-500" />
          )}
        </label>
      </div>

      {/* Filter */}
      <div className="flex items-center gap-2 border-b border-slate-200 px-4 py-1.5">
        <div className="flex items-center gap-1 rounded border border-slate-300 px-1.5">
          <Search className="h-3 w-3 text-slate-400" />
          <input
            className="w-48 py-1 text-xs text-slate-700 outline-none"
            placeholder="Filter loaded messages…"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        </div>
        <select
          className={ctrl}
          value={field}
          onChange={(e) => setField(e.target.value as FilterField)}
        >
          <option value="all">All</option>
          <option value="key">Key</option>
          <option value="value">Value</option>
          <option value="header">Header</option>
        </select>
        <span className="ml-auto text-[11px] text-slate-400">
          {displayed.length} shown
          {totalLoaded > displayed.length && ` of ${totalLoaded}`}
          {!tailOn && page.data?.truncated && ' · more available'}
        </span>
      </div>

      <div className="flex flex-1 overflow-hidden">
        <div className="flex-1 overflow-y-auto">
          {(page.error || partitionsQuery.error) && !tailOn && (
            <ErrorBanner
              message={page.error ?? partitionsQuery.error ?? ''}
              onRetry={() => setLoadNonce((n) => n + 1)}
            />
          )}
          {!tailOn && page.loading && !page.data && (
            <p className="p-4 text-xs text-slate-400">Loading…</p>
          )}
          <MessageList
            records={displayed}
            selectedKey={selected ? recordKey(selected) : null}
            onSelect={setSelected}
          />
        </div>
        {selected && (
          <MessageDetail
            record={selected}
            onReplay={() => setReplayRecord(selected)}
            onClose={() => setSelected(null)}
          />
        )}
      </div>

      {showProduce && (
        <ProduceModal
          conn={conn}
          topic={topic}
          onClose={() => setShowProduce(false)}
          onProduced={() => {
            if (!tailOn) setLoadNonce((n) => n + 1)
          }}
        />
      )}
      {replayRecord && (
        <ReplayModal
          conn={conn}
          sourceTopic={topic}
          record={replayRecord}
          onClose={() => setReplayRecord(null)}
          onReplayed={() => {
            if (!tailOn) setLoadNonce((n) => n + 1)
          }}
        />
      )}
    </div>
  )
}
