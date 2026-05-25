import { useMemo } from 'react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { getTopicMetrics } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'

interface TopicMetricsPanelProps {
  cluster: SavedCluster
  topic: string
}

/** Human-readable byte size. */
function formatBytes(bytes: number): string {
  if (bytes < 0) return '—'
  if (bytes < 1024) return `${bytes} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let value = bytes / 1024
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit++
  }
  return `${value.toFixed(1)} ${units[unit]}`
}

/** Offset-based counts, size and health for a topic. */
export function TopicMetricsPanel({ cluster, topic }: TopicMetricsPanelProps) {
  const conn = toConnection(cluster)
  const { data, loading, error } = useAdminQuery(
    `${cluster.id}:topic-metrics:${topic}`,
    () => getTopicMetrics(conn, topic),
  )

  const skew = useMemo(() => {
    if (!data || data.partitions.length === 0) return 0
    const counts = data.partitions.map((p) => p.count)
    return Math.max(...counts) - Math.min(...counts)
  }, [data])

  if (error) {
    return (
      <p className="text-xs text-slate-400">Metrics unavailable: {error}</p>
    )
  }
  if (loading && !data) {
    return <p className="text-xs text-slate-400">Loading metrics…</p>
  }
  if (!data) return null

  const warnings: string[] = []
  if (data.offlinePartitions > 0) {
    warnings.push(`${data.offlinePartitions} partition(s) have no leader`)
  }
  if (data.underReplicatedPartitions > 0) {
    warnings.push(
      `${data.underReplicatedPartitions} partition(s) under-replicated`,
    )
  }

  return (
    <div className="flex flex-col gap-2">
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
        <Stat label="Messages" value={data.messageCount.toLocaleString()} />
        <Stat label="Size" value={formatBytes(data.sizeBytes)} />
        <Stat label="Partitions" value={String(data.partitionCount)} />
        <Stat label="Partition skew" value={skew.toLocaleString()} />
      </div>
      {warnings.length > 0 && (
        <div className="rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-[11px] text-amber-700">
          {warnings.join(' · ')}
        </div>
      )}
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded border border-slate-200 p-2">
      <p className="text-[10px] tracking-wide text-slate-400 uppercase">
        {label}
      </p>
      <p className="mt-0.5 text-sm font-semibold text-slate-800">{value}</p>
    </div>
  )
}
