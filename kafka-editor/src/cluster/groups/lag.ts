import type { PartitionLag } from '../../api/adminClient'

/** Pure helpers for consumer-lag display. */

export interface TopicLag {
  topic: string
  lag: number
  partitions: number
}

/** Roll up per-partition lag into per-topic totals (null lag counts as 0). */
export function lagByTopic(partitions: PartitionLag[]): TopicLag[] {
  const map = new Map<string, { lag: number; partitions: number }>()
  for (const p of partitions) {
    const entry = map.get(p.topic) ?? { lag: 0, partitions: 0 }
    entry.lag += p.lag ?? 0
    entry.partitions += 1
    map.set(p.topic, entry)
  }
  return [...map.entries()]
    .map(([topic, e]) => ({ topic, lag: e.lag, partitions: e.partitions }))
    .sort((a, b) => a.topic.localeCompare(b.topic))
}

/** Format a lag value — a dash when there is no committed offset. */
export function formatLag(lag: number | null): string {
  return lag == null ? '—' : lag.toLocaleString()
}
