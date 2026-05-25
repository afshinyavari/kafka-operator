import { describe, expect, it } from 'vitest'
import type { PartitionLag } from '../../api/adminClient'
import { formatLag, lagByTopic } from './lag'

function pl(
  topic: string,
  partition: number,
  lag: number | null,
): PartitionLag {
  return { topic, partition, committedOffset: 0, endOffset: 0, lag }
}

describe('lagByTopic', () => {
  it('rolls partitions up into per-topic totals', () => {
    const rolled = lagByTopic([
      pl('orders', 0, 5),
      pl('orders', 1, 3),
      pl('events', 0, 10),
    ])
    expect(rolled).toEqual([
      { topic: 'events', lag: 10, partitions: 1 },
      { topic: 'orders', lag: 8, partitions: 2 },
    ])
  })

  it('treats a null partition lag as zero', () => {
    const rolled = lagByTopic([pl('orders', 0, null), pl('orders', 1, 4)])
    expect(rolled[0]).toEqual({ topic: 'orders', lag: 4, partitions: 2 })
  })

  it('returns nothing for an empty input', () => {
    expect(lagByTopic([])).toEqual([])
  })
})

describe('formatLag', () => {
  it('renders a number', () => {
    expect(formatLag(1500)).toBe((1500).toLocaleString())
  })

  it('renders a dash when there is no committed offset', () => {
    expect(formatLag(null)).toBe('—')
  })
})
