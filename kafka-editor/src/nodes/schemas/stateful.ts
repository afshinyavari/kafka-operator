import { registerNode } from '../registry'
import type { PropertySpec } from '../types'

/**
 * Stateful DSL operators — grouping and aggregation. Grouping turns a KStream
 * into a KGroupedStream; the aggregations turn that into a KTable. `reduce`
 * picks a combine strategy; `aggregate` uses a structured accumulator builder.
 */

const storeName: PropertySpec = {
  key: 'storeName',
  label: 'State Store Name',
  kind: 'text',
  placeholder: 'optional store name',
  help: 'Names the queryable state store backing this aggregation.',
}

/** Optional windowing for an aggregation. */
const windowProp: PropertySpec = {
  key: 'window',
  label: 'Window',
  kind: 'window',
}

/** A reduce combine strategy. */
const reducerProp: PropertySpec = {
  key: 'reducer',
  label: 'Reducer',
  kind: 'select',
  default: 'latest',
  options: [
    { label: 'Keep latest value', value: 'latest' },
    { label: 'Keep earliest value', value: 'earliest' },
    { label: 'Sum values', value: 'sum' },
    { label: 'Keep minimum', value: 'min' },
    { label: 'Keep maximum', value: 'max' },
  ],
}

registerNode({
  type: 'group-by-key',
  label: 'Group By Key',
  category: 'stateful',
  description: 'Groups a stream by its current key, ready for aggregation.',
  icon: 'Group',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KGroupedStream' }],
  properties: [],
})

registerNode({
  type: 'group-by',
  label: 'Group By',
  category: 'stateful',
  description: 'Re-groups a stream by a new key, ready for aggregation.',
  icon: 'Layers',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KGroupedStream' }],
  properties: [
    { key: 'keyExpression', label: 'Group Key', kind: 'keyExpression' },
  ],
})

registerNode({
  type: 'count',
  label: 'Count',
  category: 'stateful',
  description: 'Counts the records per group into a KTable.',
  icon: 'Hash',
  inputs: [{ id: 'in', accepts: ['KGroupedStream'] }],
  outputs: [{ id: 'out', produces: 'KTable' }],
  properties: [windowProp, storeName],
})

registerNode({
  type: 'reduce',
  label: 'Reduce',
  category: 'stateful',
  description: 'Combines records per group with a reduce strategy into a KTable.',
  icon: 'FoldVertical',
  inputs: [{ id: 'in', accepts: ['KGroupedStream'] }],
  outputs: [{ id: 'out', produces: 'KTable' }],
  properties: [reducerProp, windowProp, storeName],
})

registerNode({
  type: 'aggregate',
  label: 'Aggregate',
  category: 'stateful',
  description: 'Aggregates records per group into a structured accumulator.',
  icon: 'Sigma',
  inputs: [{ id: 'in', accepts: ['KGroupedStream'] }],
  outputs: [{ id: 'out', produces: 'KTable' }],
  properties: [
    { key: 'aggregation', label: 'Accumulator', kind: 'aggregation' },
    windowProp,
    storeName,
  ],
})
