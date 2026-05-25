import { registerNode } from '../registry'
import { logicProp } from './shared'

/**
 * Stateless DSL operators. `filter`, `filterNot` and `mapValues` are polymorphic
 * — they accept a KStream or a KTable and produce the same kind (`produces:
 * 'inherit'`). The rest are KStream-only, matching the Kafka Streams API.
 * filter/filterNot use the structured predicate builder; the remaining
 * operators keep a placeholder logic field until later M3 work.
 */

registerNode({
  type: 'filter',
  label: 'Filter',
  category: 'stateless',
  description: 'Keeps only records that match a predicate. Works on a stream or a table.',
  icon: 'Filter',
  inputs: [{ id: 'in', accepts: ['KStream', 'KTable'] }],
  outputs: [{ id: 'out', produces: 'inherit' }],
  properties: [{ key: 'predicate', label: 'Predicate', kind: 'predicate' }],
})

registerNode({
  type: 'filter-not',
  label: 'Filter Not',
  category: 'stateless',
  description: 'Drops records that match a predicate. Works on a stream or a table.',
  icon: 'FilterX',
  inputs: [{ id: 'in', accepts: ['KStream', 'KTable'] }],
  outputs: [{ id: 'out', produces: 'inherit' }],
  properties: [{ key: 'predicate', label: 'Predicate', kind: 'predicate' }],
})

registerNode({
  type: 'map',
  label: 'Map',
  category: 'stateless',
  description: 'Transforms each record into a new key and value.',
  icon: 'ArrowRightLeft',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    { key: 'keyExpression', label: 'New Key', kind: 'keyExpression' },
    { key: 'valueMapping', label: 'Value Mapping', kind: 'valueMapping' },
  ],
})

registerNode({
  type: 'map-values',
  label: 'Map Values',
  category: 'stateless',
  description: 'Transforms each value, keeping the key. Works on a stream or a table.',
  icon: 'Replace',
  inputs: [{ id: 'in', accepts: ['KStream', 'KTable'] }],
  outputs: [{ id: 'out', produces: 'inherit' }],
  properties: [
    { key: 'valueMapping', label: 'Value Mapping', kind: 'valueMapping' },
  ],
})

registerNode({
  type: 'flat-map',
  label: 'Flat Map',
  category: 'stateless',
  description: 'Maps each record to zero or more output records.',
  icon: 'Split',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    logicProp('mapper', 'Mapper', '(key, value) -> Iterable<KeyValue>'),
  ],
})

registerNode({
  type: 'flat-map-values',
  label: 'Flat Map Values',
  category: 'stateless',
  description: 'Maps each value to zero or more output values.',
  icon: 'GitFork',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    logicProp('mapper', 'Value Mapper', 'value -> Iterable<newValue>'),
  ],
})

registerNode({
  type: 'select-key',
  label: 'Select Key',
  category: 'stateless',
  description: 'Assigns a new key to each record.',
  icon: 'KeyRound',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    { key: 'keyExpression', label: 'New Key', kind: 'keyExpression' },
  ],
})

registerNode({
  type: 'peek',
  label: 'Peek',
  category: 'stateless',
  description: 'Runs a side effect per record without changing the stream.',
  icon: 'Eye',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [logicProp('action', 'Action', '(key, value) -> void')],
})

registerNode({
  type: 'foreach',
  label: 'For Each',
  category: 'stateless',
  description: 'Terminal: runs a side effect per record, produces no output.',
  icon: 'Repeat',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [],
  properties: [logicProp('action', 'Action', '(key, value) -> void')],
})

registerNode({
  type: 'repartition',
  label: 'Repartition',
  category: 'stateless',
  description: 'Forces a repartition of the stream through an internal topic.',
  icon: 'Shuffle',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    {
      key: 'name',
      label: 'Repartition Name',
      kind: 'text',
      placeholder: 'optional internal topic name',
    },
    {
      key: 'numberOfPartitions',
      label: 'Partitions',
      kind: 'number',
      placeholder: 'optional',
      help: 'Number of partitions for the repartition topic.',
    },
  ],
})

registerNode({
  type: 'branch',
  label: 'Branch',
  category: 'stateless',
  description: 'Splits a stream into multiple named branches.',
  icon: 'GitBranch',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [],
  dynamicOutputs: { configKey: 'branches', produces: 'KStream' },
  properties: [{ key: 'branches', label: 'Branches', kind: 'branchList' }],
})
