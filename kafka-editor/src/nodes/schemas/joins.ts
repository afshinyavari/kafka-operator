import { registerNode } from '../registry'
import type { PropertySpec } from '../types'

/** Join node types — two inputs, one output. */

const joinType: PropertySpec = {
  key: 'joinType',
  label: 'Join Type',
  kind: 'select',
  default: 'inner',
  options: [
    { label: 'Inner join', value: 'inner' },
    { label: 'Left join', value: 'left' },
    { label: 'Outer join', value: 'outer' },
  ],
}

/** The joined output value, built field-by-field from left/right inputs. */
const valueJoiner: PropertySpec = {
  key: 'valueJoiner',
  label: 'Value Joiner',
  kind: 'valueMapping',
}

registerNode({
  type: 'stream-stream-join',
  label: 'Stream-Stream Join',
  category: 'join',
  description: 'Joins two KStreams within a time window.',
  icon: 'Merge',
  inputs: [
    { id: 'left', label: 'left', accepts: ['KStream'] },
    { id: 'right', label: 'right', accepts: ['KStream'] },
  ],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    joinType,
    {
      key: 'windowSizeMs',
      label: 'Window (ms)',
      kind: 'number',
      default: 60000,
      help: 'Join window size in milliseconds.',
    },
    valueJoiner,
  ],
})

registerNode({
  type: 'stream-table-join',
  label: 'Stream-Table Join',
  category: 'join',
  description: 'Enriches a KStream with lookups into a KTable.',
  icon: 'Merge',
  inputs: [
    { id: 'left', label: 'stream', accepts: ['KStream'] },
    { id: 'right', label: 'table', accepts: ['KTable'] },
  ],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [joinType, valueJoiner],
})

registerNode({
  type: 'table-table-join',
  label: 'Table-Table Join',
  category: 'join',
  description: 'Joins two KTables into a new KTable.',
  icon: 'Merge',
  inputs: [
    { id: 'left', label: 'left', accepts: ['KTable'] },
    { id: 'right', label: 'right', accepts: ['KTable'] },
  ],
  outputs: [{ id: 'out', produces: 'KTable' }],
  properties: [joinType, valueJoiner],
})
