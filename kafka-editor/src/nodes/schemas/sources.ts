import { registerNode } from '../registry'
import type { PropertySpec } from '../types'

/** Source node types — they read from a Kafka topic and have no inputs. */

/** References a topic defined in the catalog. */
const topicProp: PropertySpec = {
  key: 'topicId',
  label: 'Topic',
  kind: 'topicRef',
  required: true,
}

/** Lets a source declare the record type of its values. */
const valueTypeProp: PropertySpec = {
  key: 'valueRecordTypeId',
  label: 'Value Type',
  kind: 'recordTypeRef',
  help: 'The record type describing this stream’s value shape.',
}

registerNode({
  type: 'source',
  label: 'Stream Source',
  category: 'source',
  description: 'Reads records from a Kafka topic as a KStream.',
  icon: 'Database',
  inputs: [],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [topicProp, valueTypeProp],
})

registerNode({
  type: 'table-source',
  label: 'Table Source',
  category: 'source',
  description: 'Reads a compacted Kafka topic as a KTable.',
  icon: 'Table2',
  inputs: [],
  outputs: [{ id: 'out', produces: 'KTable' }],
  properties: [topicProp, valueTypeProp],
})
