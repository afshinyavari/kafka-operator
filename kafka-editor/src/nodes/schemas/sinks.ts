import { registerNode } from '../registry'

/** Sink node types — they write a stream to a Kafka topic and have no outputs. */

registerNode({
  type: 'sink',
  label: 'Sink',
  category: 'sink',
  description: 'Writes records from a stream to a Kafka topic.',
  icon: 'Send',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [],
  properties: [
    { key: 'topicId', label: 'Topic', kind: 'topicRef', required: true },
  ],
})
