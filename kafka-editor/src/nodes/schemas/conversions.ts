import { registerNode } from '../registry'

/** Conversions between the KStream and KTable views of data. */

registerNode({
  type: 'to-stream',
  label: 'To Stream',
  category: 'stateless',
  description: 'Converts a KTable into a KStream of its changelog.',
  icon: 'Spline',
  inputs: [{ id: 'in', accepts: ['KTable'] }],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [],
})

registerNode({
  type: 'to-table',
  label: 'To Table',
  category: 'stateless',
  description: 'Materializes a KStream into a KTable.',
  icon: 'Table',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [{ id: 'out', produces: 'KTable' }],
  properties: [],
})
