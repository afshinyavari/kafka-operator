import { registerNode } from '../registry'
import type { PropertySpec } from '../types'

/**
 * Kafka Connect connectors. They are not part of the Streams topology — they
 * feed or drain a topic — but appear on the canvas as source-like / sink-like
 * nodes so the data flow is visible. They carry an arbitrary connector config.
 */

const topicProp: PropertySpec = {
  key: 'topicId',
  label: 'Topic',
  kind: 'topicRef',
  required: true,
}

const connectorClassProp: PropertySpec = {
  key: 'connectorClass',
  label: 'Connector Class',
  kind: 'text',
  required: true,
  placeholder: 'io.debezium.connector.postgresql.PostgresConnector',
}

const connectorConfigProp: PropertySpec = {
  key: 'connectorConfig',
  label: 'Connector Config',
  kind: 'keyValue',
}

registerNode({
  type: 'connect-source',
  label: 'Connect Source',
  category: 'connector',
  description: 'A Kafka Connect source connector feeding a topic.',
  icon: 'PlugZap',
  inputs: [],
  outputs: [{ id: 'out', produces: 'KStream' }],
  properties: [
    topicProp,
    {
      key: 'valueRecordTypeId',
      label: 'Value Type',
      kind: 'recordTypeRef',
      help: 'The record type the connector produces into the topic.',
    },
    connectorClassProp,
    connectorConfigProp,
  ],
})

registerNode({
  type: 'connect-sink',
  label: 'Connect Sink',
  category: 'connector',
  description: 'A Kafka Connect sink connector draining a topic.',
  icon: 'Plug',
  inputs: [{ id: 'in', accepts: ['KStream'] }],
  outputs: [],
  properties: [topicProp, connectorClassProp, connectorConfigProp],
})
