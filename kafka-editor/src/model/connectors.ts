import { nanoid } from 'nanoid'

/**
 * Kafka Connect connector config. A connector is configured with an arbitrary
 * string-to-string property map; this is the editable representation of one
 * entry. Connectors compile to connector-config JSON (not Streams Java).
 */
export interface ConnectorConfigEntry {
  id: string
  key: string
  value: string
}

export function newConfigEntry(): ConnectorConfigEntry {
  return { id: nanoid(), key: '', value: '' }
}
