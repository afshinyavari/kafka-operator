import type { ProjectDocument, ProjectNode } from '../model/project'
import type { ConnectorConfigEntry } from '../model/connectors'
import type { GeneratedFile } from './generate'

/**
 * Kafka Connect connectors are not part of the Streams topology — they compile
 * to connector-config JSON, posted to the Connect REST API, not to Java.
 */

/** Sanitize a node label into a Kafka Connect connector name. */
function connectorName(node: ProjectNode): string {
  const label =
    typeof node.config.label === 'string' && node.config.label
      ? node.config.label
      : (node.type ?? 'connector')
  return (
    label
      .trim()
      .replace(/[^a-zA-Z0-9._-]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .toLowerCase() || 'connector'
  )
}

function configEntries(node: ProjectNode): ConnectorConfigEntry[] {
  return Array.isArray(node.config.connectorConfig)
    ? (node.config.connectorConfig as ConnectorConfigEntry[])
    : []
}

/** Generate one Kafka Connect connector-config JSON file per connector node. */
export function generateConnectorConfigs(
  doc: ProjectDocument,
): GeneratedFile[] {
  const files: GeneratedFile[] = []
  const names = new Set<string>()

  for (const node of doc.nodes) {
    if (node.type !== 'connect-source' && node.type !== 'connect-sink') {
      continue
    }
    const topic = doc.catalog.topics.find((t) => t.id === node.config.topicId)
    const connectorClass =
      typeof node.config.connectorClass === 'string' &&
      node.config.connectorClass
        ? node.config.connectorClass
        : 'TODO.ConnectorClass'

    let name = connectorName(node)
    const base = name
    let suffix = 2
    while (names.has(name)) name = `${base}-${suffix++}`
    names.add(name)

    // Defaults first; user-supplied config entries override them.
    const config: Record<string, string> = {
      'connector.class': connectorClass,
      'tasks.max': '1',
    }
    if (topic) {
      // Sink connectors consume `topics`; source connectors target `topic`.
      config[node.type === 'connect-sink' ? 'topics' : 'topic'] = topic.name
    }
    for (const entry of configEntries(node)) {
      if (entry.key) config[entry.key] = entry.value
    }

    files.push({
      path: `connectors/${name}.json`,
      content: `${JSON.stringify({ name, config }, null, 2)}\n`,
    })
  }

  return files
}
