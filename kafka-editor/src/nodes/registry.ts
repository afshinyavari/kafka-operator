import type { Category, NodeSchema } from './types'

/**
 * The runtime catalog of node types. Schemas register themselves on import
 * (see `./schemas`); consumers look them up by type id.
 */
const registry = new Map<string, NodeSchema>()

/** Register a node schema. A repeated type id overwrites the prior schema. */
export function registerNode(schema: NodeSchema): void {
  registry.set(schema.type, schema)
}

export function getNodeSchema(type: string): NodeSchema | undefined {
  return registry.get(type)
}

export function allNodeSchemas(): NodeSchema[] {
  return [...registry.values()]
}

/** Group every registered schema by its category, for the palette. */
export function nodeSchemasByCategory(): Map<Category, NodeSchema[]> {
  const grouped = new Map<Category, NodeSchema[]>()
  for (const schema of registry.values()) {
    const list = grouped.get(schema.category)
    if (list) {
      list.push(schema)
    } else {
      grouped.set(schema.category, [schema])
    }
  }
  return grouped
}
