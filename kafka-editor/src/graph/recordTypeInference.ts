import { getNodeSchema, resolveInputs, resolveOutputs } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import type { Catalog } from '../model/catalog'
import { effectiveValueRecordTypeId, emptyCatalog } from '../model/catalog'

/**
 * Node types that pass the value record type through unchanged. Value-reshaping
 * operators (map, mapValues, flatMap, joins) are deliberately absent — they
 * terminate propagation until the M3.4 mapper builders describe the new shape.
 * `select-key` is included: it changes the key, not the value.
 */
const VALUE_PASSTHROUGH = new Set([
  'filter',
  'filter-not',
  'peek',
  'repartition',
  'branch',
  'to-stream',
  'to-table',
  'select-key',
  'group-by-key',
  'group-by',
])

/**
 * Infers which record type flows out of each node output port, propagating the
 * `valueRecordTypeId` declared on source nodes through pass-through operators.
 * Returns a map keyed by `${nodeId}:${portId}`; ports whose type is unknown are
 * absent.
 */
export function inferRecordTypes(
  nodes: KafkaNode[],
  edges: KafkaEdge[],
  catalog: Catalog = emptyCatalog(),
): Map<string, string> {
  const resolved = new Map<string, string>()
  const nodeById = new Map(nodes.map((n) => [n.id, n]))
  const inProgress = new Set<string>()

  function outputRecordType(
    nodeId: string,
    portId: string,
  ): string | undefined {
    const key = `${nodeId}:${portId}`
    const cached = resolved.get(key)
    if (cached) return cached

    const node = nodeById.get(nodeId)
    if (!node) return undefined
    const schema = getNodeSchema(node.type ?? '')
    if (!schema) return undefined

    // A source's value record type — its own override, else the topic's.
    const declared = effectiveValueRecordTypeId(node.data.config, catalog)
    if (declared) {
      resolved.set(key, declared)
      return declared
    }

    // Pass-through operators inherit from their single input.
    if (!VALUE_PASSTHROUGH.has(schema.type)) return undefined
    if (inProgress.has(nodeId)) return undefined // cycle guard
    inProgress.add(nodeId)
    let inherited: string | undefined
    const inputPort = resolveInputs(schema)[0]
    if (inputPort) {
      const incoming = edges.find(
        (e) => e.target === nodeId && e.targetHandle === inputPort.id,
      )
      if (incoming?.sourceHandle) {
        inherited = outputRecordType(incoming.source, incoming.sourceHandle)
      }
    }
    inProgress.delete(nodeId)
    if (inherited) resolved.set(key, inherited)
    return inherited
  }

  for (const node of nodes) {
    const schema = getNodeSchema(node.type ?? '')
    if (!schema) continue
    for (const port of resolveOutputs(schema, node.data.config)) {
      outputRecordType(node.id, port.id)
    }
  }

  return resolved
}

/** The record type id flowing into a specific input port of a node, if any. */
export function recordTypeIdForInput(
  nodeId: string,
  portId: string,
  nodes: KafkaNode[],
  edges: KafkaEdge[],
  catalog: Catalog = emptyCatalog(),
): string | undefined {
  const incoming = edges.find(
    (e) => e.target === nodeId && e.targetHandle === portId,
  )
  if (!incoming?.sourceHandle) return undefined
  return inferRecordTypes(nodes, edges, catalog).get(
    `${incoming.source}:${incoming.sourceHandle}`,
  )
}

/** The record type id flowing into a node's (first) input port, if any. */
export function inputRecordTypeId(
  nodeId: string,
  nodes: KafkaNode[],
  edges: KafkaEdge[],
  catalog: Catalog = emptyCatalog(),
): string | undefined {
  const node = nodes.find((n) => n.id === nodeId)
  if (!node) return undefined
  const schema = getNodeSchema(node.type ?? '')
  if (!schema) return undefined
  const inputPort = resolveInputs(schema)[0]
  if (!inputPort) return undefined
  return recordTypeIdForInput(nodeId, inputPort.id, nodes, edges, catalog)
}
