import { getNodeSchema, resolveInputs, resolveOutputs } from '../nodes'
import type { DataKind } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'

/**
 * Infers the resolved `DataKind` of every node output port.
 *
 * Most ports have a fixed kind. Polymorphic operators (filter, filterNot,
 * mapValues) declare `produces: 'inherit'` and take the kind flowing into their
 * single input port — so a `KTable` stays a `KTable` through them. This walks
 * the graph to resolve those.
 *
 * Returns a map keyed by `${nodeId}:${portId}`. A port whose kind cannot be
 * determined (e.g. a polymorphic node with no upstream connection yet) is
 * simply absent from the map.
 */
export function inferPortKinds(
  nodes: KafkaNode[],
  edges: KafkaEdge[],
): Map<string, DataKind> {
  const resolved = new Map<string, DataKind>()
  const nodeById = new Map(nodes.map((n) => [n.id, n]))
  const inProgress = new Set<string>()

  function outputKind(nodeId: string, portId: string): DataKind | undefined {
    const key = `${nodeId}:${portId}`
    const cached = resolved.get(key)
    if (cached) return cached

    const node = nodeById.get(nodeId)
    if (!node) return undefined
    const schema = getNodeSchema(node.type ?? '')
    if (!schema) return undefined
    const port = resolveOutputs(schema, node.data.config).find(
      (p) => p.id === portId,
    )
    if (!port || !port.produces) return undefined

    if (port.produces !== 'inherit') {
      resolved.set(key, port.produces)
      return port.produces
    }

    // 'inherit': take the kind flowing into the node's single input port.
    if (inProgress.has(nodeId)) return undefined // cycle guard
    inProgress.add(nodeId)
    let inherited: DataKind | undefined
    const inputPort = resolveInputs(schema)[0]
    if (inputPort) {
      const incoming = edges.find(
        (e) => e.target === nodeId && e.targetHandle === inputPort.id,
      )
      if (incoming?.sourceHandle) {
        inherited = outputKind(incoming.source, incoming.sourceHandle)
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
      outputKind(node.id, port.id)
    }
  }

  return resolved
}
