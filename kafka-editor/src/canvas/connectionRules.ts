import type { Connection, Edge } from '@xyflow/react'
import { getNodeSchema, resolveInputs, resolveOutputs } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import { inferPortKinds } from '../graph/typeInference'

/** A connection candidate — React Flow passes a `Connection` or an `Edge`. */
type ConnectionLike = Connection | Edge

/**
 * Whether a proposed connection is allowed. Enforces:
 *  - no self-connections,
 *  - each input port accepts at most one edge (cardinality),
 *  - the source port's data kind is accepted by the target port.
 *
 * Kind compatibility uses inference, so the kind leaving a polymorphic node
 * reflects what flows into it. When the source kind is not yet determinable
 * (a polymorphic node with no upstream connection) the kind check is skipped —
 * it resolves once the upstream edge exists.
 */
export function validateConnection(
  connection: ConnectionLike,
  nodes: KafkaNode[],
  edges: KafkaEdge[],
): boolean {
  const { source, target, sourceHandle, targetHandle } = connection
  if (!source || !target || source === target) return false

  const sourceNode = nodes.find((n) => n.id === source)
  const targetNode = nodes.find((n) => n.id === target)
  if (!sourceNode || !targetNode) return false

  const sourceSchema = getNodeSchema(sourceNode.type ?? '')
  const targetSchema = getNodeSchema(targetNode.type ?? '')
  if (!sourceSchema || !targetSchema) return false

  const outPort = resolveOutputs(sourceSchema, sourceNode.data.config).find(
    (p) => p.id === sourceHandle,
  )
  const inPort = resolveInputs(targetSchema).find((p) => p.id === targetHandle)
  if (!outPort || !inPort) return false

  // Cardinality: an input port accepts a single incoming edge.
  const occupied = edges.some(
    (e) => e.target === target && e.targetHandle === targetHandle,
  )
  if (occupied) return false

  // Type compatibility: the source's (inferred) kind must be accepted.
  const sourceKind = sourceHandle
    ? inferPortKinds(nodes, edges).get(`${source}:${sourceHandle}`)
    : undefined
  if (sourceKind && inPort.accepts && !inPort.accepts.includes(sourceKind)) {
    return false
  }

  return true
}
