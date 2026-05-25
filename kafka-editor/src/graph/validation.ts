import { getNodeSchema, resolveInputs, resolveOutputs } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import { inferPortKinds } from './typeInference'

/**
 * The graph validation engine — pure functions that surface problems in a
 * topology. Used by the problems panel; later, codegen will refuse to run
 * while there are errors.
 */

export type Severity = 'error' | 'warning'

export interface Diagnostic {
  severity: Severity
  message: string
  /** The node the problem belongs to, if any. */
  nodeId?: string
}

function nodeLabel(node: KafkaNode): string {
  const label = node.data.config.label
  return typeof label === 'string' && label.length > 0
    ? label
    : (node.type ?? 'node')
}

/** Detect a cycle in the directed graph (a topology must be a DAG). */
function hasCycle(nodes: KafkaNode[], edges: KafkaEdge[]): boolean {
  const adjacency = new Map<string, string[]>()
  for (const edge of edges) {
    const list = adjacency.get(edge.source) ?? []
    list.push(edge.target)
    adjacency.set(edge.source, list)
  }
  const WHITE = 0
  const GRAY = 1
  const BLACK = 2
  const color = new Map<string, number>()
  for (const node of nodes) color.set(node.id, WHITE)
  let cyclic = false

  function visit(id: string): void {
    color.set(id, GRAY)
    for (const next of adjacency.get(id) ?? []) {
      const c = color.get(next)
      if (c === GRAY) {
        cyclic = true
        return
      }
      if (c === WHITE) visit(next)
    }
    color.set(id, BLACK)
  }

  for (const node of nodes) {
    if (color.get(node.id) === WHITE) visit(node.id)
  }
  return cyclic
}

/** Validate a topology and return all diagnostics, errors and warnings. */
export function validateGraph(
  nodes: KafkaNode[],
  edges: KafkaEdge[],
): Diagnostic[] {
  const diagnostics: Diagnostic[] = []
  const portKinds = inferPortKinds(nodes, edges)
  const nodeById = new Map(nodes.map((n) => [n.id, n]))

  for (const node of nodes) {
    const schema = getNodeSchema(node.type ?? '')
    if (!schema) continue
    const name = nodeLabel(node)

    // Required properties must be set.
    for (const prop of schema.properties) {
      if (!prop.required) continue
      const value = node.data.config[prop.key]
      if (value === undefined || value === null || value === '') {
        diagnostics.push({
          severity: 'error',
          nodeId: node.id,
          message: `${name}: "${prop.label}" is required`,
        })
      }
    }

    // Every input port should have an incoming edge.
    for (const port of resolveInputs(schema)) {
      const connected = edges.some(
        (e) => e.target === node.id && e.targetHandle === port.id,
      )
      if (!connected) {
        diagnostics.push({
          severity: 'warning',
          nodeId: node.id,
          message: `${name}: input "${port.label ?? port.id}" is not connected`,
        })
      }
    }

    // Every output port should be consumed.
    for (const port of resolveOutputs(schema, node.data.config)) {
      const consumed = edges.some(
        (e) => e.source === node.id && e.sourceHandle === port.id,
      )
      if (!consumed) {
        diagnostics.push({
          severity: 'warning',
          nodeId: node.id,
          message: `${name}: output "${port.label ?? port.id}" is not used`,
        })
      }
    }
  }

  // Edges must carry a data kind the target accepts.
  for (const edge of edges) {
    const target = nodeById.get(edge.target)
    const schema = target ? getNodeSchema(target.type ?? '') : undefined
    if (!target || !schema) continue
    const inPort = resolveInputs(schema).find((p) => p.id === edge.targetHandle)
    const sourceKind = edge.sourceHandle
      ? portKinds.get(`${edge.source}:${edge.sourceHandle}`)
      : undefined
    if (inPort?.accepts && sourceKind && !inPort.accepts.includes(sourceKind)) {
      diagnostics.push({
        severity: 'error',
        nodeId: edge.target,
        message: `${nodeLabel(target)}: input "${
          inPort.label ?? inPort.id
        }" receives ${sourceKind}, expected ${inPort.accepts.join(' or ')}`,
      })
    }
  }

  if (hasCycle(nodes, edges)) {
    diagnostics.push({
      severity: 'error',
      message: 'The topology contains a cycle — it must be acyclic',
    })
  }

  return diagnostics
}
