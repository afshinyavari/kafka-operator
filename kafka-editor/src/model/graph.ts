import type { Edge, Node } from '@xyflow/react'
import type { NodeConfig } from '../nodes/types'

/**
 * The editor's live, in-memory graph types. These wrap React Flow's `Node` and
 * `Edge`; the serialized form lives in `./project`. `KafkaNodeData` is a `type`
 * (not an `interface`) so it satisfies React Flow's `Record<string, unknown>`
 * data constraint.
 */

export type KafkaNodeData = {
  config: NodeConfig
}

export type KafkaNode = Node<KafkaNodeData>
export type KafkaEdge = Edge
