import { MarkerType } from '@xyflow/react'
import type { DataKind } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import { inferPortKinds } from '../graph/typeInference'

/** Edge / port colors per data kind. */
export const DATAKIND_HEX: Record<DataKind, string> = {
  KStream: '#3b82f6',
  KTable: '#8b5cf6',
  KGroupedStream: '#14b8a6',
}

/** Edge dash pattern per data kind (solid when absent). */
const DATAKIND_DASH: Partial<Record<DataKind, string>> = {
  KTable: '6 4',
  KGroupedStream: '1 3',
}

/** Color for an edge whose data kind cannot yet be determined. */
const UNKNOWN_HEX = '#94a3b8'

/**
 * Decorate edges with kind-based styling for rendering. The data kind is taken
 * from inference (so an edge leaving a polymorphic node reflects what actually
 * flows into it). KStream edges are solid blue; KTable edges are dashed violet.
 * The store keeps edges undecorated; this runs as a render-time transform.
 */
export function styleEdges(edges: KafkaEdge[], nodes: KafkaNode[]): KafkaEdge[] {
  const portKinds = inferPortKinds(nodes, edges)
  return edges.map((edge) => {
    const kind = edge.sourceHandle
      ? portKinds.get(`${edge.source}:${edge.sourceHandle}`)
      : undefined
    const color = kind ? DATAKIND_HEX[kind] : UNKNOWN_HEX
    return {
      ...edge,
      data: { ...edge.data, dataKind: kind },
      style: {
        stroke: color,
        strokeWidth: 2,
        strokeDasharray: kind ? DATAKIND_DASH[kind] : undefined,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color,
        width: 18,
        height: 18,
      },
    }
  })
}
