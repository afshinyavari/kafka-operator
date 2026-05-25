import { CURRENT_SCHEMA_VERSION } from './project'
import type {
  ProjectDocument,
  ProjectEdge,
  ProjectNode,
  ProjectViewport,
} from './project'
import type { RecordType } from './recordTypes'
import type { Catalog } from './catalog'
import { emptyCatalog } from './catalog'
import type { Environment } from './environment'
import { emptyEnvironment } from './environment'
import type { KafkaEdge, KafkaNode } from './graph'

/** The project-global collections the editor keeps alongside the graph. */
export interface ProjectExtras {
  recordTypes: RecordType[]
  catalog: Catalog
  environment: Environment
}

/**
 * Project the editor's live React Flow graph into a serializable document.
 * React Flow runtime fields (selection, measured size, dragging) are dropped.
 */
export function toDocument(
  base: ProjectDocument,
  nodes: KafkaNode[],
  edges: KafkaEdge[],
  viewport: ProjectViewport,
  extras: ProjectExtras,
): ProjectDocument {
  const projectNodes: ProjectNode[] = nodes.map((node) => ({
    id: node.id,
    type: node.type ?? 'unknown',
    position: { x: node.position.x, y: node.position.y },
    config: { ...node.data.config },
  }))

  const projectEdges: ProjectEdge[] = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    sourceHandle: edge.sourceHandle ?? null,
    target: edge.target,
    targetHandle: edge.targetHandle ?? null,
  }))

  return {
    schemaVersion: CURRENT_SCHEMA_VERSION,
    id: base.id,
    meta: { ...base.meta, updatedAt: new Date().toISOString() },
    nodes: projectNodes,
    edges: projectEdges,
    viewport: { x: viewport.x, y: viewport.y, zoom: viewport.zoom },
    recordTypes: extras.recordTypes,
    catalog: extras.catalog,
    environment: extras.environment,
  }
}

/** Rebuild the editor's live React Flow graph from a serialized document. */
export function fromDocument(doc: ProjectDocument): {
  nodes: KafkaNode[]
  edges: KafkaEdge[]
  recordTypes: RecordType[]
  catalog: Catalog
  environment: Environment
} {
  const nodes: KafkaNode[] = doc.nodes.map((node) => ({
    id: node.id,
    type: node.type,
    position: { x: node.position.x, y: node.position.y },
    data: { config: { ...node.config } },
  }))

  const edges: KafkaEdge[] = doc.edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    sourceHandle: edge.sourceHandle,
    target: edge.target,
    targetHandle: edge.targetHandle,
  }))

  return {
    nodes,
    edges,
    recordTypes: doc.recordTypes ?? [],
    catalog: doc.catalog ?? emptyCatalog(),
    environment: doc.environment ?? emptyEnvironment(),
  }
}
