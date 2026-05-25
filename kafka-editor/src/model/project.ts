import { nanoid } from 'nanoid'
import type { RecordType } from './recordTypes'
import type { Catalog } from './catalog'
import { emptyCatalog } from './catalog'
import type { Environment } from './environment'
import { emptyEnvironment } from './environment'

/**
 * The serialized project document — the clean, versioned JSON contract for
 * persistence, file I/O, and (later) Quarkus code generation. It deliberately
 * contains no React Flow types so the model can evolve independently of the UI.
 */

export const CURRENT_SCHEMA_VERSION = 5

export interface ProjectMeta {
  name: string
  /** ISO-8601 timestamps. */
  createdAt: string
  updatedAt: string
}

export interface ProjectNode {
  id: string
  /** `NodeSchema.type`. */
  type: string
  position: { x: number; y: number }
  /** User-edited property values, keyed by `PropertySpec.key`. */
  config: Record<string, unknown>
}

export interface ProjectEdge {
  id: string
  source: string
  sourceHandle: string | null
  target: string
  targetHandle: string | null
}

export interface ProjectViewport {
  x: number
  y: number
  zoom: number
}

export interface ProjectDocument {
  schemaVersion: number
  id: string
  meta: ProjectMeta
  nodes: ProjectNode[]
  edges: ProjectEdge[]
  viewport: ProjectViewport
  /** Named record-type definitions (M3.2). */
  recordTypes: RecordType[]
  /** Topics and SerDes (M4). */
  catalog: Catalog
  /** Kafka cluster + schema registry the topology runs against (R4). */
  environment: Environment
}

/** Create a fresh, empty project document at the current schema version. */
export function createEmptyProject(): ProjectDocument {
  const now = new Date().toISOString()
  return {
    schemaVersion: CURRENT_SCHEMA_VERSION,
    id: nanoid(),
    meta: {
      name: 'Untitled topology',
      createdAt: now,
      updatedAt: now,
    },
    nodes: [],
    edges: [],
    viewport: { x: 0, y: 0, zoom: 1 },
    recordTypes: [],
    catalog: emptyCatalog(),
    environment: emptyEnvironment(),
  }
}
