import type { NodeTypes } from '@xyflow/react'
import { allNodeSchemas } from '../nodes'
import { GenericNode } from './GenericNode'

/**
 * Every registered node type renders with the same schema-driven component.
 * The `as unknown as NodeTypes` cast bridges our specifically-typed node
 * component to React Flow's loosely-typed registry (a known v12 TS friction).
 */
export const nodeTypes = Object.fromEntries(
  allNodeSchemas().map((schema) => [schema.type, GenericNode]),
) as unknown as NodeTypes
