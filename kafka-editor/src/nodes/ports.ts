import type { BranchItem, NodeConfig, NodeSchema, PortSpec } from './types'

/**
 * Resolve a node's output ports. Most nodes have static outputs; nodes with
 * `dynamicOutputs` (branch) derive one output port per config-list item.
 */
export function resolveOutputs(
  schema: NodeSchema,
  config: NodeConfig,
): PortSpec[] {
  const dynamic = schema.dynamicOutputs
  if (!dynamic) return schema.outputs

  const list = config[dynamic.configKey]
  if (!Array.isArray(list)) return []
  return (list as BranchItem[]).map((item) => ({
    id: item.id,
    label: item.name,
    produces: dynamic.produces,
  }))
}

/** Resolve a node's input ports (always static in M2). */
export function resolveInputs(schema: NodeSchema): PortSpec[] {
  return schema.inputs
}
