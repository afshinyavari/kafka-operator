import { Handle, Position, type NodeProps } from '@xyflow/react'
import { getNodeSchema, resolveInputs, resolveOutputs } from '../nodes'
import type { NodeConfig, NodeSchema } from '../nodes'
import type { KafkaNode } from '../model/graph'
import type { Catalog } from '../model/catalog'
import { useEditorStore } from '../state/store'
import { useRunStore } from '../run/runStore'
import { NodeIcon } from '../components/NodeIcon'
import { CATEGORY_STYLE } from './categoryStyles'
import { DATAKIND_HEX } from './edges'
import {
  describeExpression,
  describePredicate,
  isExpressionSet,
  isPredicate,
  isValueExpression,
} from '../expressions/types'
import { describeWindow, isWindowConfig } from '../model/windows'
import { describeAggregation } from '../model/aggregations'

/** Base styling for connection handles (overrides React Flow's defaults). */
const HANDLE_BASE = {
  width: 10,
  height: 10,
  border: '2px solid #ffffff',
} as const

/** Vertically distribute `count` handles down the side of a node. */
function handleTop(index: number, count: number): string {
  return `${((index + 1) / (count + 1)) * 100}%`
}

/** A one-line summary shown under the node title. */
function summarise(
  schema: NodeSchema,
  config: NodeConfig,
  catalog: Catalog,
): string {
  if (schema.dynamicOutputs) {
    const list = config[schema.dynamicOutputs.configKey]
    if (Array.isArray(list)) {
      const names = (list as { name: string }[]).map((b) => b.name)
      return names.length > 0
        ? `${names.length} branches: ${names.join(', ')}`
        : 'No branches yet'
    }
  }
  for (const prop of schema.properties) {
    const value = config[prop.key]
    if (prop.kind === 'predicate') {
      if (isPredicate(value) && value.conditions.length > 0) {
        return `${prop.label}: ${describePredicate(value)}`
      }
      continue
    }
    // recordTypeRef values are opaque ids — not useful in a one-line summary.
    if (prop.kind === 'recordTypeRef') continue
    if (prop.kind === 'topicRef') {
      const topic = catalog.topics.find((t) => t.id === value)
      if (topic) return `${prop.label}: ${topic.name}`
      continue
    }
    if (prop.kind === 'keyValue') continue
    if (prop.kind === 'keyExpression') {
      if (isValueExpression(value) && isExpressionSet(value)) {
        return `${prop.label}: ${describeExpression(value)}`
      }
      continue
    }
    if (prop.kind === 'valueMapping') {
      if (Array.isArray(value) && value.length > 0) {
        return `${prop.label}: ${value.length} field${
          value.length === 1 ? '' : 's'
        }`
      }
      continue
    }
    if (prop.kind === 'aggregation') {
      if (Array.isArray(value) && value.length > 0) {
        return `${prop.label}: ${describeAggregation(value)}`
      }
      continue
    }
    if (prop.kind === 'window') {
      if (isWindowConfig(value) && value.type !== 'none') {
        return `${prop.label}: ${describeWindow(value)}`
      }
      continue
    }
    if (typeof value === 'string' && value.length > 0) {
      return `${prop.label}: ${value}`
    }
  }
  return schema.description
}

/**
 * The single component that renders every node type, driven by its `NodeSchema`.
 * Output handles are colored by the data kind they emit; branch nodes get one
 * output handle per configured branch. After a run, a metrics strip shows the
 * per-node record counts.
 */
export function GenericNode({ id, type, data, selected }: NodeProps<KafkaNode>) {
  const schema = getNodeSchema(type)
  const catalog = useEditorStore((s) => s.catalog)
  const edges = useEditorStore((s) => s.edges)
  const runStatus = useRunStore((s) => s.status)
  const runMetrics = useRunStore((s) => s.metrics)

  if (!schema) {
    return (
      <div className="rounded-md border-2 border-rose-400 bg-rose-50 px-3 py-2 text-xs text-rose-700">
        Unknown node type: {type}
      </div>
    )
  }

  const config = data.config
  const label =
    typeof config.label === 'string' && config.label.length > 0
      ? config.label
      : schema.label
  const inputs = resolveInputs(schema)
  const outputs = resolveOutputs(schema, config)

  const showMetrics = runStatus === 'done' || runStatus === 'running'
  const recordsOut = runMetrics[id] ?? 0
  const recordsIn = edges
    .filter((e) => e.target === id)
    .reduce((sum, e) => sum + (runMetrics[e.source] ?? 0), 0)
  const dropped = Math.max(0, recordsIn - recordsOut)

  return (
    <div
      className={`relative w-[210px] rounded-md border-2 shadow-sm ${
        CATEGORY_STYLE[schema.category]
      } ${selected ? 'ring-2 ring-slate-500 ring-offset-1' : ''}`}
    >
      {inputs.map((port, index) => (
        <Handle
          key={port.id}
          id={port.id}
          type="target"
          position={Position.Left}
          title={port.label ?? port.id}
          style={{
            ...HANDLE_BASE,
            top: handleTop(index, inputs.length),
            background: '#64748b',
          }}
        />
      ))}

      <div className="flex items-center gap-2 px-3 py-2">
        <NodeIcon name={schema.icon} className="h-4 w-4 shrink-0 text-slate-700" />
        <span className="truncate text-sm font-semibold text-slate-800">
          {label}
        </span>
      </div>
      <div className="truncate border-t border-black/5 px-3 py-1 text-[11px] text-slate-500">
        {summarise(schema, config, catalog)}
      </div>

      {showMetrics && (
        <div className="flex items-center gap-3 border-t border-black/5 bg-slate-800 px-3 py-1 text-[10px] font-semibold text-white">
          {inputs.length > 0 && (
            <span title="records in">▼ {recordsIn}</span>
          )}
          {outputs.length > 0 && (
            <span title="records out">▲ {recordsOut}</span>
          )}
          {outputs.length > 0 && dropped > 0 && (
            <span title="dropped" className="text-rose-300">
              ✕ {dropped}
            </span>
          )}
        </div>
      )}

      {outputs.map((port, index) => (
        <Handle
          key={port.id}
          id={port.id}
          type="source"
          position={Position.Right}
          title={port.label ?? port.id}
          style={{
            ...HANDLE_BASE,
            top: handleTop(index, outputs.length),
            background:
              port.produces && port.produces !== 'inherit'
                ? DATAKIND_HEX[port.produces]
                : '#64748b',
          }}
        />
      ))}
    </div>
  )
}
