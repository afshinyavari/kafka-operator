import type { ChangeEvent, ReactNode } from 'react'
import { ExternalLink, Trash2 } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { useViewStore } from '../state/viewStore'
import { getNodeSchema, resolveInputs } from '../nodes'
import type { BranchItem, PropertySpec } from '../nodes'
import {
  emptyPredicate,
  isPredicate,
  isValueExpression,
  newFieldExpression,
} from '../expressions/types'
import type { ValueMappingEntry } from '../expressions/types'
import { flattenFieldPaths } from '../model/recordTypes'
import { defaultWindow, isWindowConfig } from '../model/windows'
import type { AggregateField } from '../model/aggregations'
import type { ConnectorConfigEntry } from '../model/connectors'
import {
  inputRecordTypeId,
  recordTypeIdForInput,
} from '../graph/recordTypeInference'
import { BranchListEditor } from './BranchListEditor'
import { PredicateBuilder } from './PredicateBuilder'
import { ExpressionBuilder } from './ExpressionBuilder'
import { ValueMappingBuilder } from './ValueMappingBuilder'
import { WindowBuilder } from './WindowBuilder'
import { AggregationBuilder } from './AggregationBuilder'
import { KeyValueEditor } from './KeyValueEditor'

const inputClass =
  'w-full rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500'

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="flex flex-col gap-1">
      <span className="text-xs font-medium text-slate-600">{label}</span>
      {children}
    </label>
  )
}

interface PropertyFieldProps {
  prop: PropertySpec
  value: unknown
  onChange: (value: unknown) => void
}

/** Renders a single config field based on its `PropertySpec.kind`. */
function PropertyField({ prop, value, onChange }: PropertyFieldProps) {
  const label = prop.required ? `${prop.label} *` : prop.label
  let control: ReactNode

  switch (prop.kind) {
    case 'number':
      control = (
        <input
          type="number"
          className={inputClass}
          value={typeof value === 'number' ? value : ''}
          placeholder={prop.placeholder}
          onChange={(e: ChangeEvent<HTMLInputElement>) =>
            onChange(e.target.value === '' ? undefined : Number(e.target.value))
          }
        />
      )
      break
    case 'boolean':
      control = (
        <input
          type="checkbox"
          className="h-4 w-4"
          checked={value === true}
          onChange={(e: ChangeEvent<HTMLInputElement>) =>
            onChange(e.target.checked)
          }
        />
      )
      break
    case 'select':
      control = (
        <select
          className={inputClass}
          value={typeof value === 'string' ? value : ''}
          onChange={(e: ChangeEvent<HTMLSelectElement>) =>
            onChange(e.target.value)
          }
        >
          <option value="">—</option>
          {prop.options?.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
      )
      break
    case 'branchList':
    case 'predicate':
    case 'recordTypeRef':
    case 'keyExpression':
    case 'valueMapping':
    case 'window':
    case 'topicRef':
    case 'aggregation':
    case 'keyValue':
      // Handled by the inspector directly, not via PropertyField.
      control = null
      break
    case 'text':
      control = (
        <input
          type="text"
          className={inputClass}
          value={typeof value === 'string' ? value : ''}
          placeholder={prop.placeholder}
          onChange={(e: ChangeEvent<HTMLInputElement>) =>
            onChange(e.target.value)
          }
        />
      )
      break
  }

  return (
    <div className="flex flex-col gap-1">
      <Field label={label}>{control}</Field>
      {prop.help && (
        <span className="text-[11px] text-slate-400">{prop.help}</span>
      )}
    </div>
  )
}

/** Right sidebar: edits the configuration of the selected node. */
export function Inspector() {
  const nodes = useEditorStore((s) => s.nodes)
  const edges = useEditorStore((s) => s.edges)
  const recordTypes = useEditorStore((s) => s.recordTypes)
  const catalog = useEditorStore((s) => s.catalog)
  const selectedNodeId = useEditorStore((s) => s.selectedNodeId)
  const updateNodeConfig = useEditorStore((s) => s.updateNodeConfig)
  const deleteNode = useEditorStore((s) => s.deleteNode)
  const openClusterAt = useViewStore((s) => s.openClusterAt)

  const node = nodes.find((n) => n.id === selectedNodeId)

  if (!node) {
    return (
      <aside className="flex w-72 shrink-0 flex-col border-l border-slate-200 bg-white p-4">
        <h2 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          Inspector
        </h2>
        <p className="mt-3 text-sm text-slate-400">
          Select a node to edit its properties.
        </p>
      </aside>
    )
  }

  const schema = getNodeSchema(node.type ?? '')
  const config = node.data.config
  const label = typeof config.label === 'string' ? config.label : ''

  // Field-path suggestions from the record type(s) flowing into this node.
  // Multi-input nodes (joins) prefix each port's fields with `left.` / `right.`.
  const resolveRt = (id: string) => recordTypes.find((rt) => rt.id === id)
  const pathsFor = (rtId: string | undefined, prefix: string): string[] => {
    const rt = rtId ? resolveRt(rtId) : undefined
    return rt ? flattenFieldPaths(rt, resolveRt).map((p) => prefix + p) : []
  }
  const schemaInputs = schema ? resolveInputs(schema) : []
  const fieldSuggestions =
    schemaInputs.length > 1
      ? schemaInputs.flatMap((port) =>
          pathsFor(
            recordTypeIdForInput(node.id, port.id, nodes, edges, catalog),
            `${port.id}.`,
          ),
        )
      : pathsFor(inputRecordTypeId(node.id, nodes, edges, catalog), '')

  return (
    <aside className="flex w-72 shrink-0 flex-col gap-4 overflow-y-auto border-l border-slate-200 bg-white p-4">
      <div>
        <h2 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          {schema?.label ?? node.type}
        </h2>
        {schema && (
          <p className="mt-1 text-[11px] text-slate-400">{schema.description}</p>
        )}
      </div>

      <Field label="Label">
        <input
          className={inputClass}
          value={label}
          onChange={(e) => updateNodeConfig(node.id, { label: e.target.value })}
        />
      </Field>

      {schema?.properties.map((prop) => {
        if (prop.kind === 'predicate') {
          const value = config[prop.key]
          return (
            <PredicateBuilder
              key={prop.key}
              predicate={isPredicate(value) ? value : emptyPredicate()}
              fieldSuggestions={fieldSuggestions}
              onChange={(p) => updateNodeConfig(node.id, { [prop.key]: p })}
            />
          )
        }
        if (prop.kind === 'branchList') {
          return (
            <BranchListEditor
              key={prop.key}
              nodeId={node.id}
              branches={
                Array.isArray(config[prop.key])
                  ? (config[prop.key] as BranchItem[])
                  : []
              }
              fieldSuggestions={fieldSuggestions}
            />
          )
        }
        if (prop.kind === 'keyExpression') {
          const value = config[prop.key]
          return (
            <div key={prop.key} className="flex flex-col gap-1">
              <span className="text-xs font-medium text-slate-600">
                {prop.label}
              </span>
              <ExpressionBuilder
                expression={
                  isValueExpression(value) ? value : newFieldExpression()
                }
                fieldSuggestions={fieldSuggestions}
                onChange={(expr) =>
                  updateNodeConfig(node.id, { [prop.key]: expr })
                }
              />
            </div>
          )
        }
        if (prop.kind === 'valueMapping') {
          const value = config[prop.key]
          return (
            <ValueMappingBuilder
              key={prop.key}
              nodeId={node.id}
              configKey={prop.key}
              label={prop.label}
              entries={
                Array.isArray(value) ? (value as ValueMappingEntry[]) : []
              }
              fieldSuggestions={fieldSuggestions}
            />
          )
        }
        if (prop.kind === 'aggregation') {
          const value = config[prop.key]
          return (
            <AggregationBuilder
              key={prop.key}
              nodeId={node.id}
              configKey={prop.key}
              fields={Array.isArray(value) ? (value as AggregateField[]) : []}
              fieldSuggestions={fieldSuggestions}
            />
          )
        }
        if (prop.kind === 'keyValue') {
          const value = config[prop.key]
          return (
            <KeyValueEditor
              key={prop.key}
              nodeId={node.id}
              configKey={prop.key}
              label={prop.label}
              entries={
                Array.isArray(value) ? (value as ConnectorConfigEntry[]) : []
              }
            />
          )
        }
        if (prop.kind === 'window') {
          const value = config[prop.key]
          return (
            <WindowBuilder
              key={prop.key}
              window={isWindowConfig(value) ? value : defaultWindow()}
              onChange={(w) => updateNodeConfig(node.id, { [prop.key]: w })}
            />
          )
        }
        if (prop.kind === 'topicRef') {
          const current = config[prop.key]
          const topicName =
            typeof current === 'string'
              ? catalog.topics.find((t) => t.id === current)?.name
              : undefined
          return (
            <div key={prop.key} className="flex flex-col gap-1">
              <Field label={prop.required ? `${prop.label} *` : prop.label}>
                <select
                  className={inputClass}
                  value={typeof current === 'string' ? current : ''}
                  onChange={(e) =>
                    updateNodeConfig(node.id, { [prop.key]: e.target.value })
                  }
                >
                  <option value="">— none —</option>
                  {catalog.topics.map((t) => (
                    <option key={t.id} value={t.id}>
                      {t.name}
                    </option>
                  ))}
                </select>
              </Field>
              {topicName && (
                <button
                  type="button"
                  onClick={() =>
                    openClusterAt({ kind: 'topic', name: topicName })
                  }
                  className="flex items-center gap-1 self-start text-[11px] text-slate-500 hover:text-slate-800"
                >
                  <ExternalLink className="h-3 w-3" />
                  Browse “{topicName}” in cluster
                </button>
              )}
            </div>
          )
        }
        if (prop.kind === 'recordTypeRef') {
          const current = config[prop.key]
          // When the node references a catalog topic, an empty value inherits
          // that topic's value type — show it so this field reads as an override.
          const topicId = config.topicId
          const topic =
            typeof topicId === 'string'
              ? catalog.topics.find((t) => t.id === topicId)
              : undefined
          const inheritedName = topic?.valueRecordTypeId
            ? recordTypes.find((rt) => rt.id === topic.valueRecordTypeId)?.name
            : undefined
          return (
            <div key={prop.key} className="flex flex-col gap-1">
              <Field label={prop.label}>
                <select
                  className={inputClass}
                  value={typeof current === 'string' ? current : ''}
                  onChange={(e) =>
                    updateNodeConfig(node.id, { [prop.key]: e.target.value })
                  }
                >
                  <option value="">
                    {inheritedName
                      ? `— inherit from topic: ${inheritedName} —`
                      : '— none —'}
                  </option>
                  {recordTypes.map((rt) => (
                    <option key={rt.id} value={rt.id}>
                      {rt.name}
                    </option>
                  ))}
                </select>
              </Field>
              {prop.help && (
                <span className="text-[11px] text-slate-400">{prop.help}</span>
              )}
            </div>
          )
        }
        return (
          <PropertyField
            key={prop.key}
            prop={prop}
            value={config[prop.key]}
            onChange={(value) => updateNodeConfig(node.id, { [prop.key]: value })}
          />
        )
      })}

      {schema && schema.properties.length === 0 && (
        <p className="text-[11px] text-slate-400">No configurable properties.</p>
      )}

      <button
        type="button"
        onClick={() => deleteNode(node.id)}
        className="mt-2 flex items-center justify-center gap-1.5 rounded border border-rose-300 px-2 py-1.5 text-xs font-medium text-rose-600 hover:bg-rose-50"
      >
        <Trash2 className="h-3.5 w-3.5" />
        Delete node
      </button>
    </aside>
  )
}
