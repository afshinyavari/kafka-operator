/**
 * The node-type taxonomy. Every node type the editor supports is described by a
 * plain `NodeSchema` data object — there are no per-type React components. A
 * single generic component renders any schema, and a single inspector edits any
 * schema's properties. Adding a node type means adding one `NodeSchema`.
 */

import type { Predicate } from '../expressions/types'

export type Category =
  | 'source'
  | 'stateless'
  | 'stateful'
  | 'join'
  | 'sink'
  | 'connector'

/** The kind of data flowing through a port / along an edge. */
export type DataKind = 'KStream' | 'KTable' | 'KGroupedStream'

export type PropertyKind =
  | 'text'
  | 'number'
  | 'select'
  | 'boolean'
  | 'branchList'
  | 'predicate'
  | 'recordTypeRef'
  | 'keyExpression'
  | 'valueMapping'
  | 'window'
  | 'topicRef'
  | 'aggregation'
  | 'keyValue'

export interface PropertyOption {
  label: string
  value: string
}

export interface PropertySpec {
  /** Key under which the value is stored in a node's config object. */
  key: string
  label: string
  kind: PropertyKind
  required?: boolean
  default?: unknown
  placeholder?: string
  help?: string
  /** Options for `kind: 'select'`. */
  options?: PropertyOption[]
}

/** What an output port emits — a fixed kind, or 'inherit' (same as the node's input). */
export type OutputKind = DataKind | 'inherit'

export interface PortSpec {
  /** Stable id, unique within the node (e.g. 'in', 'out', 'left'). */
  id: string
  label?: string
  /** Input ports: the data kinds this port can receive. */
  accepts?: DataKind[]
  /** Output ports: what this port emits ('inherit' = same kind as the input). */
  produces?: OutputKind
}

/** Output ports derived from a config array (one port per item) — used by branch. */
export interface DynamicOutputs {
  /** Config key holding the array; each item becomes one output port. */
  configKey: string
  produces: DataKind
}

export interface NodeSchema {
  /** Unique node-type id (e.g. 'source', 'filter', 'sink'). */
  type: string
  label: string
  category: Category
  description: string
  /** Icon name resolved by the `NodeIcon` component. */
  icon: string
  inputs: PortSpec[]
  outputs: PortSpec[]
  /** When set, output ports are derived from config instead of `outputs`. */
  dynamicOutputs?: DynamicOutputs
  properties: PropertySpec[]
}

/** User-edited configuration values for a node, keyed by `PropertySpec.key`. */
export type NodeConfig = Record<string, unknown>

/** One branch of a `branch` node — the shape stored in `config.branches`. */
export interface BranchItem {
  id: string
  name: string
  /** Records matching this predicate are routed to this branch. */
  predicate?: Predicate
}
