import type { Category } from '../nodes'

/**
 * Per-category presentation. Tailwind class strings are full literals (not
 * built dynamically) so Tailwind's content scanner detects them.
 */

export const CATEGORY_LABEL: Record<Category, string> = {
  source: 'Sources',
  stateless: 'Stateless',
  stateful: 'Stateful',
  join: 'Joins',
  sink: 'Sinks',
  connector: 'Connectors',
}

/** Card border + background classes for a node, by category. */
export const CATEGORY_STYLE: Record<Category, string> = {
  source: 'border-emerald-400 bg-emerald-50',
  stateless: 'border-sky-400 bg-sky-50',
  stateful: 'border-violet-400 bg-violet-50',
  join: 'border-amber-400 bg-amber-50',
  sink: 'border-rose-400 bg-rose-50',
  connector: 'border-slate-400 bg-slate-50',
}

/** Hex colors for the minimap, by category. */
export const CATEGORY_HEX: Record<Category, string> = {
  source: '#10b981',
  stateless: '#0ea5e9',
  stateful: '#8b5cf6',
  join: '#f59e0b',
  sink: '#f43f5e',
  connector: '#64748b',
}
