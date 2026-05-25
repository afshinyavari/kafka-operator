import { useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { nodeSchemasByCategory } from '../nodes'
import type { Category, NodeSchema } from '../nodes'
import { CATEGORY_LABEL } from '../canvas/categoryStyles'
import { PaletteItem } from './PaletteItem'

/** Category display order in the palette. */
const CATEGORY_ORDER: Category[] = [
  'source',
  'stateless',
  'stateful',
  'join',
  'sink',
  'connector',
]

/** Left sidebar: searchable, collapsible node types grouped by category. */
export function Palette() {
  const [query, setQuery] = useState('')
  const [collapsed, setCollapsed] = useState<Set<Category>>(new Set())
  const grouped = nodeSchemasByCategory()

  const q = query.trim().toLowerCase()
  const matches = (schema: NodeSchema) =>
    q === '' ||
    schema.label.toLowerCase().includes(q) ||
    schema.description.toLowerCase().includes(q)

  const toggle = (category: Category) =>
    setCollapsed((prev) => {
      const next = new Set(prev)
      if (next.has(category)) next.delete(category)
      else next.add(category)
      return next
    })

  const visibleGroups = CATEGORY_ORDER.map((category) => ({
    category,
    schemas: (grouped.get(category) ?? []).filter(matches),
  })).filter((group) => group.schemas.length > 0)

  return (
    <aside className="flex w-60 shrink-0 flex-col gap-3 overflow-y-auto border-r border-slate-200 bg-slate-50 p-3">
      <div>
        <h2 className="text-xs font-semibold tracking-wide text-slate-500 uppercase">
          Nodes
        </h2>
        <p className="mt-1 text-[11px] text-slate-400">
          Drag a node onto the canvas.
        </p>
      </div>

      <input
        type="search"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Search nodes…"
        className="w-full rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500"
      />

      {visibleGroups.length === 0 && (
        <p className="text-[11px] text-slate-400">
          No nodes match “{query.trim()}”.
        </p>
      )}

      {visibleGroups.map(({ category, schemas }) => {
        // A search query keeps every matching section expanded.
        const isCollapsed = q === '' && collapsed.has(category)
        return (
          <div key={category} className="flex flex-col gap-2">
            <button
              type="button"
              onClick={() => toggle(category)}
              className="flex items-center gap-1 text-[11px] font-semibold tracking-wide text-slate-400 uppercase hover:text-slate-600"
            >
              {isCollapsed ? (
                <ChevronRight className="h-3 w-3" />
              ) : (
                <ChevronDown className="h-3 w-3" />
              )}
              {CATEGORY_LABEL[category]}
              <span className="font-normal text-slate-300">
                {schemas.length}
              </span>
            </button>
            {!isCollapsed &&
              schemas.map((schema) => (
                <PaletteItem key={schema.type} schema={schema} />
              ))}
          </div>
        )
      })}
    </aside>
  )
}
