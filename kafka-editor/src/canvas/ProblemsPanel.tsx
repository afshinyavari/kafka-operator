import { useMemo, useState } from 'react'
import {
  AlertCircle,
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
} from 'lucide-react'
import { useEditorStore } from '../state/store'
import { validateGraph } from '../graph/validation'

/** Collapsible panel below the canvas listing topology validation problems. */
export function ProblemsPanel() {
  const nodes = useEditorStore((s) => s.nodes)
  const edges = useEditorStore((s) => s.edges)
  const selectNode = useEditorStore((s) => s.selectNode)
  const [expanded, setExpanded] = useState(false)

  const diagnostics = useMemo(
    () => validateGraph(nodes, edges),
    [nodes, edges],
  )
  const errorCount = diagnostics.filter((d) => d.severity === 'error').length
  const warningCount = diagnostics.length - errorCount

  return (
    <div className="shrink-0 border-t border-slate-200 bg-white text-xs">
      <button
        type="button"
        onClick={() => setExpanded((e) => !e)}
        className="flex w-full items-center gap-2 px-3 py-1.5 text-slate-600 hover:bg-slate-50"
      >
        {expanded ? (
          <ChevronDown className="h-3.5 w-3.5" />
        ) : (
          <ChevronUp className="h-3.5 w-3.5" />
        )}
        <span className="font-medium">Problems</span>
        {diagnostics.length === 0 ? (
          <span className="flex items-center gap-1 text-emerald-600">
            <CheckCircle2 className="h-3.5 w-3.5" />
            none
          </span>
        ) : (
          <span className="flex items-center gap-3">
            {errorCount > 0 && (
              <span className="flex items-center gap-1 text-rose-600">
                <AlertCircle className="h-3.5 w-3.5" />
                {errorCount}
              </span>
            )}
            {warningCount > 0 && (
              <span className="flex items-center gap-1 text-amber-600">
                <AlertTriangle className="h-3.5 w-3.5" />
                {warningCount}
              </span>
            )}
          </span>
        )}
      </button>

      {expanded && diagnostics.length > 0 && (
        <div className="max-h-40 overflow-y-auto border-t border-slate-100">
          {diagnostics.map((d, i) => (
            <button
              key={i}
              type="button"
              onClick={() => d.nodeId && selectNode(d.nodeId)}
              className="flex w-full items-center gap-2 px-3 py-1 text-left hover:bg-slate-50"
            >
              {d.severity === 'error' ? (
                <AlertCircle className="h-3.5 w-3.5 shrink-0 text-rose-500" />
              ) : (
                <AlertTriangle className="h-3.5 w-3.5 shrink-0 text-amber-500" />
              )}
              <span className="text-slate-700">{d.message}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
