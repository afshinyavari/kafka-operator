import { nanoid } from 'nanoid'
import { Plus, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { emptyPredicate } from '../expressions/types'
import type { Predicate } from '../expressions/types'
import type { BranchItem } from '../nodes'
import { PredicateBuilder } from './PredicateBuilder'

interface BranchListEditorProps {
  nodeId: string
  branches: BranchItem[]
  fieldSuggestions: string[]
}

/** Inspector editor for a branch node — each branch is a named output with a predicate. */
export function BranchListEditor({
  nodeId,
  branches,
  fieldSuggestions,
}: BranchListEditorProps) {
  const updateBranches = useEditorStore((s) => s.updateBranches)

  const patch = (id: string, change: Partial<BranchItem>) =>
    updateBranches(
      nodeId,
      branches.map((b) => (b.id === id ? { ...b, ...change } : b)),
    )
  const remove = (id: string) =>
    updateBranches(
      nodeId,
      branches.filter((b) => b.id !== id),
    )
  const add = () =>
    updateBranches(nodeId, [
      ...branches,
      {
        id: nanoid(),
        name: `branch-${branches.length + 1}`,
        predicate: emptyPredicate(),
      },
    ])

  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-xs font-medium text-slate-600">Branches</span>

      {branches.map((branch) => (
        <div
          key={branch.id}
          className="flex flex-col gap-1.5 rounded border border-slate-200 bg-slate-50 p-2"
        >
          <div className="flex items-center gap-1">
            <input
              value={branch.name}
              placeholder="branch name"
              onChange={(e) => patch(branch.id, { name: e.target.value })}
              className="w-full rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500"
            />
            <button
              type="button"
              onClick={() => remove(branch.id)}
              aria-label="Remove branch"
              className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
            >
              <X className="h-3 w-3" />
            </button>
          </div>
          <PredicateBuilder
            predicate={branch.predicate ?? emptyPredicate()}
            fieldSuggestions={fieldSuggestions}
            onChange={(p: Predicate) => patch(branch.id, { predicate: p })}
          />
        </div>
      ))}

      <button
        type="button"
        onClick={add}
        className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
      >
        <Plus className="h-3.5 w-3.5" />
        Add branch
      </button>
      <span className="text-[11px] text-slate-400">
        Each branch becomes an output port; records matching its predicate are
        routed there.
      </span>
    </div>
  )
}
