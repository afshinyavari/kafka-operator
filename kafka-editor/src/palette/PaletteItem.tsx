import type { DragEvent } from 'react'
import type { NodeSchema } from '../nodes'
import { NodeIcon } from '../components/NodeIcon'
import { NODE_DRAG_MIME } from '../dnd'

interface PaletteItemProps {
  schema: NodeSchema
}

/** A draggable node type in the palette. Drop is handled by the canvas. */
export function PaletteItem({ schema }: PaletteItemProps) {
  const onDragStart = (event: DragEvent) => {
    event.dataTransfer.setData(NODE_DRAG_MIME, schema.type)
    event.dataTransfer.effectAllowed = 'move'
  }

  return (
    <div
      draggable
      onDragStart={onDragStart}
      className="flex cursor-grab items-start gap-2 rounded-md border border-slate-200 bg-white p-2 shadow-sm transition hover:border-slate-300 hover:shadow active:cursor-grabbing"
    >
      <NodeIcon
        name={schema.icon}
        className="mt-0.5 h-4 w-4 shrink-0 text-slate-600"
      />
      <div className="min-w-0">
        <div className="text-sm font-medium text-slate-800">{schema.label}</div>
        <div className="truncate text-[11px] text-slate-400">
          {schema.description}
        </div>
      </div>
    </div>
  )
}
