import { useCallback, useMemo, useRef } from 'react'
import type { DragEvent } from 'react'
import {
  Background,
  Controls,
  MiniMap,
  Panel,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type Edge,
} from '@xyflow/react'
import { useEditorStore } from '../state/store'
import { getNodeSchema } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import { CATEGORY_HEX } from './categoryStyles'
import { DATAKIND_HEX, styleEdges } from './edges'
import { validateConnection } from './connectionRules'
import { nodeTypes } from './nodeTypes'
import { NODE_DRAG_MIME } from '../dnd'

/** Explains the solid/dashed edge styling. */
function Legend() {
  return (
    <div className="rounded border border-slate-200 bg-white/90 px-2 py-1.5 text-[10px] text-slate-500 shadow-sm">
      <div className="mb-1 font-semibold tracking-wide uppercase">
        Edge types
      </div>
      <div className="flex items-center gap-1.5">
        <span
          className="inline-block h-0 w-5 border-t-2"
          style={{ borderColor: DATAKIND_HEX.KStream }}
        />
        KStream
      </div>
      <div className="mt-0.5 flex items-center gap-1.5">
        <span
          className="inline-block h-0 w-5 border-t-2 border-dashed"
          style={{ borderColor: DATAKIND_HEX.KTable }}
        />
        KTable
      </div>
      <div className="mt-0.5 flex items-center gap-1.5">
        <span
          className="inline-block h-0 w-5 border-t-2 border-dotted"
          style={{ borderColor: DATAKIND_HEX.KGroupedStream }}
        />
        KGrouped
      </div>
    </div>
  )
}

function CanvasInner() {
  const nodes = useEditorStore((s) => s.nodes)
  const edges = useEditorStore((s) => s.edges)
  const onNodesChange = useEditorStore((s) => s.onNodesChange)
  const onEdgesChange = useEditorStore((s) => s.onEdgesChange)
  const onConnect = useEditorStore((s) => s.onConnect)
  const setViewport = useEditorStore((s) => s.setViewport)
  const setSelectedNode = useEditorStore((s) => s.setSelectedNode)
  const addNode = useEditorStore((s) => s.addNode)

  // Captured once: React Flow reads `defaultViewport` only at mount.
  // eslint-disable-next-line react-hooks/refs -- intentional one-time capture
  const initialViewport = useRef(useEditorStore.getState().viewport).current
  const { screenToFlowPosition } = useReactFlow()

  // Edges are stored undecorated; styling by data kind is a render-time derive.
  const styledEdges = useMemo(() => styleEdges(edges, nodes), [edges, nodes])

  const isValidConnection = useCallback(
    (candidate: Connection | Edge) =>
      validateConnection(
        candidate,
        useEditorStore.getState().nodes,
        useEditorStore.getState().edges,
      ),
    [],
  )

  const onDragOver = useCallback((event: DragEvent) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const onDrop = useCallback(
    (event: DragEvent) => {
      event.preventDefault()
      const schemaType = event.dataTransfer.getData(NODE_DRAG_MIME)
      if (!schemaType) return
      const position = screenToFlowPosition({
        x: event.clientX,
        y: event.clientY,
      })
      addNode(schemaType, position)
    },
    [screenToFlowPosition, addNode],
  )

  return (
    <div className="h-full w-full" onDrop={onDrop} onDragOver={onDragOver}>
      <ReactFlow<KafkaNode, KafkaEdge>
        nodes={nodes}
        edges={styledEdges}
        nodeTypes={nodeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        isValidConnection={isValidConnection}
        onMoveEnd={(_, viewport) => setViewport(viewport)}
        onSelectionChange={({ nodes: selected }) =>
          setSelectedNode(selected[0]?.id ?? null)
        }
        defaultViewport={initialViewport}
        deleteKeyCode={['Backspace', 'Delete']}
      >
        <Background gap={16} />
        <MiniMap
          pannable
          zoomable
          nodeColor={(node) =>
            CATEGORY_HEX[getNodeSchema(node.type ?? '')?.category ?? 'connector']
          }
        />
        <Controls />
        <Panel position="top-right">
          <Legend />
        </Panel>
      </ReactFlow>
    </div>
  )
}

/** The canvas — React Flow host with minimap, controls, and palette drop. */
export function Canvas() {
  return (
    <ReactFlowProvider>
      <CanvasInner />
    </ReactFlowProvider>
  )
}
