import { create } from 'zustand'
import {
  addEdge,
  applyEdgeChanges,
  applyNodeChanges,
  type Connection,
  type EdgeChange,
  type NodeChange,
  type Viewport,
} from '@xyflow/react'
import { nanoid } from 'nanoid'
import { getNodeSchema } from '../nodes'
import type { BranchItem, NodeConfig } from '../nodes'
import type { KafkaEdge, KafkaNode } from '../model/graph'
import { createEmptyProject } from '../model/project'
import type { ProjectDocument } from '../model/project'
import type { RecordType } from '../model/recordTypes'
import type { Catalog, TopicDef } from '../model/catalog'
import type { Environment } from '../model/environment'
import { fromDocument } from '../model/serialize'
import { loadProjectFromStorage } from '../persistence/localStorage'
import { emptyPredicate, newFieldExpression } from '../expressions/types'
import { defaultWindow } from '../model/windows'

/** The document the store was hydrated from. Autosave uses it as its base. */
export const initialProjectDoc: ProjectDocument =
  loadProjectFromStorage() ?? createEmptyProject()

const initialGraph = fromDocument(initialProjectDoc)

/** Seed a new node's config with its label, property defaults, and branches. */
function buildInitialConfig(schemaType: string): NodeConfig {
  const schema = getNodeSchema(schemaType)
  const config: NodeConfig = { label: schema?.label ?? schemaType }
  if (schema) {
    for (const prop of schema.properties) {
      if (prop.default !== undefined) {
        config[prop.key] = prop.default
      } else if (prop.kind === 'predicate') {
        config[prop.key] = emptyPredicate()
      } else if (prop.kind === 'keyExpression') {
        config[prop.key] = newFieldExpression()
      } else if (prop.kind === 'valueMapping') {
        config[prop.key] = []
      } else if (prop.kind === 'window') {
        config[prop.key] = defaultWindow()
      } else if (prop.kind === 'aggregation') {
        config[prop.key] = []
      } else if (prop.kind === 'keyValue') {
        config[prop.key] = []
      }
    }
    if (schema.dynamicOutputs) {
      config[schema.dynamicOutputs.configKey] = [
        { id: nanoid(), name: 'branch-1', predicate: emptyPredicate() },
        { id: nanoid(), name: 'branch-2', predicate: emptyPredicate() },
      ]
    }
  }
  return config
}

export interface EditorState {
  nodes: KafkaNode[]
  edges: KafkaEdge[]
  viewport: Viewport
  selectedNodeId: string | null
  /** Named record-type definitions, project-global. */
  recordTypes: RecordType[]
  /** Topics and SerDes, project-global. */
  catalog: Catalog
  /** Kafka cluster + schema registry the topology runs against. */
  environment: Environment

  /** React Flow change handlers. */
  onNodesChange: (changes: NodeChange<KafkaNode>[]) => void
  onEdgesChange: (changes: EdgeChange<KafkaEdge>[]) => void
  onConnect: (connection: Connection) => void
  setViewport: (viewport: Viewport) => void
  setSelectedNode: (nodeId: string | null) => void

  /** Node actions. */
  addNode: (schemaType: string, position: { x: number; y: number }) => void
  updateNodeConfig: (nodeId: string, patch: NodeConfig) => void
  updateBranches: (nodeId: string, branches: BranchItem[]) => void
  deleteNode: (nodeId: string) => void
  clearGraph: () => void
  selectNode: (nodeId: string) => void
  loadProject: (doc: ProjectDocument) => void
  setEnvironment: (environment: Environment) => void

  /** Record-type actions. */
  addRecordType: (recordType: RecordType) => void
  updateRecordType: (id: string, patch: Partial<RecordType>) => void
  removeRecordType: (id: string) => void

  /** Catalog actions. */
  addTopic: (topic: TopicDef) => void
  updateTopic: (id: string, patch: Partial<TopicDef>) => void
  removeTopic: (id: string) => void
}

export const useEditorStore = create<EditorState>((set) => ({
  nodes: initialGraph.nodes,
  edges: initialGraph.edges,
  viewport: initialProjectDoc.viewport,
  selectedNodeId: null,
  recordTypes: initialGraph.recordTypes,
  catalog: initialGraph.catalog,
  environment: initialGraph.environment,

  onNodesChange: (changes) =>
    set((state) => ({ nodes: applyNodeChanges(changes, state.nodes) })),

  onEdgesChange: (changes) =>
    set((state) => ({ edges: applyEdgeChanges(changes, state.edges) })),

  onConnect: (connection) =>
    set((state) => ({ edges: addEdge(connection, state.edges) })),

  setViewport: (viewport) => set({ viewport }),

  setSelectedNode: (nodeId) => set({ selectedNodeId: nodeId }),

  setEnvironment: (environment) => set({ environment }),

  addNode: (schemaType, position) => {
    const schema = getNodeSchema(schemaType)
    if (!schema) {
      console.warn(`Cannot add node: unknown schema type "${schemaType}"`)
      return
    }
    const node: KafkaNode = {
      id: nanoid(),
      type: schemaType,
      position,
      selected: true,
      data: { config: buildInitialConfig(schemaType) },
    }
    set((state) => ({
      nodes: [
        ...state.nodes.map((n) => (n.selected ? { ...n, selected: false } : n)),
        node,
      ],
      selectedNodeId: node.id,
    }))
  },

  updateNodeConfig: (nodeId, patch) =>
    set((state) => ({
      nodes: state.nodes.map((node) =>
        node.id === nodeId
          ? {
              ...node,
              data: { ...node.data, config: { ...node.data.config, ...patch } },
            }
          : node,
      ),
    })),

  updateBranches: (nodeId, branches) =>
    set((state) => {
      const validHandles = new Set(branches.map((b) => b.id))
      return {
        nodes: state.nodes.map((node) =>
          node.id === nodeId
            ? {
                ...node,
                data: {
                  ...node.data,
                  config: { ...node.data.config, branches },
                },
              }
            : node,
        ),
        edges: state.edges.filter(
          (edge) =>
            edge.source !== nodeId ||
            validHandles.has(edge.sourceHandle ?? ''),
        ),
      }
    }),

  deleteNode: (nodeId) =>
    set((state) => ({
      nodes: state.nodes.filter((node) => node.id !== nodeId),
      edges: state.edges.filter(
        (edge) => edge.source !== nodeId && edge.target !== nodeId,
      ),
      selectedNodeId:
        state.selectedNodeId === nodeId ? null : state.selectedNodeId,
    })),

  clearGraph: () => set({ nodes: [], edges: [], selectedNodeId: null }),

  selectNode: (nodeId) =>
    set((state) => ({
      nodes: state.nodes.map((n) => ({ ...n, selected: n.id === nodeId })),
      selectedNodeId: nodeId,
    })),

  loadProject: (doc) => {
    const graph = fromDocument(doc)
    set({
      nodes: graph.nodes,
      edges: graph.edges,
      recordTypes: graph.recordTypes,
      catalog: graph.catalog,
      environment: graph.environment,
      viewport: doc.viewport,
      selectedNodeId: null,
    })
  },

  addRecordType: (recordType) =>
    set((state) => ({ recordTypes: [...state.recordTypes, recordType] })),

  updateRecordType: (id, patch) =>
    set((state) => ({
      recordTypes: state.recordTypes.map((rt) =>
        rt.id === id ? { ...rt, ...patch } : rt,
      ),
    })),

  removeRecordType: (id) =>
    set((state) => ({
      recordTypes: state.recordTypes.filter((rt) => rt.id !== id),
    })),

  addTopic: (topic) =>
    set((state) => ({
      catalog: { ...state.catalog, topics: [...state.catalog.topics, topic] },
    })),

  updateTopic: (id, patch) =>
    set((state) => ({
      catalog: {
        ...state.catalog,
        topics: state.catalog.topics.map((t) =>
          t.id === id ? { ...t, ...patch } : t,
        ),
      },
    })),

  removeTopic: (id) =>
    set((state) => ({
      catalog: {
        ...state.catalog,
        topics: state.catalog.topics.filter((t) => t.id !== id),
      },
    })),
}))
