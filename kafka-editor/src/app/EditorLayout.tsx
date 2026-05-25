import { useEffect, useRef, useState } from 'react'
import type { ChangeEvent } from 'react'
import {
  Braces,
  Code2,
  Download,
  Library,
  Play,
  Settings,
  Upload,
} from 'lucide-react'
import { Palette } from '../palette/Palette'
import { Canvas } from '../canvas/Canvas'
import { Inspector } from '../inspector/Inspector'
import { ProblemsPanel } from '../canvas/ProblemsPanel'
import { RecordTypesModal } from '../recordTypes/RecordTypesModal'
import { CatalogModal } from '../catalog/CatalogModal'
import { CodegenModal } from '../codegen/CodegenModal'
import { RunDialog } from '../run/RunDialog'
import { useRunStore } from '../run/runStore'
import { SettingsModal } from '../settings/SettingsModal'
import { initialProjectDoc, useEditorStore } from '../state/store'
import { startAutosave } from '../persistence/autosave'
import { toDocument } from '../model/serialize'
import type { ProjectDocument } from '../model/project'
import { downloadProject, readProjectFile } from '../persistence/fileIO'
import { useViewStore } from '../state/viewStore'
import { ClusterView } from '../cluster/ClusterView'
import { ClusterTopBar } from '../cluster/ClusterTopBar'

const headerBtn =
  'flex items-center gap-1.5 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100'

/** The app shell: header + (editor workbench | cluster management view). */
export function EditorLayout() {
  const clearGraph = useEditorStore((s) => s.clearGraph)
  const loadProject = useEditorStore((s) => s.loadProject)
  const nodeCount = useEditorStore((s) => s.nodes.length)
  const recordTypeCount = useEditorStore((s) => s.recordTypes.length)
  const topicCount = useEditorStore((s) => s.catalog.topics.length)
  const view = useViewStore((s) => s.view)
  const setView = useViewStore((s) => s.setView)
  // The Cluster view mounts on first use, then stays mounted so a toggle back
  // to the editor never tears down the React Flow canvas state.
  const clusterMounted = useViewStore((s) => s.clusterEverOpened)
  const [showRecordTypes, setShowRecordTypes] = useState(false)
  const [showCatalog, setShowCatalog] = useState(false)
  const [codegenDoc, setCodegenDoc] = useState<ProjectDocument | null>(null)
  const [showRun, setShowRun] = useState(false)
  const [showSettings, setShowSettings] = useState(false)
  const fileInputRef = useRef<HTMLInputElement>(null)

  // Persist to localStorage while the editor is mounted.
  useEffect(() => startAutosave(), [])

  const currentDocument = (): ProjectDocument => {
    const s = useEditorStore.getState()
    return toDocument(initialProjectDoc, s.nodes, s.edges, s.viewport, {
      recordTypes: s.recordTypes,
      catalog: s.catalog,
      environment: s.environment,
    })
  }

  const onImportFile = async (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    e.target.value = ''
    if (!file) return
    try {
      loadProject(await readProjectFile(file))
    } catch (err) {
      alert(
        `Could not import project: ${
          err instanceof Error ? err.message : String(err)
        }`,
      )
    }
  }

  return (
    <div className="flex h-screen w-screen flex-col bg-white text-slate-800">
      <header className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
        <div className="flex items-center gap-3">
          <h1 className="text-sm font-semibold">Kafka Editor</h1>
          <div className="flex overflow-hidden rounded border border-slate-300 text-xs">
            <button
              type="button"
              onClick={() => setView('editor')}
              className={
                view === 'editor'
                  ? 'bg-slate-800 px-2.5 py-1 text-white'
                  : 'px-2.5 py-1 text-slate-600 hover:bg-slate-100'
              }
            >
              Editor
            </button>
            <button
              type="button"
              onClick={() => setView('cluster')}
              className={
                view === 'cluster'
                  ? 'bg-slate-800 px-2.5 py-1 text-white'
                  : 'px-2.5 py-1 text-slate-600 hover:bg-slate-100'
              }
            >
              Cluster
            </button>
          </div>
        </div>

        <div className="flex items-center gap-2">
          {view === 'editor' ? (
            <>
              <span className="mr-1 text-[11px] text-slate-400">
                {nodeCount} nodes
              </span>
              <button
                type="button"
                onClick={() => setShowSettings(true)}
                aria-label="Settings"
                title="Settings"
                className="rounded border border-slate-300 p-1.5 text-slate-600 hover:bg-slate-100"
              >
                <Settings className="h-3.5 w-3.5" />
              </button>
              <button
                type="button"
                onClick={() => setShowCatalog(true)}
                className={headerBtn}
              >
                <Library className="h-3.5 w-3.5" />
                Catalog
                {topicCount > 0 && (
                  <span className="text-slate-400">({topicCount})</span>
                )}
              </button>
              <button
                type="button"
                onClick={() => setShowRecordTypes(true)}
                className={headerBtn}
              >
                <Braces className="h-3.5 w-3.5" />
                Record Types
                {recordTypeCount > 0 && (
                  <span className="text-slate-400">({recordTypeCount})</span>
                )}
              </button>
              <button
                type="button"
                onClick={() => downloadProject(currentDocument())}
                className={headerBtn}
              >
                <Download className="h-3.5 w-3.5" />
                Export
              </button>
              <button
                type="button"
                onClick={() => fileInputRef.current?.click()}
                className={headerBtn}
              >
                <Upload className="h-3.5 w-3.5" />
                Import
              </button>
              <button
                type="button"
                onClick={() => {
                  clearGraph()
                  useRunStore.getState().reset()
                }}
                className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
              >
                Clear
              </button>
              <button
                type="button"
                onClick={() => setShowRun(true)}
                className="flex items-center gap-1.5 rounded border border-emerald-700 bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700"
              >
                <Play className="h-3.5 w-3.5" />
                Run
              </button>
              <button
                type="button"
                onClick={() => setCodegenDoc(currentDocument())}
                className="flex items-center gap-1.5 rounded border border-slate-800 bg-slate-800 px-2 py-1 text-xs font-medium text-white hover:bg-slate-700"
              >
                <Code2 className="h-3.5 w-3.5" />
                Generate Code
              </button>
            </>
          ) : (
            <ClusterTopBar />
          )}
        </div>
      </header>

      <div className="flex flex-1 overflow-hidden">
        <div
          className={
            view === 'editor' ? 'flex flex-1 overflow-hidden' : 'hidden'
          }
        >
          <Palette />
          <main className="flex flex-1 flex-col overflow-hidden">
            <div className="relative flex-1">
              <Canvas />
            </div>
            <ProblemsPanel />
          </main>
          <Inspector />
        </div>

        {clusterMounted && (
          <div
            className={
              view === 'cluster' ? 'flex flex-1 overflow-hidden' : 'hidden'
            }
          >
            <ClusterView />
          </div>
        )}
      </div>

      <input
        ref={fileInputRef}
        type="file"
        accept=".json,application/json"
        className="hidden"
        onChange={onImportFile}
      />

      {showRecordTypes && (
        <RecordTypesModal onClose={() => setShowRecordTypes(false)} />
      )}
      {showCatalog && <CatalogModal onClose={() => setShowCatalog(false)} />}
      {codegenDoc && (
        <CodegenModal doc={codegenDoc} onClose={() => setCodegenDoc(null)} />
      )}
      {showRun && (
        <RunDialog
          buildDocument={currentDocument}
          onClose={() => setShowRun(false)}
        />
      )}
      {showSettings && (
        <SettingsModal onClose={() => setShowSettings(false)} />
      )}
    </div>
  )
}
