import { saveProjectToStorage } from './localStorage'
import { toDocument } from '../model/serialize'
import { initialProjectDoc, useEditorStore } from '../state/store'
import type { ProjectDocument } from '../model/project'

/**
 * The most recently persisted document. Carried forward so the project id and
 * `createdAt` survive across saves.
 */
let baseDoc: ProjectDocument = initialProjectDoc
let timer: ReturnType<typeof setTimeout> | null = null

/**
 * Subscribe to the editor store and persist the project to localStorage,
 * debounced. Returns an unsubscribe function (use it as an effect cleanup).
 */
export function startAutosave(delayMs = 600): () => void {
  const unsubscribe = useEditorStore.subscribe((state) => {
    if (timer) clearTimeout(timer)
    timer = setTimeout(() => {
      baseDoc = toDocument(baseDoc, state.nodes, state.edges, state.viewport, {
        recordTypes: state.recordTypes,
        catalog: state.catalog,
        environment: state.environment,
      })
      saveProjectToStorage(baseDoc)
    }, delayMs)
  })

  return () => {
    if (timer) {
      clearTimeout(timer)
      timer = null
    }
    unsubscribe()
  }
}
