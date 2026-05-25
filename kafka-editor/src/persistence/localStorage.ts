import { migrateProject } from '../model/migrations'
import type { ProjectDocument } from '../model/project'

const STORAGE_KEY = 'kafka-editor:project'

/**
 * Load the persisted project, running schema migrations. Returns `null` (rather
 * than throwing) for missing, malformed, or unreadable storage so callers can
 * fall back to an empty project.
 */
export function loadProjectFromStorage(): ProjectDocument | null {
  try {
    if (typeof localStorage === 'undefined') return null
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw) as Record<string, unknown>
    return migrateProject(parsed)
  } catch (error) {
    console.error('Failed to load project from storage:', error)
    return null
  }
}

/** Persist the project document. Storage failures are logged, not thrown. */
export function saveProjectToStorage(doc: ProjectDocument): void {
  try {
    if (typeof localStorage === 'undefined') return
    localStorage.setItem(STORAGE_KEY, JSON.stringify(doc))
  } catch (error) {
    console.error('Failed to save project to storage:', error)
  }
}
