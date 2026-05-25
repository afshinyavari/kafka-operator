import type { ProjectDocument } from '../model/project'
import { migrateProject } from '../model/migrations'

/** Download a project document as a pretty-printed `.kafka.json` file. */
export function downloadProject(doc: ProjectDocument): void {
  const safeName =
    (doc.meta.name || 'project').trim().replace(/\s+/g, '-').toLowerCase() ||
    'project'
  const blob = new Blob([JSON.stringify(doc, null, 2)], {
    type: 'application/json',
  })
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = `${safeName}.kafka.json`
  document.body.appendChild(link)
  link.click()
  link.remove()
  URL.revokeObjectURL(url)
}

/**
 * Read and parse a project file, running schema migrations. Throws on invalid
 * JSON or an unsupported schema version — the caller surfaces the error.
 */
export async function readProjectFile(file: File): Promise<ProjectDocument> {
  const text = await file.text()
  const parsed = JSON.parse(text) as Record<string, unknown>
  return migrateProject(parsed)
}
