import { create } from 'zustand'
import { nanoid } from 'nanoid'

/**
 * Environment variables — a local-only key/value store. Deliberately kept out
 * of the project document (and its export) because these typically hold
 * secrets. Persisted to localStorage; sent with each run as extra config.
 */

export interface EnvVar {
  id: string
  key: string
  value: string
}

const STORAGE_KEY = 'kafka-editor-env-vars'

function load(): EnvVar[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    return raw ? (JSON.parse(raw) as EnvVar[]) : []
  } catch {
    return []
  }
}

function persist(vars: EnvVar[]): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(vars))
  } catch {
    // Storage may be unavailable — non-fatal.
  }
}

interface EnvVarState {
  envVars: EnvVar[]
  addEnvVar: () => void
  updateEnvVar: (id: string, patch: Partial<EnvVar>) => void
  removeEnvVar: (id: string) => void
}

export const useEnvVarStore = create<EnvVarState>((set) => ({
  envVars: load(),

  addEnvVar: () =>
    set((s) => {
      const next = [...s.envVars, { id: nanoid(), key: '', value: '' }]
      persist(next)
      return { envVars: next }
    }),

  updateEnvVar: (id, patch) =>
    set((s) => {
      const next = s.envVars.map((v) => (v.id === id ? { ...v, ...patch } : v))
      persist(next)
      return { envVars: next }
    }),

  removeEnvVar: (id) =>
    set((s) => {
      const next = s.envVars.filter((v) => v.id !== id)
      persist(next)
      return { envVars: next }
    }),
}))

/** The current env vars as a plain key/value map (for sending with a run). */
export function envVarsAsMap(): Record<string, string> {
  const map: Record<string, string> = {}
  for (const v of useEnvVarStore.getState().envVars) {
    if (v.key.trim().length > 0) {
      map[v.key.trim()] = v.value
    }
  }
  return map
}
