import { create } from 'zustand'
import type { ProjectDocument } from '../model/project'
import { runLive, runTest, stopRun, subscribeMetrics } from '../api/runClient'
import { envVarsAsMap } from './envStore'

export type RunStatus = 'idle' | 'running' | 'done' | 'error'
export type RunMode = 'test' | 'live'

interface RunState {
  status: RunStatus
  mode: RunMode | null
  runId: string | null
  error: string | null
  /** Per-node record counts, keyed by node id. */
  metrics: Record<string, number>

  runTest: (doc: ProjectDocument, recordsPerSource: number) => Promise<void>
  startLive: (doc: ProjectDocument, generateInput: boolean) => Promise<void>
  stopLive: () => Promise<void>
  reset: () => void
}

/** SSE unsubscribe handle for the active live run — module-level so it
 *  survives the Run dialog being closed while a run keeps streaming. */
let metricsUnsubscribe: (() => void) | null = null

function closeStream() {
  if (metricsUnsubscribe) {
    metricsUnsubscribe()
    metricsUnsubscribe = null
  }
}

/**
 * Holds the result of the most recent run, kept separate from the editor store
 * so a run never marks the topology dirty. Owns the run orchestration.
 */
export const useRunStore = create<RunState>((set, get) => ({
  status: 'idle',
  mode: null,
  runId: null,
  error: null,
  metrics: {},

  runTest: async (doc, recordsPerSource) => {
    closeStream()
    set({ status: 'running', mode: 'test', error: null, metrics: {} })
    try {
      const result = await runTest(doc, recordsPerSource)
      set({
        status: result.error ? 'error' : 'done',
        error: result.error,
        metrics: result.error ? {} : result.metrics,
        runId: result.runId,
      })
    } catch (e) {
      set({
        status: 'error',
        error: e instanceof Error ? e.message : String(e),
        metrics: {},
      })
    }
  },

  startLive: async (doc, generateInput) => {
    closeStream()
    set({ status: 'running', mode: 'live', error: null, metrics: {}, runId: null })
    try {
      const result = await runLive(
        doc,
        doc.environment.bootstrapServers,
        doc.environment.schemaRegistryUrl,
        envVarsAsMap(),
        generateInput,
      )
      if (result.error) {
        set({ status: 'error', error: result.error })
        return
      }
      set({ runId: result.runId })
      metricsUnsubscribe = subscribeMetrics(result.runId, (snapshot) => {
        set({ metrics: snapshot.metrics })
        if (snapshot.status === 'error') {
          set({
            status: 'error',
            error: 'The streams application entered an error state.',
          })
        }
      })
    } catch (e) {
      set({ status: 'error', error: e instanceof Error ? e.message : String(e) })
    }
  },

  stopLive: async () => {
    closeStream()
    const { runId } = get()
    if (runId) {
      await stopRun(runId)
    }
    set({ status: 'done' })
  },

  reset: () => {
    closeStream()
    set({ status: 'idle', mode: null, runId: null, error: null, metrics: {} })
  },
}))
