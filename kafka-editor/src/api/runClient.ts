import type { ProjectDocument } from '../model/project'
import { apiBase } from './baseUrl'

/**
 * Client for the Kafka Editor backend's run API. On the web, calls go through
 * the Vite dev proxy (`/api` → the Quarkus backend on port 8080); in the
 * packaged desktop app they hit the bundled backend directly.
 */

const BASE = `${apiBase()}/api`

/** The outcome of starting a run. */
export interface RunResult {
  runId: string
  mode: string
  /** Per-node counts — populated for test mode; empty for live (use SSE). */
  metrics: Record<string, number>
  error: string | null
}

/** A streamed view of a live run. */
export interface MetricsSnapshot {
  runId: string
  status: string
  metrics: Record<string, number>
}

async function postRun(body: unknown): Promise<RunResult> {
  let res: Response
  try {
    res = await fetch(`${BASE}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  } catch {
    throw new Error(
      'Could not reach the backend. Start it with `mvn quarkus:dev` in /backend.',
    )
  }
  if (!res.ok) {
    throw new Error(`Run failed (HTTP ${res.status})`)
  }
  return (await res.json()) as RunResult
}

/** Run a topology in test mode (TopologyTestDriver, generated records). */
export function runTest(
  document: ProjectDocument,
  recordsPerSource: number,
): Promise<RunResult> {
  return postRun({ document, mode: 'test', recordsPerSource })
}

/** Start a live run against a real broker. */
export function runLive(
  document: ProjectDocument,
  bootstrapServers: string,
  schemaRegistryUrl: string,
  envVars: Record<string, string>,
  generateInput: boolean,
): Promise<RunResult> {
  return postRun({
    document,
    mode: 'live',
    connection: { bootstrapServers, schemaRegistryUrl },
    generateInput,
    envVars,
  })
}

/** Subscribe to a live run's metric stream (SSE). Returns an unsubscribe fn. */
export function subscribeMetrics(
  runId: string,
  onSnapshot: (snapshot: MetricsSnapshot) => void,
): () => void {
  const source = new EventSource(`${BASE}/runs/${runId}/metrics`)
  source.onmessage = (event) => {
    try {
      onSnapshot(JSON.parse(event.data) as MetricsSnapshot)
    } catch {
      // Ignore a malformed event.
    }
  }
  return () => source.close()
}

/** Stop a live run. */
export async function stopRun(runId: string): Promise<void> {
  try {
    await fetch(`${BASE}/runs/${runId}`, { method: 'DELETE' })
  } catch {
    // Best effort — the run is abandoned regardless.
  }
}
