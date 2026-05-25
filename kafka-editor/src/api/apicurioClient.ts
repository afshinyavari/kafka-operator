/**
 * Client for the schema registry, via the Quarkus backend's proxy
 * (`/api/registry`) — so any configured registry URL works without CORS.
 */

import { apiBase } from './baseUrl'

const BASE = `${apiBase()}/api/registry`

/** A schema artifact as returned by the registry's search endpoint. */
export interface RegistryArtifact {
  groupId: string
  artifactId: string
  name?: string
  /** Artifact type — AVRO, JSON, PROTOBUF, … */
  type: string
}

interface SearchResponse {
  artifacts?: Array<{
    groupId?: string
    id?: string
    name?: string
    type?: string
  }>
}

/** Search artifacts by name (empty `name` lists everything, up to a limit). */
export async function searchArtifacts(
  registryUrl: string,
  name = '',
): Promise<RegistryArtifact[]> {
  if (!registryUrl.trim()) {
    throw new Error('No schema registry URL configured — set one in Settings.')
  }
  const params = new URLSearchParams({ registry: registryUrl.trim() })
  if (name.trim()) params.set('name', name.trim())

  let res: Response
  try {
    res = await fetch(`${BASE}/artifacts?${params.toString()}`)
  } catch {
    throw new Error('Could not reach the backend (mvn quarkus:dev in /backend).')
  }
  if (!res.ok) {
    throw new Error(`Registry search failed (HTTP ${res.status})`)
  }
  const data = (await res.json()) as SearchResponse
  return (data.artifacts ?? []).map((a) => ({
    groupId: a.groupId ?? 'default',
    artifactId: a.id ?? '',
    name: a.name,
    type: a.type ?? 'UNKNOWN',
  }))
}

/** Fetch the latest content (raw schema text) of an artifact. */
export async function getArtifactContent(
  registryUrl: string,
  groupId: string,
  artifactId: string,
): Promise<string> {
  const params = new URLSearchParams({
    registry: registryUrl.trim(),
    groupId,
    artifactId,
  })
  let res: Response
  try {
    res = await fetch(`${BASE}/content?${params.toString()}`)
  } catch {
    throw new Error('Could not reach the backend.')
  }
  if (!res.ok) {
    throw new Error(`Failed to fetch artifact content (HTTP ${res.status})`)
  }
  return res.text()
}

/** Artifact metadata — notably the global id used by the message envelope. */
export interface ArtifactMeta {
  globalId: number
  type: string
  version: string
}

async function registryError(res: Response, fallback: string): Promise<Error> {
  try {
    const body = (await res.json()) as { message?: string; error?: string }
    return new Error(body.message ?? body.error ?? fallback)
  } catch {
    return new Error(`${fallback} (HTTP ${res.status})`)
  }
}

/** Fetch an artifact's metadata. */
export async function getArtifactMeta(
  registryUrl: string,
  groupId: string,
  artifactId: string,
): Promise<ArtifactMeta> {
  const params = new URLSearchParams({
    registry: registryUrl.trim(),
    groupId,
    artifactId,
  })
  const res = await fetch(`${BASE}/meta?${params.toString()}`)
  if (!res.ok) throw await registryError(res, 'Failed to fetch artifact meta')
  const data = (await res.json()) as Partial<ArtifactMeta>
  return {
    globalId: data.globalId ?? 0,
    type: data.type ?? 'UNKNOWN',
    version: data.version ?? '',
  }
}

/** List an artifact's version identifiers. */
export async function getArtifactVersions(
  registryUrl: string,
  groupId: string,
  artifactId: string,
): Promise<string[]> {
  const params = new URLSearchParams({
    registry: registryUrl.trim(),
    groupId,
    artifactId,
  })
  const res = await fetch(`${BASE}/versions?${params.toString()}`)
  if (!res.ok) throw await registryError(res, 'Failed to fetch versions')
  const data = (await res.json()) as {
    versions?: Array<{ version?: string }>
  }
  return (data.versions ?? []).map((v) => v.version ?? '?')
}

/** Create a new registry artifact. */
export async function createArtifact(
  registryUrl: string,
  groupId: string,
  artifactId: string,
  type: string,
  content: string,
): Promise<void> {
  const res = await fetch(`${BASE}/artifacts`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      registry: registryUrl.trim(),
      groupId,
      artifactId,
      type,
      content,
    }),
  })
  if (!res.ok) throw await registryError(res, 'Failed to create artifact')
}

/** Publish a new version of an existing artifact. */
export async function updateArtifact(
  registryUrl: string,
  groupId: string,
  artifactId: string,
  content: string,
): Promise<void> {
  const res = await fetch(`${BASE}/artifacts`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      registry: registryUrl.trim(),
      groupId,
      artifactId,
      content,
    }),
  })
  if (!res.ok) throw await registryError(res, 'Failed to update artifact')
}

/** Delete a registry artifact. */
export async function deleteArtifact(
  registryUrl: string,
  groupId: string,
  artifactId: string,
): Promise<void> {
  const params = new URLSearchParams({
    registry: registryUrl.trim(),
    groupId,
    artifactId,
  })
  const res = await fetch(`${BASE}/artifacts?${params.toString()}`, {
    method: 'DELETE',
  })
  if (!res.ok) throw await registryError(res, 'Failed to delete artifact')
}
