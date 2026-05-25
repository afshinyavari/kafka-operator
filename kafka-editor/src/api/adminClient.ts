import type { ClusterConnection } from '../cluster/clusterStore'
import { apiBase } from './baseUrl'

/**
 * Client for the backend's Kafka admin API (`/api/admin/*`), reached through
 * the Vite dev proxy (web) or the bundled backend (desktop). Connection
 * details ride in the query string on GETs (SSE / EventSource cannot send a
 * body); write calls carry them in the body.
 *
 * NOTE: connection strings are passed in the URL. That is fine for the
 * local-first model (broker hosts are not secrets). If OIDC/credentials are
 * added later, switch SSE to a POST-handshake → session-id GET pattern.
 */

const BASE = `${apiBase()}/api/admin`

export interface ApiError {
  error: string
  detail: string | null
  kind: string
}

export interface ApiResponse<T> {
  data: T | null
  capability: string
  error: ApiError | null
}

// ── DTOs (mirror org.acme.kafkaeditor.admin.dto) ──────────────────────────

export interface BrokerInfo {
  id: number
  host: string
  port: number
  rack: string | null
}

export interface ClusterOverview {
  clusterId: string
  controller: BrokerInfo | null
  brokers: BrokerInfo[]
  topicCount: number
  partitionCount: number
}

export interface TopicSummary {
  name: string
  partitionCount: number
  replicationFactor: number
  internal: boolean
}

export interface PartitionDetail {
  partition: number
  leader: number
  replicas: number[]
  isr: number[]
}

export interface ConfigKv {
  name: string
  value: string | null
  isDefault: boolean
  sensitive: boolean
  readOnly: boolean
}

export interface TopicDetail {
  name: string
  internal: boolean
  partitions: PartitionDetail[]
  configs: ConfigKv[]
}

export interface PartitionMetric {
  partition: number
  startOffset: number
  endOffset: number
  count: number
}

export interface TopicMetrics {
  partitionCount: number
  messageCount: number
  /** -1 when the broker did not report log-dir sizes. */
  sizeBytes: number
  underReplicatedPartitions: number
  offlinePartitions: number
  partitions: PartitionMetric[]
}

// ── transport helpers ─────────────────────────────────────────────────────

/** The connection query string carried on GET / SSE requests. */
export function connQuery(conn: ClusterConnection): string {
  const params = new URLSearchParams()
  if (conn.bootstrapServers) params.set('bootstrap', conn.bootstrapServers)
  if (conn.schemaRegistryUrl) params.set('registry', conn.schemaRegistryUrl)
  if (conn.connectUrl) params.set('connect', conn.connectUrl)
  return params.toString()
}

const UNREACHABLE =
  'Could not reach the backend. Start it with `mvn quarkus:dev` in /backend.'

async function parseBody(res: Response): Promise<unknown> {
  try {
    return await res.json()
  } catch {
    return null
  }
}

/** Unwrap an `ApiResponse<T>` envelope, throwing on HTTP or capability errors. */
async function unwrap<T>(res: Response): Promise<T> {
  const body = await parseBody(res)
  if (!res.ok) {
    const err = body as ApiError | null
    throw new Error(err?.error ?? `Request failed (HTTP ${res.status})`)
  }
  const envelope = body as ApiResponse<T>
  if (envelope?.error) throw new Error(envelope.error.error)
  return envelope?.data as T
}

type QueryValue = string | number | boolean | undefined | null

function buildSearch(
  conn: ClusterConnection,
  params?: Record<string, QueryValue>,
): string {
  const search = new URLSearchParams(connQuery(conn))
  if (params) {
    for (const [key, value] of Object.entries(params)) {
      if (value !== undefined && value !== null && value !== '') {
        search.set(key, String(value))
      }
    }
  }
  return search.toString()
}

async function getJson<T>(
  path: string,
  conn: ClusterConnection,
  params?: Record<string, QueryValue>,
): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${BASE}${path}?${buildSearch(conn, params)}`)
  } catch {
    throw new Error(UNREACHABLE)
  }
  return unwrap<T>(res)
}

/** A GET that returns the full envelope — for capability-bearing endpoints. */
async function getEnvelope<T>(
  path: string,
  conn: ClusterConnection,
  params?: Record<string, QueryValue>,
): Promise<ApiResponse<T>> {
  let res: Response
  try {
    res = await fetch(`${BASE}${path}?${buildSearch(conn, params)}`)
  } catch {
    throw new Error(UNREACHABLE)
  }
  const body = await parseBody(res)
  if (!res.ok) {
    throw new Error((body as ApiError | null)?.error
      ?? `Request failed (HTTP ${res.status})`)
  }
  return body as ApiResponse<T>
}

/** A POST/PUT/PATCH carrying a JSON body (connection embedded by the caller). */
async function sendJson<T>(
  method: string,
  path: string,
  body: unknown,
): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${BASE}${path}`, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  } catch {
    throw new Error(UNREACHABLE)
  }
  return unwrap<T>(res)
}

/** A DELETE with the connection in the query string. */
async function deleteWithConn(
  path: string,
  conn: ClusterConnection,
): Promise<void> {
  let res: Response
  try {
    res = await fetch(`${BASE}${path}?${connQuery(conn)}`, { method: 'DELETE' })
  } catch {
    throw new Error(UNREACHABLE)
  }
  await unwrap<void>(res)
}

// ── M-A endpoints ─────────────────────────────────────────────────────────

export function getClusterOverview(
  conn: ClusterConnection,
): Promise<ClusterOverview> {
  return getJson<ClusterOverview>('/cluster', conn)
}

export function listTopics(conn: ClusterConnection): Promise<TopicSummary[]> {
  return getJson<TopicSummary[]>('/topics', conn)
}

export function getTopic(
  conn: ClusterConnection,
  name: string,
): Promise<TopicDetail> {
  return getJson<TopicDetail>(`/topics/${encodeURIComponent(name)}`, conn)
}

// ── M-B endpoints ─────────────────────────────────────────────────────────

export interface CreateTopicInput {
  name: string
  partitions: number
  replicationFactor: number
  configs: Record<string, string>
}

export function createTopic(
  conn: ClusterConnection,
  input: CreateTopicInput,
): Promise<void> {
  return sendJson('POST', '/topics', { connection: conn, ...input })
}

export function updateTopicConfig(
  conn: ClusterConnection,
  name: string,
  changes: Record<string, string>,
): Promise<void> {
  return sendJson('POST', `/topics/${encodeURIComponent(name)}/config`, {
    connection: conn,
    changes,
  })
}

export function addPartitions(
  conn: ClusterConnection,
  name: string,
  totalCount: number,
): Promise<void> {
  return sendJson('POST', `/topics/${encodeURIComponent(name)}/partitions`, {
    connection: conn,
    totalCount,
  })
}

export function deleteTopic(
  conn: ClusterConnection,
  name: string,
): Promise<void> {
  return deleteWithConn(`/topics/${encodeURIComponent(name)}`, conn)
}

export function getTopicMetrics(
  conn: ClusterConnection,
  name: string,
): Promise<TopicMetrics> {
  return getJson<TopicMetrics>(
    `/topics/${encodeURIComponent(name)}/metrics`,
    conn,
  )
}

// ── M-C endpoints (message workbench) ─────────────────────────────────────

export type SeekMode = 'OFFSET' | 'TIMESTAMP' | 'LATEST'

export interface Rendered {
  /** TOMBSTONE | EMPTY | AVRO | JSON | STRING | BINARY */
  strategy: string
  schemaRef: string | null
  text: string | null
  size: number
  truncated: boolean
  warnings: string[]
}

export interface HeaderKv {
  key: string
  value: string | null
}

export interface RenderedRecord {
  partition: number
  offset: number
  timestamp: number
  key: Rendered
  value: Rendered
  headers: HeaderKv[]
}

export interface MessagePage {
  topic: string
  partition: number
  startOffset: number
  endOffset: number
  nextOffset: number
  truncated: boolean
  rows: RenderedRecord[]
}

export interface SearchResult {
  scanned: number
  truncated: boolean
  matches: RenderedRecord[]
}

export interface SendResult {
  topic: string
  partition: number
  offset: number
  timestamp: number
}

export interface BrowseParams {
  topic: string
  partition: number
  seek: SeekMode
  offset?: number
  timestamp?: number
  size: number
}

export function getMessagePartitions(
  conn: ClusterConnection,
  topic: string,
): Promise<number[]> {
  return getJson<number[]>('/messages/partitions', conn, { topic })
}

export function browseMessages(
  conn: ClusterConnection,
  params: BrowseParams,
): Promise<MessagePage> {
  return getJson<MessagePage>('/messages', conn, { ...params })
}

export interface SearchParams {
  topic: string
  partition: number
  query: string
  field: string
  caseSensitive: boolean
  scanLimit: number
}

export function searchMessages(
  conn: ClusterConnection,
  params: SearchParams,
): Promise<SearchResult> {
  return getJson<SearchResult>('/messages/search', conn, { ...params })
}

export interface ProduceInput {
  topic: string
  partition?: number
  key?: string
  value?: string
  valueType: string
  tombstone: boolean
  headers: Record<string, string>
}

export function produceMessage(
  conn: ClusterConnection,
  input: ProduceInput,
): Promise<SendResult> {
  return sendJson('POST', '/messages/produce', { connection: conn, ...input })
}

export interface ReplayInput {
  sourceTopic: string
  partition: number
  offset: number
  targetTopic: string
}

export function replayMessage(
  conn: ClusterConnection,
  input: ReplayInput,
): Promise<SendResult> {
  return sendJson('POST', '/messages/replay', { connection: conn, ...input })
}

export interface ProduceAvroInput {
  topic: string
  partition?: number
  key?: string
  jsonValue: string
  schema: string
  globalId?: number
}

/** Produce a record whose value is JSON encoded against an Avro schema. */
export function produceAvro(
  conn: ClusterConnection,
  input: ProduceAvroInput,
): Promise<SendResult> {
  return sendJson('POST', '/messages/produce-avro', {
    connection: conn,
    ...input,
  })
}

/**
 * Subscribe to a topic's live tail (SSE). The connection rides in the query
 * string because EventSource cannot send a body. Returns an unsubscribe fn.
 */
export function tailMessages(
  conn: ClusterConnection,
  topic: string,
  partitions: number[],
  onRecord: (record: RenderedRecord) => void,
): () => void {
  const search = new URLSearchParams(connQuery(conn))
  search.set('topic', topic)
  if (partitions.length > 0) search.set('partitions', partitions.join(','))
  const source = new EventSource(`${BASE}/messages/tail?${search.toString()}`)
  source.onmessage = (event) => {
    try {
      onRecord(JSON.parse(event.data) as RenderedRecord)
    } catch {
      // ignore a malformed event
    }
  }
  return () => source.close()
}

// ── M-D endpoints (consumer groups) ───────────────────────────────────────

export interface GroupSummary {
  groupId: string
  state: string
  members: number
  assignor: string
  coordinator: string
}

export interface PartitionLag {
  topic: string
  partition: number
  committedOffset: number | null
  endOffset: number
  lag: number | null
}

export interface GroupOffsets {
  groupId: string
  state: string
  totalLag: number
  partitions: PartitionLag[]
}

export function listConsumerGroups(
  conn: ClusterConnection,
): Promise<GroupSummary[]> {
  return getJson<GroupSummary[]>('/groups', conn)
}

export function getGroupOffsets(
  conn: ClusterConnection,
  groupId: string,
): Promise<GroupOffsets> {
  return getJson<GroupOffsets>(
    `/groups/${encodeURIComponent(groupId)}/offsets`,
    conn,
  )
}

export interface ResetOffsetsInput {
  topic: string
  partition?: number
  target: 'EARLIEST' | 'LATEST' | 'OFFSET' | 'TIMESTAMP'
  offset?: number
  timestamp?: number
}

export function resetGroupOffsets(
  conn: ClusterConnection,
  groupId: string,
  input: ResetOffsetsInput,
): Promise<void> {
  return sendJson(
    'POST',
    `/groups/${encodeURIComponent(groupId)}/reset-offsets`,
    { connection: conn, ...input },
  )
}

export function deleteConsumerGroup(
  conn: ClusterConnection,
  groupId: string,
): Promise<void> {
  return deleteWithConn(`/groups/${encodeURIComponent(groupId)}`, conn)
}

// ── M-E endpoints (ACLs & Kafka Connect) ──────────────────────────────────

export interface AclEntry {
  resourceType: string
  resourceName: string
  patternType: string
  principal: string
  host: string
  operation: string
  permissionType: string
}

/** ACLs — the envelope's capability is 'unsupported' on a no-authorizer broker. */
export function listAcls(
  conn: ClusterConnection,
): Promise<ApiResponse<AclEntry[]>> {
  return getEnvelope<AclEntry[]>('/acls', conn)
}

export function createAcl(
  conn: ClusterConnection,
  acl: AclEntry,
): Promise<void> {
  return sendJson('POST', '/acls', { connection: conn, acl })
}

export function deleteAcl(
  conn: ClusterConnection,
  acl: AclEntry,
): Promise<number> {
  return sendJson('DELETE', '/acls', { connection: conn, acl })
}

/** One connector as returned by Connect's expanded /connectors listing. */
export interface ConnectorEntry {
  info?: {
    name?: string
    type?: string
    config?: Record<string, string>
  }
  status?: {
    name?: string
    connector?: { state?: string; worker_id?: string }
    tasks?: Array<{ id?: number; state?: string; trace?: string }>
    type?: string
  }
}

export type ConnectorMap = Record<string, ConnectorEntry>

/** Connectors — the envelope's capability is 'not_configured' with no URL. */
export function listConnectors(
  conn: ClusterConnection,
): Promise<ApiResponse<ConnectorMap>> {
  return getEnvelope<ConnectorMap>('/connect/connectors', conn)
}

function connectAction(
  conn: ClusterConnection,
  name: string,
  action: 'restart' | 'pause' | 'resume',
): Promise<unknown> {
  const method = action === 'restart' ? 'POST' : 'PUT'
  return sendJson(
    method,
    `/connect/connectors/${encodeURIComponent(name)}/${action}`,
    { connection: conn },
  )
}

export function restartConnector(conn: ClusterConnection, name: string) {
  return connectAction(conn, name, 'restart')
}

export function pauseConnector(conn: ClusterConnection, name: string) {
  return connectAction(conn, name, 'pause')
}

export function resumeConnector(conn: ClusterConnection, name: string) {
  return connectAction(conn, name, 'resume')
}

export function deleteConnector(
  conn: ClusterConnection,
  name: string,
): Promise<void> {
  return deleteWithConn(
    `/connect/connectors/${encodeURIComponent(name)}`,
    conn,
  )
}
