import { useState } from 'react'
import { Modal } from '../../components/Modal'
import { ErrorBanner } from '../../components/ErrorBanner'
import { createAcl } from '../../api/adminClient'
import type { AclEntry } from '../../api/adminClient'
import type { ClusterConnection } from '../clusterStore'

interface CreateAclModalProps {
  conn: ClusterConnection
  onClose: () => void
  onCreated: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

const RESOURCE_TYPES = ['TOPIC', 'GROUP', 'CLUSTER', 'TRANSACTIONAL_ID']
const PATTERN_TYPES = ['LITERAL', 'PREFIXED']
const OPERATIONS = [
  'ALL',
  'READ',
  'WRITE',
  'CREATE',
  'DELETE',
  'ALTER',
  'DESCRIBE',
  'DESCRIBE_CONFIGS',
  'ALTER_CONFIGS',
]
const PERMISSIONS = ['ALLOW', 'DENY']

/** Form for creating a native Kafka ACL. */
export function CreateAclModal({
  conn,
  onClose,
  onCreated,
}: CreateAclModalProps) {
  const [acl, setAcl] = useState<AclEntry>({
    resourceType: 'TOPIC',
    resourceName: '',
    patternType: 'LITERAL',
    principal: 'User:',
    host: '*',
    operation: 'READ',
    permissionType: 'ALLOW',
  })
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const set = (patch: Partial<AclEntry>) => setAcl((a) => ({ ...a, ...patch }))

  const submit = async () => {
    setSubmitting(true)
    setError(null)
    try {
      await createAcl(conn, acl)
      onCreated()
      onClose()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
      setSubmitting(false)
    }
  }

  return (
    <Modal
      title="New ACL"
      onClose={onClose}
      footer={
        <>
          <button
            type="button"
            onClick={onClose}
            className="rounded border border-slate-300 px-3 py-1 text-xs text-slate-600 hover:bg-slate-100"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={submit}
            disabled={
              submitting ||
              acl.resourceName.trim() === '' ||
              acl.principal.trim() === ''
            }
            className="rounded border border-emerald-700 bg-emerald-600 px-3 py-1 text-xs font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
          >
            {submitting ? 'Creating…' : 'Create ACL'}
          </button>
        </>
      }
    >
      {error && <ErrorBanner message={error} />}
      <div className="flex gap-2">
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Resource type
          <select
            className={inputCls}
            value={acl.resourceType}
            onChange={(e) => set({ resourceType: e.target.value })}
          >
            {RESOURCE_TYPES.map((t) => (
              <option key={t}>{t}</option>
            ))}
          </select>
        </label>
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Pattern
          <select
            className={inputCls}
            value={acl.patternType}
            onChange={(e) => set({ patternType: e.target.value })}
          >
            {PATTERN_TYPES.map((t) => (
              <option key={t}>{t}</option>
            ))}
          </select>
        </label>
      </div>
      <label className="flex flex-col gap-1 text-xs text-slate-600">
        Resource name
        <input
          className={`${inputCls} font-mono`}
          value={acl.resourceName}
          placeholder="orders  (or * for all)"
          onChange={(e) => set({ resourceName: e.target.value })}
        />
      </label>
      <div className="flex gap-2">
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Principal
          <input
            className={`${inputCls} font-mono`}
            value={acl.principal}
            placeholder="User:alice"
            onChange={(e) => set({ principal: e.target.value })}
          />
        </label>
        <label className="flex w-24 flex-col gap-1 text-xs text-slate-600">
          Host
          <input
            className={`${inputCls} font-mono`}
            value={acl.host}
            onChange={(e) => set({ host: e.target.value })}
          />
        </label>
      </div>
      <div className="flex gap-2">
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Operation
          <select
            className={inputCls}
            value={acl.operation}
            onChange={(e) => set({ operation: e.target.value })}
          >
            {OPERATIONS.map((o) => (
              <option key={o}>{o}</option>
            ))}
          </select>
        </label>
        <label className="flex flex-1 flex-col gap-1 text-xs text-slate-600">
          Permission
          <select
            className={inputCls}
            value={acl.permissionType}
            onChange={(e) => set({ permissionType: e.target.value })}
          >
            {PERMISSIONS.map((p) => (
              <option key={p}>{p}</option>
            ))}
          </select>
        </label>
      </div>
    </Modal>
  )
}
