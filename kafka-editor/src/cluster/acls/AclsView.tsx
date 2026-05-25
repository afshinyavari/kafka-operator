import { useState } from 'react'
import { Plus, RefreshCw, Trash2 } from 'lucide-react'
import type { SavedCluster } from '../clusterStore'
import { toConnection } from '../clusterStore'
import { deleteAcl, listAcls } from '../../api/adminClient'
import type { AclEntry } from '../../api/adminClient'
import { useAdminQuery } from '../clusterSelectors'
import { ErrorBanner } from '../../components/ErrorBanner'
import { EmptyState } from '../../components/EmptyState'
import { CreateAclModal } from './CreateAclModal'

interface AclsViewProps {
  cluster: SavedCluster
}

/** Native Kafka ACLs — list, create, delete. */
export function AclsView({ cluster }: AclsViewProps) {
  const conn = toConnection(cluster)
  const [reload, setReload] = useState(0)
  const [showCreate, setShowCreate] = useState(false)
  const [busy, setBusy] = useState(false)
  const { data, loading, error } = useAdminQuery(
    `${cluster.id}:acls:${reload}`,
    () => listAcls(conn),
  )

  const refresh = () => setReload((n) => n + 1)

  const remove = async (acl: AclEntry) => {
    if (!window.confirm('Delete this ACL?')) return
    setBusy(true)
    try {
      await deleteAcl(conn, acl)
      refresh()
    } catch (e) {
      window.alert(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  if (data && data.capability !== 'supported') {
    return (
      <EmptyState
        title="ACLs not available"
        hint={
          data.error?.error ??
          'This broker has no authorizer configured, so ACLs cannot be managed.'
        }
      />
    )
  }

  const acls = data?.data ?? []

  return (
    <div className="flex flex-1 flex-col overflow-hidden">
      <div className="flex items-center justify-between border-b border-slate-200 px-4 py-2">
        <h2 className="text-sm font-semibold text-slate-800">
          ACLs
          {data && (
            <span className="ml-1 text-xs font-normal text-slate-400">
              ({acls.length})
            </span>
          )}
        </h2>
        <div className="flex gap-2">
          <button
            type="button"
            onClick={refresh}
            className="flex items-center gap-1 rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-100"
          >
            <RefreshCw className="h-3.5 w-3.5" />
            Refresh
          </button>
          <button
            type="button"
            onClick={() => setShowCreate(true)}
            className="flex items-center gap-1 rounded border border-emerald-700 bg-emerald-600 px-2 py-1 text-xs font-medium text-white hover:bg-emerald-700"
          >
            <Plus className="h-3.5 w-3.5" />
            New ACL
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto">
        {error && <ErrorBanner message={error} onRetry={refresh} />}
        {loading && !data && (
          <p className="p-4 text-xs text-slate-400">Loading…</p>
        )}
        {data && acls.length === 0 && (
          <p className="p-4 text-xs text-slate-400">No ACLs defined.</p>
        )}
        {acls.length > 0 && (
          <table className="w-full border-collapse text-xs">
            <thead className="sticky top-0 bg-white">
              <tr className="border-b border-slate-200 text-left text-slate-500">
                <th className="py-1.5 pr-3 pl-4 font-medium">Principal</th>
                <th className="py-1.5 pr-3 font-medium">Resource</th>
                <th className="py-1.5 pr-3 font-medium">Operation</th>
                <th className="py-1.5 pr-3 font-medium">Permission</th>
                <th className="py-1.5 pr-3 font-medium">Host</th>
                <th className="py-1.5 pr-4 font-medium" />
              </tr>
            </thead>
            <tbody>
              {acls.map((a, i) => (
                <tr
                  key={`${a.principal}-${a.resourceName}-${a.operation}-${i}`}
                  className="border-b border-slate-100"
                >
                  <td className="py-1.5 pr-3 pl-4 font-mono text-slate-700">
                    {a.principal}
                  </td>
                  <td className="py-1.5 pr-3 font-mono text-slate-600">
                    {a.resourceType.toLowerCase()}:{a.resourceName}
                    <span className="text-slate-400">
                      {' '}
                      ({a.patternType.toLowerCase()})
                    </span>
                  </td>
                  <td className="py-1.5 pr-3">{a.operation}</td>
                  <td className="py-1.5 pr-3">
                    <span
                      className={
                        a.permissionType === 'ALLOW'
                          ? 'text-emerald-600'
                          : 'text-rose-600'
                      }
                    >
                      {a.permissionType}
                    </span>
                  </td>
                  <td className="py-1.5 pr-3 font-mono text-slate-500">
                    {a.host}
                  </td>
                  <td className="py-1.5 pr-4 text-right">
                    <button
                      type="button"
                      onClick={() => remove(a)}
                      disabled={busy}
                      aria-label="Delete ACL"
                      className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100 disabled:opacity-50"
                    >
                      <Trash2 className="h-3 w-3" />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {showCreate && (
        <CreateAclModal
          conn={conn}
          onClose={() => setShowCreate(false)}
          onCreated={refresh}
        />
      )}
    </div>
  )
}
