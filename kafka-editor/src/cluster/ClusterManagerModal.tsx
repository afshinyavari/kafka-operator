import { Check, Plus, Trash2 } from 'lucide-react'
import { Modal } from '../components/Modal'
import { useClusterStore } from './clusterStore'
import { useEditorStore } from '../state/store'

interface ClusterManagerModalProps {
  onClose: () => void
}

const inputCls =
  'w-full rounded border border-slate-300 px-2 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

/** CRUD for the local saved-cluster list. */
export function ClusterManagerModal({ onClose }: ClusterManagerModalProps) {
  const clusters = useClusterStore((s) => s.clusters)
  const activeId = useClusterStore((s) => s.activeId)
  const addCluster = useClusterStore((s) => s.addCluster)
  const updateCluster = useClusterStore((s) => s.updateCluster)
  const removeCluster = useClusterStore((s) => s.removeCluster)
  const setActiveCluster = useClusterStore((s) => s.setActiveCluster)
  const environment = useEditorStore((s) => s.environment)

  const addBlank = () =>
    addCluster({
      name: `Cluster ${clusters.length + 1}`,
      bootstrapServers: 'localhost:9092',
      schemaRegistryUrl: '',
      connectUrl: '',
    })

  const importFromProject = () =>
    setActiveCluster(
      addCluster({
        name: 'Project connection',
        bootstrapServers: environment.bootstrapServers || 'localhost:9092',
        schemaRegistryUrl: environment.schemaRegistryUrl ?? '',
        connectUrl: '',
      }),
    )

  return (
    <Modal title="Manage clusters" onClose={onClose} width="w-[560px]">
      {clusters.length === 0 && (
        <p className="text-xs text-slate-400">
          No clusters yet. Add one, or import the project's connection from
          Settings.
        </p>
      )}

      {clusters.map((cluster) => (
        <div
          key={cluster.id}
          className={`flex flex-col gap-1.5 rounded border p-2.5 ${
            cluster.id === activeId
              ? 'border-emerald-300 bg-emerald-50/40'
              : 'border-slate-200'
          }`}
        >
          <div className="flex items-center gap-1.5">
            <input
              className={`${inputCls} font-medium`}
              value={cluster.name}
              placeholder="Cluster name"
              onChange={(e) =>
                updateCluster(cluster.id, { name: e.target.value })
              }
            />
            <button
              type="button"
              onClick={() => setActiveCluster(cluster.id)}
              title="Set active"
              className={`flex items-center gap-1 rounded border px-1.5 py-1 text-xs ${
                cluster.id === activeId
                  ? 'border-emerald-300 bg-emerald-100 text-emerald-700'
                  : 'border-slate-300 text-slate-500 hover:bg-slate-100'
              }`}
            >
              <Check className="h-3 w-3" />
              {cluster.id === activeId ? 'Active' : 'Use'}
            </button>
            <button
              type="button"
              onClick={() => removeCluster(cluster.id)}
              aria-label="Remove cluster"
              className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </button>
          </div>
          <label className="flex flex-col gap-0.5 text-[11px] text-slate-500">
            Bootstrap servers
            <input
              className={`${inputCls} font-mono`}
              value={cluster.bootstrapServers}
              placeholder="localhost:9092"
              onChange={(e) =>
                updateCluster(cluster.id, { bootstrapServers: e.target.value })
              }
            />
          </label>
          <div className="flex gap-1.5">
            <label className="flex flex-1 flex-col gap-0.5 text-[11px] text-slate-500">
              Schema Registry URL
              <input
                className={`${inputCls} font-mono`}
                value={cluster.schemaRegistryUrl}
                placeholder="http://localhost:8085"
                onChange={(e) =>
                  updateCluster(cluster.id, {
                    schemaRegistryUrl: e.target.value,
                  })
                }
              />
            </label>
            <label className="flex flex-1 flex-col gap-0.5 text-[11px] text-slate-500">
              Kafka Connect URL
              <input
                className={`${inputCls} font-mono`}
                value={cluster.connectUrl}
                placeholder="http://localhost:8083"
                onChange={(e) =>
                  updateCluster(cluster.id, { connectUrl: e.target.value })
                }
              />
            </label>
          </div>
        </div>
      ))}

      <div className="flex gap-2">
        <button
          type="button"
          onClick={addBlank}
          className="flex flex-1 items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1.5 text-xs text-slate-500 hover:bg-slate-100"
        >
          <Plus className="h-3.5 w-3.5" />
          Add cluster
        </button>
        <button
          type="button"
          onClick={importFromProject}
          className="rounded border border-slate-300 px-2 py-1.5 text-xs text-slate-500 hover:bg-slate-100"
        >
          Import project connection
        </button>
      </div>
      <p className="text-[11px] text-slate-400">
        Saved in this browser only — never exported with the project.
      </p>
    </Modal>
  )
}
