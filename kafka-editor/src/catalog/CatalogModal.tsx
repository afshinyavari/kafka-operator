import { useEffect } from 'react'
import { Plus, Trash2, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { formatCarriesRecordType, newTopic, SERDE_KINDS } from '../model/catalog'
import type { SerdeKind } from '../model/catalog'

interface CatalogModalProps {
  onClose: () => void
}

const fieldCls =
  'rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

const removeBtnCls =
  'rounded border border-rose-300 p-1 text-rose-500 hover:bg-rose-50'

const addBtnCls =
  'mt-1 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1.5 text-xs text-slate-600 hover:bg-slate-100'

/** Project-global modal for managing catalog topics and their types. */
export function CatalogModal({ onClose }: CatalogModalProps) {
  const catalog = useEditorStore((s) => s.catalog)
  const recordTypes = useEditorStore((s) => s.recordTypes)
  const addTopic = useEditorStore((s) => s.addTopic)
  const updateTopic = useEditorStore((s) => s.updateTopic)
  const removeTopic = useEditorStore((s) => s.removeTopic)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-6"
      onClick={onClose}
    >
      <div
        className="flex max-h-[80vh] w-[600px] flex-col rounded-lg bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <h2 className="text-sm font-semibold text-slate-800">Catalog</h2>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-4 w-4" />
          </button>
        </header>

        <div className="flex flex-col gap-2 overflow-y-auto p-4">
          <p className="text-[11px] text-slate-400">
            Topics referenced by source and sink nodes. A topic's value type is
            inherited by the nodes that use it.
          </p>
          {catalog.topics.length === 0 && (
            <p className="py-2 text-center text-sm text-slate-400">
              No topics yet.
            </p>
          )}
          {catalog.topics.map((topic) => {
            const valueFormat = topic.valueFormat ?? 'json'
            return (
              <div
                key={topic.id}
                className="flex flex-col gap-1.5 rounded border border-slate-200 p-2"
              >
                <div className="flex items-center gap-1">
                  <input
                    className={`${fieldCls} min-w-0 flex-1`}
                    value={topic.name}
                    placeholder="topic name"
                    onChange={(e) =>
                      updateTopic(topic.id, { name: e.target.value })
                    }
                  />
                  <input
                    type="number"
                    className={`${fieldCls} w-20`}
                    value={topic.partitions ?? ''}
                    placeholder="parts"
                    onChange={(e) =>
                      updateTopic(topic.id, {
                        partitions:
                          e.target.value === ''
                            ? undefined
                            : Number(e.target.value),
                      })
                    }
                  />
                  <button
                    type="button"
                    onClick={() => removeTopic(topic.id)}
                    aria-label="Remove topic"
                    className={removeBtnCls}
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                  </button>
                </div>

                <div className="flex items-center gap-2">
                  <label className="flex items-center gap-1 text-[11px] text-slate-500">
                    Key
                    <select
                      className={fieldCls}
                      value={topic.keyFormat ?? 'string'}
                      onChange={(e) =>
                        updateTopic(topic.id, {
                          keyFormat: e.target.value as SerdeKind,
                        })
                      }
                    >
                      {SERDE_KINDS.map((k) => (
                        <option key={k} value={k}>
                          {k}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="flex items-center gap-1 text-[11px] text-slate-500">
                    Value
                    <select
                      className={fieldCls}
                      value={valueFormat}
                      onChange={(e) =>
                        updateTopic(topic.id, {
                          valueFormat: e.target.value as SerdeKind,
                        })
                      }
                    >
                      {SERDE_KINDS.map((k) => (
                        <option key={k} value={k}>
                          {k}
                        </option>
                      ))}
                    </select>
                  </label>
                  {formatCarriesRecordType(valueFormat) && (
                    <label className="flex min-w-0 flex-1 items-center gap-1 text-[11px] text-slate-500">
                      Value type
                      <select
                        className={`${fieldCls} min-w-0 flex-1`}
                        value={topic.valueRecordTypeId ?? ''}
                        onChange={(e) =>
                          updateTopic(topic.id, {
                            valueRecordTypeId: e.target.value || undefined,
                          })
                        }
                      >
                        <option value="">— none —</option>
                        {recordTypes.map((rt) => (
                          <option key={rt.id} value={rt.id}>
                            {rt.name}
                          </option>
                        ))}
                      </select>
                    </label>
                  )}
                </div>
              </div>
            )
          })}
          <button
            type="button"
            onClick={() =>
              addTopic(newTopic(`topic-${catalog.topics.length + 1}`))
            }
            className={addBtnCls}
          >
            <Plus className="h-3.5 w-3.5" />
            New topic
          </button>
        </div>
      </div>
    </div>
  )
}
