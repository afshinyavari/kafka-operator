import { useEffect, useState } from 'react'
import { ChevronLeft, Pencil, Plus, Trash2, X } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { newRecordType } from '../model/recordTypes'
import { RecordTypeEditor } from './RecordTypeEditor'
import { ApicurioBrowser } from './ApicurioBrowser'

interface RecordTypesModalProps {
  onClose: () => void
}

type Tab = 'types' | 'apicurio'

/** Project-global modal for defining and importing record types. */
export function RecordTypesModal({ onClose }: RecordTypesModalProps) {
  const recordTypes = useEditorStore((s) => s.recordTypes)
  const addRecordType = useEditorStore((s) => s.addRecordType)
  const removeRecordType = useEditorStore((s) => s.removeRecordType)
  const [tab, setTab] = useState<Tab>('types')
  const [editingId, setEditingId] = useState<string | null>(null)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const editing = recordTypes.find((rt) => rt.id === editingId)

  const createNew = () => {
    const rt = newRecordType(`Record${recordTypes.length + 1}`)
    addRecordType(rt)
    setEditingId(rt.id)
  }

  const tabButton = (id: Tab, label: string) => (
    <button
      type="button"
      onClick={() => {
        setTab(id)
        setEditingId(null)
      }}
      className={`border-b-2 px-3 py-1.5 text-xs font-medium ${
        tab === id
          ? 'border-slate-700 text-slate-800'
          : 'border-transparent text-slate-400 hover:text-slate-600'
      }`}
    >
      {label}
    </button>
  )

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-6"
      onClick={onClose}
    >
      <div
        className="flex max-h-[80vh] w-[640px] flex-col rounded-lg bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <div className="flex items-center gap-2">
            {editing && (
              <button
                type="button"
                onClick={() => setEditingId(null)}
                aria-label="Back to list"
                className="rounded p-1 text-slate-500 hover:bg-slate-100"
              >
                <ChevronLeft className="h-4 w-4" />
              </button>
            )}
            <h2 className="text-sm font-semibold text-slate-800">
              {editing ? 'Edit record type' : 'Record Types'}
            </h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Close"
            className="rounded p-1 text-slate-500 hover:bg-slate-100"
          >
            <X className="h-4 w-4" />
          </button>
        </header>

        {!editing && (
          <div className="flex gap-1 border-b border-slate-200 px-3">
            {tabButton('types', 'My Types')}
            {tabButton('apicurio', 'Browse Apicurio')}
          </div>
        )}

        <div className="overflow-y-auto p-4">
          {editing ? (
            <RecordTypeEditor recordType={editing} />
          ) : tab === 'apicurio' ? (
            <ApicurioBrowser />
          ) : (
            <div className="flex flex-col gap-2">
              <p className="text-[11px] text-slate-400">
                Record types describe the shape of your Kafka record values.
                They power field autocomplete in the predicate builder.
              </p>
              {recordTypes.length === 0 && (
                <p className="py-4 text-center text-sm text-slate-400">
                  No record types yet.
                </p>
              )}
              {recordTypes.map((rt) => (
                <div
                  key={rt.id}
                  className="flex items-center justify-between rounded border border-slate-200 px-3 py-2"
                >
                  <div className="min-w-0">
                    <div className="flex items-center gap-1.5">
                      <span className="truncate text-sm font-medium text-slate-800">
                        {rt.name}
                      </span>
                      {rt.source.kind === 'apicurio' && (
                        <span className="rounded bg-violet-100 px-1 py-0.5 text-[9px] font-semibold tracking-wide text-violet-600 uppercase">
                          Apicurio
                        </span>
                      )}
                    </div>
                    <div className="text-[11px] text-slate-400">
                      {rt.fields.length}{' '}
                      {rt.fields.length === 1 ? 'field' : 'fields'}
                      {rt.source.kind === 'apicurio' &&
                        ` · ${rt.source.artifactId}`}
                    </div>
                  </div>
                  <div className="flex gap-1">
                    <button
                      type="button"
                      onClick={() => setEditingId(rt.id)}
                      aria-label="Edit record type"
                      className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </button>
                    <button
                      type="button"
                      onClick={() => removeRecordType(rt.id)}
                      aria-label="Delete record type"
                      className="rounded border border-rose-300 p-1 text-rose-500 hover:bg-rose-50"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                    </button>
                  </div>
                </div>
              ))}
              <button
                type="button"
                onClick={createNew}
                className="mt-1 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1.5 text-xs text-slate-600 hover:bg-slate-100"
              >
                <Plus className="h-3.5 w-3.5" />
                New record type
              </button>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
