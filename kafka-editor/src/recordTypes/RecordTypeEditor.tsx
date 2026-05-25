import { Plus } from 'lucide-react'
import { useEditorStore } from '../state/store'
import { newField } from '../model/recordTypes'
import type { RecordField, RecordType } from '../model/recordTypes'
import { FieldRowEditor } from './FieldRowEditor'

interface RecordTypeEditorProps {
  recordType: RecordType
}

/** Edits one record type — its name and its fields. Writes through live. */
export function RecordTypeEditor({ recordType }: RecordTypeEditorProps) {
  const updateRecordType = useEditorStore((s) => s.updateRecordType)
  const recordTypes = useEditorStore((s) => s.recordTypes)

  const setFields = (fields: RecordField[]) =>
    updateRecordType(recordType.id, { fields })

  return (
    <div className="flex flex-col gap-3">
      <label className="flex flex-col gap-1">
        <span className="text-xs font-medium text-slate-600">Name</span>
        <input
          className="w-full rounded border border-slate-300 px-2 py-1 text-sm text-slate-800 outline-none focus:border-slate-500"
          value={recordType.name}
          onChange={(e) =>
            updateRecordType(recordType.id, { name: e.target.value })
          }
        />
      </label>

      <div className="flex flex-col gap-1.5">
        <span className="text-xs font-medium text-slate-600">Fields</span>
        {recordType.fields.length === 0 && (
          <span className="text-[11px] text-slate-400">No fields yet.</span>
        )}
        {recordType.fields.map((field, i) => (
          <FieldRowEditor
            key={i}
            field={field}
            recordTypes={recordTypes}
            onChange={(f) =>
              setFields(recordType.fields.map((x, idx) => (idx === i ? f : x)))
            }
            onRemove={() =>
              setFields(recordType.fields.filter((_, idx) => idx !== i))
            }
          />
        ))}
        <button
          type="button"
          onClick={() => setFields([...recordType.fields, newField()])}
          className="mt-0.5 flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-xs text-slate-500 hover:bg-slate-100"
        >
          <Plus className="h-3.5 w-3.5" />
          Add field
        </button>
      </div>
    </div>
  )
}
