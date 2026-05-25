import { Plus, Trash2 } from 'lucide-react'
import {
  defaultFieldType,
  FIELD_KINDS,
  newField,
  PRIMITIVE_TYPES,
} from '../model/recordTypes'
import type {
  FieldKind,
  FieldType,
  PrimitiveType,
  RecordField,
  RecordType,
} from '../model/recordTypes'

const inputCls =
  'rounded border border-slate-300 px-1.5 py-1 text-xs text-slate-800 outline-none focus:border-slate-500'

interface FieldRowEditorProps {
  field: RecordField
  recordTypes: RecordType[]
  onChange: (field: RecordField) => void
  onRemove: () => void
  depth?: number
}

/** Edits one field of a record type; recurses for nested records and arrays. */
export function FieldRowEditor({
  field,
  recordTypes,
  onChange,
  onRemove,
  depth = 0,
}: FieldRowEditorProps) {
  return (
    <div
      className="flex flex-col gap-1 rounded border border-slate-200 bg-slate-50 p-2"
      style={{ marginLeft: depth * 14 }}
    >
      <div className="flex items-center gap-1">
        <input
          className={`${inputCls} min-w-0 flex-1`}
          placeholder="field name"
          value={field.name}
          onChange={(e) => onChange({ ...field, name: e.target.value })}
        />
        <select
          className={inputCls}
          value={field.type.kind}
          onChange={(e) =>
            onChange({
              ...field,
              type: defaultFieldType(e.target.value as FieldKind),
            })
          }
        >
          {FIELD_KINDS.map((k) => (
            <option key={k} value={k}>
              {k}
            </option>
          ))}
        </select>
        <label className="flex items-center gap-1 text-[10px] text-slate-500">
          <input
            type="checkbox"
            checked={field.nullable ?? false}
            onChange={(e) => onChange({ ...field, nullable: e.target.checked })}
          />
          nullable
        </label>
        <button
          type="button"
          onClick={onRemove}
          aria-label="Remove field"
          className="rounded border border-slate-300 p-1 text-slate-500 hover:bg-slate-100"
        >
          <Trash2 className="h-3 w-3" />
        </button>
      </div>
      <FieldTypeEditor
        type={field.type}
        recordTypes={recordTypes}
        depth={depth}
        onChange={(type) => onChange({ ...field, type })}
      />
    </div>
  )
}

interface FieldTypeEditorProps {
  type: FieldType
  recordTypes: RecordType[]
  depth: number
  onChange: (type: FieldType) => void
}

/** Edits the kind-specific detail of a `FieldType`. */
function FieldTypeEditor({
  type,
  recordTypes,
  depth,
  onChange,
}: FieldTypeEditorProps) {
  switch (type.kind) {
    case 'primitive':
      return (
        <select
          className={inputCls}
          value={type.primitive}
          onChange={(e) =>
            onChange({
              kind: 'primitive',
              primitive: e.target.value as PrimitiveType,
            })
          }
        >
          {PRIMITIVE_TYPES.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
      )
    case 'enum':
      return (
        <input
          className={inputCls}
          placeholder="comma,separated,symbols"
          value={type.symbols.join(',')}
          onChange={(e) =>
            onChange({
              kind: 'enum',
              symbols: e.target.value
                .split(',')
                .map((s) => s.trim())
                .filter(Boolean),
            })
          }
        />
      )
    case 'array':
      return (
        <div className="flex flex-col gap-1">
          <span className="text-[10px] text-slate-400">array of:</span>
          <FieldTypeEditor
            type={type.items}
            recordTypes={recordTypes}
            depth={depth + 1}
            onChange={(items) => onChange({ kind: 'array', items })}
          />
        </div>
      )
    case 'record':
      return (
        <div className="flex flex-col gap-1">
          {type.fields.map((f, i) => (
            <FieldRowEditor
              key={i}
              field={f}
              recordTypes={recordTypes}
              depth={depth + 1}
              onChange={(nf) =>
                onChange({
                  kind: 'record',
                  fields: type.fields.map((x, idx) => (idx === i ? nf : x)),
                })
              }
              onRemove={() =>
                onChange({
                  kind: 'record',
                  fields: type.fields.filter((_, idx) => idx !== i),
                })
              }
            />
          ))}
          <button
            type="button"
            onClick={() =>
              onChange({ kind: 'record', fields: [...type.fields, newField()] })
            }
            className="flex items-center justify-center gap-1 rounded border border-dashed border-slate-300 px-2 py-1 text-[11px] text-slate-500 hover:bg-slate-100"
            style={{ marginLeft: (depth + 1) * 14 }}
          >
            <Plus className="h-3 w-3" />
            Add nested field
          </button>
        </div>
      )
    case 'ref':
      return (
        <select
          className={inputCls}
          value={type.recordTypeId}
          onChange={(e) =>
            onChange({ kind: 'ref', recordTypeId: e.target.value })
          }
        >
          <option value="">— select a record type —</option>
          {recordTypes.map((rt) => (
            <option key={rt.id} value={rt.id}>
              {rt.name}
            </option>
          ))}
        </select>
      )
    case 'unknown':
      return (
        <span className="text-[10px] text-slate-400">
          unsupported type — imported without detail
        </span>
      )
  }
}
