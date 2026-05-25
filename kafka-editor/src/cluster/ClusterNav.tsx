import {
  FileJson,
  LayoutDashboard,
  List,
  Plug,
  ShieldCheck,
  Users,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import type { ClusterSection } from '../state/viewStore'

const ITEMS: { id: ClusterSection; label: string; icon: LucideIcon }[] = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'topics', label: 'Topics', icon: List },
  { id: 'groups', label: 'Consumer Groups', icon: Users },
  { id: 'schemas', label: 'Schemas', icon: FileJson },
  { id: 'acls', label: 'ACLs', icon: ShieldCheck },
  { id: 'connect', label: 'Connect', icon: Plug },
]

interface ClusterNavProps {
  section: ClusterSection
  onSelect: (section: ClusterSection) => void
}

/** The Cluster view's persistent left navigation. */
export function ClusterNav({ section, onSelect }: ClusterNavProps) {
  return (
    <nav className="flex w-44 shrink-0 flex-col gap-0.5 border-r border-slate-200 bg-slate-50 p-2">
      {ITEMS.map(({ id, label, icon: Icon }) => (
        <button
          key={id}
          type="button"
          onClick={() => onSelect(id)}
          className={`flex items-center gap-2 rounded px-2 py-1.5 text-xs ${
            section === id
              ? 'bg-slate-800 text-white'
              : 'text-slate-600 hover:bg-slate-200'
          }`}
        >
          <Icon className="h-3.5 w-3.5" />
          {label}
        </button>
      ))}
    </nav>
  )
}
