import {
  ArrowRightLeft,
  Database,
  Eye,
  Filter,
  FilterX,
  FoldVertical,
  GitBranch,
  GitFork,
  Group,
  Hash,
  KeyRound,
  Layers,
  Merge,
  Plug,
  PlugZap,
  Repeat,
  Replace,
  Send,
  Shuffle,
  Sigma,
  Spline,
  Split,
  Table,
  Table2,
} from 'lucide-react'

/** Component type of a lucide-react icon, derived to stay version-agnostic. */
type IconComponent = typeof Database

/** Icon names referenced by node schemas, mapped to lucide components. */
const ICONS: Record<string, IconComponent> = {
  ArrowRightLeft,
  Database,
  Eye,
  Filter,
  FilterX,
  FoldVertical,
  GitBranch,
  GitFork,
  Group,
  Hash,
  KeyRound,
  Layers,
  Merge,
  Plug,
  PlugZap,
  Repeat,
  Replace,
  Send,
  Shuffle,
  Sigma,
  Spline,
  Split,
  Table,
  Table2,
}

interface NodeIconProps {
  name: string
  className?: string
}

export function NodeIcon({ name, className }: NodeIconProps) {
  const Icon = ICONS[name] ?? Database
  return <Icon className={className} />
}
