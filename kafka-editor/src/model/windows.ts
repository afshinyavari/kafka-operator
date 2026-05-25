/**
 * Windowing configuration for the aggregation operators (count / reduce /
 * aggregate). A `type` of 'none' means an unwindowed aggregation.
 */

export type WindowType =
  | 'none'
  | 'tumbling'
  | 'hopping'
  | 'sliding'
  | 'session'

export interface WindowConfig {
  type: WindowType
  /** Window size — or the inactivity gap for session windows (ms). */
  sizeMs: number
  /** Hop interval, for hopping windows (ms). */
  advanceMs?: number
  /** Allowed lateness (ms). */
  graceMs?: number
}

export const WINDOW_TYPES: { value: WindowType; label: string }[] = [
  { value: 'none', label: 'None (unwindowed)' },
  { value: 'tumbling', label: 'Tumbling' },
  { value: 'hopping', label: 'Hopping' },
  { value: 'sliding', label: 'Sliding' },
  { value: 'session', label: 'Session' },
]

export function defaultWindow(): WindowConfig {
  return { type: 'none', sizeMs: 60000 }
}

/** Runtime type guard for a `WindowConfig`. */
export function isWindowConfig(value: unknown): value is WindowConfig {
  if (typeof value !== 'object' || value === null) return false
  const v = value as Record<string, unknown>
  return typeof v.type === 'string' && typeof v.sizeMs === 'number'
}

/** A short human-readable rendering of a window, for node summaries. */
export function describeWindow(window: WindowConfig): string {
  switch (window.type) {
    case 'none':
      return 'unwindowed'
    case 'hopping':
      return `hopping ${window.sizeMs}ms / ${window.advanceMs ?? '?'}ms`
    case 'session':
      return `session gap ${window.sizeMs}ms`
    default:
      return `${window.type} ${window.sizeMs}ms`
  }
}
