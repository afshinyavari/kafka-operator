/** Tailwind classes for a Kafka Connect connector / task state badge. */
export function stateColor(state: string): string {
  switch (state.toUpperCase()) {
    case 'RUNNING':
      return 'bg-emerald-100 text-emerald-700'
    case 'PAUSED':
      return 'bg-amber-100 text-amber-700'
    case 'FAILED':
      return 'bg-rose-100 text-rose-700'
    default:
      return 'bg-slate-100 text-slate-500'
  }
}
