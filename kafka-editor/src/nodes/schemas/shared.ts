import type { PropertySpec } from '../types'

/** Help text shown on operator-logic fields until the M3 structured builder. */
export const LOGIC_HELP =
  'Free text for now — a structured, guided builder replaces this in milestone M3.'

/** A placeholder text property capturing operator logic until M3. */
export function logicProp(
  key: string,
  label: string,
  placeholder: string,
): PropertySpec {
  return { key, label, kind: 'text', placeholder, help: LOGIC_HELP }
}
