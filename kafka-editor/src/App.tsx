import { EditorLayout } from './app/EditorLayout'
import { useBootstrapOperatorMode } from './operator/useOperatorConfig'

export default function App() {
  // Fire once on mount: detect operator-mode and auto-bootstrap the managed
  // cluster so the cluster-aware UI has something to activate.
  useBootstrapOperatorMode()
  return <EditorLayout />
}
