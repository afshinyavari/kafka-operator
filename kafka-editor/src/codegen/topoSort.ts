/**
 * Topological sort of the topology graph (Kahn's algorithm). Sources come
 * first, so codegen can emit each node after the nodes it depends on.
 */
export function topoSort<N extends { id: string }>(
  nodes: N[],
  edges: { source: string; target: string }[],
): N[] | null {
  const indegree = new Map<string, number>()
  const adjacency = new Map<string, string[]>()
  for (const node of nodes) {
    indegree.set(node.id, 0)
    adjacency.set(node.id, [])
  }
  for (const edge of edges) {
    if (!indegree.has(edge.source) || !indegree.has(edge.target)) continue
    adjacency.get(edge.source)?.push(edge.target)
    indegree.set(edge.target, (indegree.get(edge.target) ?? 0) + 1)
  }

  const queue = nodes
    .filter((n) => (indegree.get(n.id) ?? 0) === 0)
    .map((n) => n.id)
  const order: string[] = []
  while (queue.length > 0) {
    const id = queue.shift() as string
    order.push(id)
    for (const next of adjacency.get(id) ?? []) {
      const remaining = (indegree.get(next) ?? 0) - 1
      indegree.set(next, remaining)
      if (remaining === 0) queue.push(next)
    }
  }

  // A short order means a cycle left some nodes unreachable.
  if (order.length !== nodes.length) return null

  const byId = new Map(nodes.map((n) => [n.id, n]))
  return order.flatMap((id) => {
    const node = byId.get(id)
    return node ? [node] : []
  })
}
