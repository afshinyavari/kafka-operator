import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * A minimal server-state hook for the Cluster view — no react-query dependency.
 * Re-fetches when `key` changes or `refetch()` is called. `loading` is derived
 * (not stored), and the only `setState` happens inside the promise callbacks,
 * so the hook keeps effects free of synchronous state writes.
 */
export interface QueryResult<T> {
  data: T | undefined
  loading: boolean
  error: string | null
  refetch: () => void
}

interface QueryState<T> {
  token: string
  key: string
  data?: T
  error?: string
}

export function useAdminQuery<T>(
  key: string,
  fetcher: () => Promise<T>,
): QueryResult<T> {
  // Hold the (per-render) fetcher in a ref so the fetch effect can depend only
  // on `token`, not on a closure that changes every render.
  const fetcherRef = useRef(fetcher)
  useEffect(() => {
    fetcherRef.current = fetcher
  })

  const [nonce, setNonce] = useState(0)
  const token = `${key}#${nonce}`
  const [result, setResult] = useState<QueryState<T>>({ token: '', key: '' })

  useEffect(() => {
    let cancelled = false
    fetcherRef.current()
      .then((data) => {
        if (!cancelled) setResult({ token, key, data })
      })
      .catch((e: unknown) => {
        if (!cancelled) {
          setResult({
            token,
            key,
            error: e instanceof Error ? e.message : String(e),
          })
        }
      })
    return () => {
      cancelled = true
    }
  }, [token, key])

  const refetch = useCallback(() => setNonce((n) => n + 1), [])

  const loading = result.token !== token
  // Stale data from a previous `key` is hidden; a refetch of the same key
  // keeps the data visible while it reloads.
  const sameKey = result.key === key
  return {
    data: sameKey ? result.data : undefined,
    loading,
    error: sameKey ? (result.error ?? null) : null,
    refetch,
  }
}
