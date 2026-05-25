import { useEffect, useMemo, useState } from 'react'
import { Check, Copy, Download, Package, X } from 'lucide-react'
import type { ProjectDocument } from '../model/project'
import { generateProject } from './generate'
import { createZip } from './zip'

interface CodegenModalProps {
  doc: ProjectDocument
  onClose: () => void
}

/** Modal showing the generated Quarkus project files. */
export function CodegenModal({ doc, onClose }: CodegenModalProps) {
  const files = useMemo(() => generateProject(doc), [doc])
  const [activeIndex, setActiveIndex] = useState(0)
  const [copied, setCopied] = useState(false)

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const file = files[activeIndex] ?? files[0]

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(file.content)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard may be unavailable; ignore.
    }
  }

  const download = () => {
    const blob = new Blob([file.content], {
      type: 'text/plain;charset=utf-8',
    })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = file.path.split('/').pop() ?? 'file.txt'
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  const downloadAll = () => {
    const blob = new Blob([createZip(files)], { type: 'application/zip' })
    const url = URL.createObjectURL(blob)
    const link = document.createElement('a')
    link.href = url
    link.download = 'kafka-streams-app.zip'
    document.body.appendChild(link)
    link.click()
    link.remove()
    URL.revokeObjectURL(url)
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-6"
      onClick={onClose}
    >
      <div
        className="flex h-[80vh] w-[920px] flex-col rounded-lg bg-white shadow-xl"
        onClick={(e) => e.stopPropagation()}
      >
        <header className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
          <div>
            <h2 className="text-sm font-semibold text-slate-800">
              Generated Code
            </h2>
            <p className="text-[11px] text-slate-400">
              A Quarkus Kafka Streams project — review the // TODO markers.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={downloadAll}
              className="flex items-center gap-1.5 rounded border border-slate-800 bg-slate-800 px-2 py-1 text-xs font-medium text-white hover:bg-slate-700"
            >
              <Package className="h-3.5 w-3.5" />
              Download all (.zip)
            </button>
            <button
              type="button"
              onClick={onClose}
              aria-label="Close"
              className="rounded p-1 text-slate-500 hover:bg-slate-100"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </header>

        <div className="flex min-h-0 flex-1">
          <div className="w-60 shrink-0 overflow-y-auto border-r border-slate-200 p-2">
            {files.map((f, i) => (
              <button
                key={f.path}
                type="button"
                onClick={() => setActiveIndex(i)}
                className={`w-full truncate rounded px-2 py-1 text-left text-xs ${
                  i === activeIndex
                    ? 'bg-slate-700 text-white'
                    : 'text-slate-600 hover:bg-slate-100'
                }`}
                title={f.path}
              >
                {f.path}
              </button>
            ))}
          </div>

          <div className="flex min-w-0 flex-1 flex-col">
            <div className="flex items-center justify-between border-b border-slate-100 px-3 py-1.5">
              <span className="truncate font-mono text-[11px] text-slate-500">
                {file.path}
              </span>
              <div className="flex shrink-0 items-center gap-1">
                <button
                  type="button"
                  onClick={download}
                  className="flex items-center gap-1 rounded border border-slate-300 px-2 py-0.5 text-xs text-slate-600 hover:bg-slate-100"
                >
                  <Download className="h-3.5 w-3.5" />
                  Download
                </button>
                <button
                  type="button"
                  onClick={copy}
                  className="flex items-center gap-1 rounded border border-slate-300 px-2 py-0.5 text-xs text-slate-600 hover:bg-slate-100"
                >
                  {copied ? (
                    <Check className="h-3.5 w-3.5 text-emerald-600" />
                  ) : (
                    <Copy className="h-3.5 w-3.5" />
                  )}
                  {copied ? 'Copied' : 'Copy'}
                </button>
              </div>
            </div>
            <pre className="flex-1 overflow-auto bg-slate-50 p-3 font-mono text-[11px] leading-relaxed text-slate-800">
              {file.content}
            </pre>
          </div>
        </div>
      </div>
    </div>
  )
}
