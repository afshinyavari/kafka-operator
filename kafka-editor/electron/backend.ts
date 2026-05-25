import { execFile, execFileSync, spawn, type ChildProcess } from 'node:child_process'
import { createWriteStream, existsSync, mkdirSync, type WriteStream } from 'node:fs'
import { get } from 'node:http'
import { createServer } from 'node:net'
import { join } from 'node:path'
import { app } from 'electron'
import { resolveBackendPaths } from './paths'

/**
 * How long to wait for the backend to answer /api/health before giving up.
 * Generous because first-launch cold reads from an AppImage's FUSE-mounted
 * squashfs (where the bundled JRE + ~100 MB of jars live) can take ~60 s on
 * slower disks. Subsequent launches hit the kernel page cache and start in
 * ~1 s — but a single tight deadline must accommodate both.
 */
const HEALTH_TIMEOUT_MS = 90_000
/** Grace period after SIGTERM before the backend is force-killed. */
const SHUTDOWN_GRACE_MS = 5_000

export interface BackendHandle {
  /** TCP port the backend is listening on. */
  port: number
  /** Absolute path of the backend log file. */
  logPath: string
  /** Terminate the backend; resolves once the process is gone. */
  stop(): Promise<void>
}

/** Raised when the backend cannot be started; carries log context for the UI. */
export class BackendError extends Error {
  constructor(
    message: string,
    public readonly logPath: string,
    public readonly tail: string,
  ) {
    super(message)
    this.name = 'BackendError'
  }
}

/** The running backend process, tracked so it can never outlive Electron. */
let activeChild: ChildProcess | undefined

/** Ask the OS for an unused TCP port on the loopback interface. */
function findFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer()
    srv.on('error', reject)
    srv.listen(0, '127.0.0.1', () => {
      const addr = srv.address()
      if (addr && typeof addr === 'object') {
        const { port } = addr
        srv.close(() => resolve(port))
      } else {
        srv.close(() => reject(new Error('Could not determine a free port')))
      }
    })
  })
}

/** Poll GET /api/health until it returns 200, the process dies, or we time out. */
function waitForHealth(
  port: number,
  timeoutMs: number,
  isAlive: () => boolean,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  return new Promise((resolve, reject) => {
    let settled = false
    const finish = (err?: Error) => {
      if (settled) return
      settled = true
      if (err) reject(err)
      else resolve()
    }
    const poll = () => {
      if (settled) return
      if (!isAlive()) {
        finish(new Error('Backend process exited before becoming healthy'))
        return
      }
      if (Date.now() > deadline) {
        finish(new Error(`Backend did not answer /api/health within ${timeoutMs} ms`))
        return
      }
      const req = get(
        { host: '127.0.0.1', port, path: '/api/health', timeout: 2000 },
        (res) => {
          res.resume()
          if (res.statusCode === 200) finish()
          else setTimeout(poll, 250)
        },
      )
      req.on('error', () => setTimeout(poll, 250))
      req.on('timeout', () => req.destroy())
    }
    poll()
  })
}

/** Force-kill the process (and, on Windows, its child tree). */
function killNow(child: ChildProcess): void {
  if (child.pid === undefined || child.exitCode !== null) return
  if (process.platform === 'win32') {
    execFile('taskkill', ['/pid', String(child.pid), '/T', '/F'])
  } else {
    child.kill('SIGKILL')
  }
}

/** Graceful stop: SIGTERM, then SIGKILL after a grace period. */
function stopChild(child: ChildProcess, logStream: WriteStream): Promise<void> {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.pid === undefined) {
      logStream.end()
      resolve()
      return
    }
    let timer: NodeJS.Timeout
    const done = () => {
      clearTimeout(timer)
      logStream.end()
      resolve()
    }
    child.once('exit', done)
    if (process.platform === 'win32') {
      execFile('taskkill', ['/pid', String(child.pid), '/T', '/F'])
    } else {
      child.kill('SIGTERM')
    }
    timer = setTimeout(() => killNow(child), SHUTDOWN_GRACE_MS)
  })
}

/**
 * Launch the Quarkus backend as a child process and wait until it is healthy.
 * Rejects with a {@link BackendError} (carrying log context) on failure.
 */
export async function startBackend(): Promise<BackendHandle> {
  const { javaBin, backendDir, jar } = resolveBackendPaths()

  const logDir = app.getPath('logs')
  mkdirSync(logDir, { recursive: true })
  const logPath = join(logDir, 'backend.log')
  const logStream = createWriteStream(logPath, { flags: 'w' })

  if (!existsSync(jar)) {
    logStream.end()
    throw new BackendError(`Backend jar not found at ${jar}`, logPath, '')
  }

  const port = await findFreePort()

  // Keep a rolling tail of output for the error dialog.
  const tail: string[] = []
  const record = (chunk: Buffer | string) => {
    const text = chunk.toString()
    logStream.write(text)
    for (const line of text.split('\n')) {
      tail.push(line)
      if (tail.length > 200) tail.shift()
    }
  }

  logStream.write(`$ ${javaBin} -jar ${jar}\n  port=${port} cwd=${backendDir}\n\n`)

  const child = spawn(
    javaBin,
    [
      `-Dquarkus.http.port=${port}`,
      '-Dquarkus.http.host=127.0.0.1',
      '-jar',
      jar,
    ],
    { cwd: backendDir, stdio: ['ignore', 'pipe', 'pipe'], windowsHide: true },
  )
  activeChild = child
  child.stdout?.on('data', record)
  child.stderr?.on('data', record)

  let exited = false
  let exitInfo = ''
  child.on('exit', (code, signal) => {
    exited = true
    activeChild = undefined
    exitInfo = `backend process exited (code=${code}, signal=${signal})`
    logStream.write(`\n${exitInfo}\n`)
  })
  child.on('error', (err) => {
    exited = true
    activeChild = undefined
    exitInfo = `failed to launch backend: ${err.message}`
    record(`\n${exitInfo}\n`)
  })

  try {
    await waitForHealth(port, HEALTH_TIMEOUT_MS, () => !exited)
  } catch (err) {
    killNow(child)
    const reason = `${(err as Error).message}.${exitInfo ? ` ${exitInfo}` : ''}`
    throw new BackendError(reason, logPath, tail.slice(-40).join('\n').trim())
  }

  return {
    port,
    logPath,
    stop: () => stopChild(child, logStream),
  }
}

/**
 * Synchronous best-effort kill, for the `process.on('exit')` last-resort hook —
 * no async work is possible there.
 */
export function killBackendSync(): void {
  const child = activeChild
  if (!child || child.exitCode !== null || child.pid === undefined) return
  try {
    if (process.platform === 'win32') {
      execFileSync('taskkill', ['/pid', String(child.pid), '/T', '/F'])
    } else {
      child.kill('SIGKILL')
    }
  } catch {
    // Best effort — we are already shutting down.
  }
}
