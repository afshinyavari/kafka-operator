import { join } from 'node:path'
import { app, BrowserWindow, dialog, shell } from 'electron'
import { BackendError, killBackendSync, startBackend, type BackendHandle } from './backend'

let mainWindow: BrowserWindow | undefined
let splashWindow: BrowserWindow | undefined
let backend: BackendHandle | undefined
let quitting = false

/** Small frameless window shown while the backend starts up. */
function createSplashWindow(): void {
  splashWindow = new BrowserWindow({
    width: 460,
    height: 260,
    frame: false,
    resizable: false,
    center: true,
    backgroundColor: '#0f172a',
  })
  void splashWindow.loadFile(join(__dirname, 'splash.html'))
}

/** Main application window, loading the built SPA. */
function createMainWindow(apiBase: string): void {
  mainWindow = new BrowserWindow({
    width: 1440,
    height: 900,
    minWidth: 900,
    minHeight: 600,
    show: false,
    backgroundColor: '#0f172a',
    autoHideMenuBar: true,
    webPreferences: {
      preload: join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      additionalArguments: [`--api-base=${apiBase}`],
    },
  })

  // Open external links (docs, etc.) in the system browser, not the app.
  mainWindow.webContents.setWindowOpenHandler(({ url }) => {
    if (url.startsWith('http:') || url.startsWith('https:')) {
      void shell.openExternal(url)
    }
    return { action: 'deny' }
  })

  mainWindow.once('ready-to-show', () => {
    splashWindow?.close()
    splashWindow = undefined
    mainWindow?.show()
    console.log('[kafka-editor] window ready')
  })

  void mainWindow.loadFile(join(__dirname, '..', 'dist', 'index.html'))
}

/** Start the backend, then show the UI; on failure surface a dialog and quit. */
async function boot(): Promise<void> {
  console.log('[kafka-editor] starting backend…')
  createSplashWindow()
  try {
    backend = await startBackend()
  } catch (err) {
    splashWindow?.close()
    splashWindow = undefined
    const detail =
      err instanceof BackendError
        ? `${err.message}\n\nLog file: ${err.logPath}\n\n${err.tail}`
        : String(err)
    console.error('[kafka-editor] backend failed:', detail)
    dialog.showErrorBox('Kafka Editor — the backend failed to start', detail)
    app.exit(1)
    return
  }
  console.log(
    `[kafka-editor] backend ready on port ${backend.port} (log: ${backend.logPath})`,
  )
  createMainWindow(`http://127.0.0.1:${backend.port}`)
}

// Single-instance: a second launch just focuses the existing window.
if (!app.requestSingleInstanceLock()) {
  app.exit(0)
} else {
  app.on('second-instance', () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore()
      mainWindow.focus()
    }
  })

  void app.whenReady().then(boot)

  app.on('window-all-closed', () => app.quit())

  // Stop the backend cleanly before the app exits.
  app.on('before-quit', (event) => {
    if (backend && !quitting) {
      quitting = true
      event.preventDefault()
      void backend.stop().finally(() => {
        backend = undefined
        app.quit()
      })
    }
  })

  // Last resort: never let the Java process outlive Electron.
  process.on('exit', () => killBackendSync())
}
