import { join } from 'node:path'
import { app } from 'electron'

/**
 * Resolves where the Java runtime and the Quarkus backend live, which differs
 * between `electron .` during development and a packaged build.
 */

const javaExe = process.platform === 'win32' ? 'java.exe' : 'java'

export interface BackendPaths {
  /** Path to the `java` executable to launch. */
  javaBin: string
  /** Directory holding the exploded quarkus-app (used as the spawn cwd). */
  backendDir: string
  /** Path to `quarkus-run.jar`. */
  jar: string
}

export function resolveBackendPaths(): BackendPaths {
  if (app.isPackaged) {
    // Packaged: JRE and backend ship as electron-builder extraResources.
    const res = process.resourcesPath
    const backendDir = join(res, 'backend')
    return {
      javaBin: join(res, 'jre', 'bin', javaExe),
      backendDir,
      jar: join(backendDir, 'quarkus-run.jar'),
    }
  }

  // Development: use the repo's Maven build output and a JDK from the
  // environment (JAVA_HOME, falling back to `java` on PATH).
  const backendDir = join(app.getAppPath(), 'backend', 'target', 'quarkus-app')
  const javaHome = process.env.JAVA_HOME
  return {
    javaBin: javaHome ? join(javaHome, 'bin', javaExe) : javaExe,
    backendDir,
    jar: join(backendDir, 'quarkus-run.jar'),
  }
}
