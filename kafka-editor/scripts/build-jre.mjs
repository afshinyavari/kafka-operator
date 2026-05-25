/**
 * Builds a minimal Java runtime into `resources/jre/` for the desktop bundle,
 * using `jlink` from the JDK on PATH (or $JAVA_HOME).
 *
 * Includes the JDK's full module set deliberately — Kafka Streams + Avro use
 * reflection in ways that make a narrower `jdeps`-derived list fragile; the
 * size cost (~30 MB compressed) is worth the reliability.
 *
 * Cross-OS note: jlink can only produce a runtime for the OS it runs on, so
 * the Linux build runs on Linux and (later) the Windows build runs on Windows.
 */
import { execFileSync } from 'node:child_process'
import { existsSync, mkdirSync, rmSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = join(dirname(fileURLToPath(import.meta.url)), '..')
const outDir = join(root, 'resources', 'jre')

const exe = process.platform === 'win32' ? '.exe' : ''
const javaHome = process.env.JAVA_HOME
const jlink = javaHome ? join(javaHome, 'bin', `jlink${exe}`) : `jlink${exe}`
const javaCmd = javaHome ? join(javaHome, 'bin', `java${exe}`) : `java${exe}`

const moduleListing = execFileSync(javaCmd, ['--list-modules'], {
  encoding: 'utf-8',
})
const modules = moduleListing
  .split('\n')
  .map((line) => line.split('@')[0].trim())
  .filter(Boolean)

console.log(
  `build-jre: ${modules.length} JDK modules → ${outDir} (this takes ~30s)`,
)

if (existsSync(outDir)) rmSync(outDir, { recursive: true, force: true })
mkdirSync(dirname(outDir), { recursive: true })

execFileSync(
  jlink,
  [
    '--add-modules',
    modules.join(','),
    '--strip-debug',
    '--no-header-files',
    '--no-man-pages',
    '--compress=zip-6',
    '--output',
    outDir,
  ],
  { stdio: 'inherit' },
)

console.log(`build-jre: done → ${outDir}`)
