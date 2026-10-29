import { spawn } from 'node:child_process'
import path from 'node:path'

import { parseCoreSelection, withCoreSource } from '../core-source.ts'

/**
 * Selects the LiveStore core (`--core-path`/`--core-ref`, like `scenario:run`) and runs the snapshot boot benchmark
 * against it in a child process, so the projected core is resolved fresh.
 */
const workspaceRoot = path.resolve(import.meta.dirname, '../../../..')

const parsed = parseCoreSelection(process.argv.slice(2))
const exitCode = await withCoreSource({
  selection: parsed.selection,
  workspaceRoot,
  run: (core) =>
    new Promise<number>((resolve, reject) => {
      console.log(`LiveStore: ${core.label}`)
      console.log(`Core revision: ${core.sourceRevision}`)
      const child = spawn(
        process.execPath,
        ['--import', import.meta.resolve('tsx'), path.join(import.meta.dirname, 'bench.ts'), ...parsed.scenarioArgs],
        {
          cwd: path.resolve(import.meta.dirname, '../..'),
          env: { ...process.env, LIVESTORE_SCENARIO_SOURCE_REVISION: core.sourceRevision },
          stdio: 'inherit',
        },
      )
      for (const signal of ['SIGINT', 'SIGTERM'] as const) process.on(signal, () => child.kill(signal))
      child.once('error', reject)
      child.once('exit', (code, signal) => resolve(code ?? (signal === null ? 1 : 130)))
    }),
})
process.exitCode = exitCode
