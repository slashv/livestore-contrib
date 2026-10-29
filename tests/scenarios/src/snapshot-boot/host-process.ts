import process from 'node:process'

import { makeWsSync } from '@livestore/sync-cf/client'

import { makeAgentHost, warmUpLiveStore } from './agent-host.ts'
import type {
  AgentHostInit,
  BenchBackendConfig,
  HostCommand,
  HostEvent,
  HostMessage,
  HostReply,
  HostRequest,
} from './protocol.ts'

/**
 * Child-process entry of the snapshot boot benchmark. The first command selects the role (agent host or checkpointer);
 * later commands are routed to it. Requests are handled one at a time in arrival order, except that long-running work
 * (restarts) continues in the background and reports through events.
 */

let handler: ((command: HostCommand) => Promise<HostReply>) | undefined
let chain = Promise.resolve()

const send = (message: HostMessage) => {
  if (process.send === undefined) throw new Error('Snapshot benchmark host requires an IPC channel')
  process.send(message)
}

const emit = (event: HostEvent) => send({ event })

const makeBackend = (config: BenchBackendConfig) => {
  if (config._tag !== 'sync-cf-ws') throw new Error('Process hosts require a sync-cf WebSocket backend')
  return makeWsSync({ url: config.url })
}

// Snapshot modules load lazily: a baseline run against a core without the snapshot API never needs them.
const makeSnapshotSource = async (command: AgentHostInit) => {
  switch (command.snapshot?._tag) {
    case undefined:
      return undefined
    case 'fs': {
      const { makeFsSnapshotStore } = await import('./fs-snapshot-store.ts')
      return makeFsSnapshotStore({ rootDir: command.snapshot.dir, storeId: command.storeId }).makeSource
    }
    case 'http': {
      const { makeHttpSnapshotSource } = await import('@livestore/sync-cf/client')
      return makeHttpSnapshotSource({ url: command.snapshot.url })
    }
  }
}

const handle = async (command: HostCommand): Promise<HostReply> => {
  switch (command._tag) {
    case 'init-agents': {
      handler = makeAgentHost(command, {
        makeBackend: makeBackend(command.backend),
        makeSnapshotSource: await makeSnapshotSource(command),
        emit,
      }).handle
      await warmUpLiveStore()
      return { _tag: 'ok' }
    }
    case 'init-checkpointer': {
      const { makeFsSnapshotStore } = await import('./fs-snapshot-store.ts')
      const store = makeFsSnapshotStore({ rootDir: command.snapshotDir, storeId: command.storeId })
      const { makeCheckpointerHost } = await import('./checkpointer-host.ts')
      handler = (
        await makeCheckpointerHost(command, { makeBackend: makeBackend(command.backend), sink: store.sink, emit })
      ).handle
      return { _tag: 'ok' }
    }
    default:
      if (handler === undefined) throw new Error(`Host is not initialized (${command._tag})`)
      return handler(command)
  }
}

process.on('message', (request: HostRequest) => {
  // `head` broadcasts and `restart` requests are answered immediately so a slow boot never delays them.
  const immediate = request.command._tag === 'head' || request.command._tag === 'restart'
  const respond = async () => {
    try {
      const reply = await handle(request.command)
      send({ requestId: request.requestId, status: 'success', reply })
      if (request.command._tag === 'shutdown') setTimeout(() => process.exit(0), 50)
    } catch (cause) {
      send({
        requestId: request.requestId,
        status: 'failure',
        error: cause instanceof Error ? (cause.stack ?? cause.message) : String(cause),
      })
    }
  }
  if (immediate === true) void respond()
  else chain = chain.then(respond)
})

process.on('SIGTERM', () => process.exit(0))
