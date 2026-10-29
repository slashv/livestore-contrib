import { makeInMemoryAdapter } from '@livestore/adapter-web'
import { OtelLiveDummy, type Snapshot, type SyncBackend } from '@livestore/common'
import type { LiveStoreEvent } from '@livestore/common/schema'
import { createStore, type Store } from '@livestore/livestore'
import { Effect, Exit, Option, type OtelTracer, Scope } from '@livestore/utils/effect'

import { todosTable } from '../corpus/applications/todo.ts'
import type {
  AgentHostInit,
  AgentStatus,
  DigestEntry,
  HostCommand,
  HostEvent,
  HostReply,
  RestartRecord,
} from './protocol.ts'
import {
  benchSchema,
  commitTimeOf,
  instrumentSyncBackend,
  makePushCounters,
  nextAgentEvent,
  seededRandom,
  todoStateDigest,
} from './workload.ts'

/** Platform pieces an agent host needs; the child process builds them from JSON config, the in-process run directly. */
export interface AgentHostEnv {
  readonly makeBackend: SyncBackend.SyncBackendConstructor<any>
  /** Present when agents boot from snapshots. */
  readonly makeSnapshotSource: Snapshot.SnapshotSourceConstructor | undefined
  readonly emit: (event: HostEvent) => void
}

/**
 * Hosts several agents in one Node process. Each agent is a real LiveStore Store on the in-memory adapter, so a
 * restart (closing the Store and creating a new one) is a fresh sandbox: new empty state and eventlog databases and a
 * new clientId. Agents write at a fixed rate with a mostly-append event mix.
 */
export const makeAgentHost = (init: AgentHostInit, env: AgentHostEnv) => {
  const counters = makePushCounters()
  let ackLatencies: number[] = []
  let visibleLatencies: number[] = []
  let knownHead = 0
  let stopped = false
  const random = seededRandom(init.seed + init.hostIndex * 7919)

  const agents = new Map<string, AgentRuntime>(
    init.agentIds.map((agentId) => [
      agentId,
      {
        agentId,
        incarnation: -1,
        scope: undefined,
        store: undefined,
        writing: false,
        booting: false,
        op: 0,
        committed: 0,
        rebasesBefore: 0,
        ownTodoIds: [],
        commitTimes: new Map(),
        writeCredit: 0,
      },
    ]),
  )

  /**
   * Observes every pulled page: the agent's own events coming back confirm its commits (commit-to-confirmed latency),
   * and sampled events of other agents give the commit-to-visible latency of a live agent.
   */
  const onPulled = (agent: AgentRuntime) => (events: ReadonlyArray<LiveStoreEvent.Global.Encoded>) => {
    if (agent.commitTimes.size > 0) {
      const nowMonotonic = performance.now()
      for (const event of events) {
        const text = (event.args as { text?: unknown } | undefined)?.text
        if (typeof text !== 'string') continue
        const committedAt = agent.commitTimes.get(text)
        if (committedAt === undefined) continue
        agent.commitTimes.delete(text)
        ackLatencies.push(nowMonotonic - committedAt)
      }
    }
    if (agent.booting === true) return
    const now = Date.now()
    for (const event of events) {
      if (event.seqNum % init.sampleEvery !== 0) continue
      const committedAt = commitTimeOf(event)
      if (committedAt !== undefined) visibleLatencies.push(now - committedAt)
    }
  }

  const localTargetHead = async () => {
    let head = knownHead
    for (const agent of agents.values()) {
      if (agent.store === undefined || agent.booting === true) continue
      const states = await agent.store._dev.syncStates()
      head = Math.max(head, states.leader.upstreamHead.global)
    }
    return head
  }

  /** Boots a fresh sandbox for the agent and waits until its session reached `targetHead`. */
  const boot = async (
    agent: AgentRuntime,
    targetHead: number,
    killed: { readonly killMs: number; readonly unconfirmed: number },
  ): Promise<RestartRecord> => {
    const { killMs, unconfirmed: unconfirmedAtKill } = killed
    agent.incarnation += 1
    agent.booting = true
    agent.commitTimes.clear()
    const incarnation = agent.incarnation
    const snapshotRecord: SnapshotProbe = {
      manifestSeqNum: undefined,
      byteLength: 0,
      manifestMs: 0,
      downloadMs: undefined,
    }
    const start = performance.now()
    const scope = await Effect.runPromise(Scope.make())
    agent.scope = scope
    try {
      const store = await runIn(
        scope,
        createStore({
          schema: benchSchema,
          storeId: init.storeId,
          disableDevtools: true,
          adapter: makeInMemoryAdapter({
            clientId: `${agent.agentId}-i${incarnation}`,
            sessionId: 'main',
            sync: {
              backend: instrumentSyncBackend(env.makeBackend, counters, onPulled(agent)),
              onSyncError: 'ignore',
              ...(env.makeSnapshotSource === undefined
                ? {}
                : {
                    snapshot: {
                      source: probeSnapshotSource(env.makeSnapshotSource, snapshotRecord),
                      timeout: init.snapshotTimeoutMs,
                    },
                  }),
            },
          }),
        }),
      )
      const bootMs = performance.now() - start
      agent.store = store
      const bootHead = (await store._dev.syncStates()).leader.upstreamHead.global

      // Time-to-head: until the backend-confirmed head (leader upstream) reached the global head that was current when
      // the restart began, and the session has applied it. The session's own "upstream" is the leader, so its upstream
      // head alone also counts events the leader has not pushed yet.
      const deadline = start + init.headTimeoutMs
      while (true) {
        const states = await store._dev.syncStates()
        if (states.leader.upstreamHead.global >= targetHead && states.session.upstreamHead.global >= targetHead) break
        if (performance.now() > deadline) {
          throw new Error(
            `did not reach head ${targetHead} within ${init.headTimeoutMs}ms: ${describeSyncStates(states)}`,
          )
        }
        await sleep(5)
      }
      const timeToHeadMs = performance.now() - start
      agent.booting = false
      agent.writing = stopped === false
      return {
        agentId: agent.agentId,
        incarnation,
        targetHead,
        killMs,
        unconfirmedAtKill,
        bootMs,
        timeToHeadMs,
        bootHead,
        snapshot:
          snapshotRecord.manifestSeqNum === undefined
            ? undefined
            : {
                manifestSeqNum: snapshotRecord.manifestSeqNum,
                byteLength: snapshotRecord.byteLength,
                manifestMs: snapshotRecord.manifestMs,
                downloadMs: snapshotRecord.downloadMs,
                installed: bootHead >= snapshotRecord.manifestSeqNum && snapshotRecord.downloadMs !== undefined,
              },
        error: undefined,
      }
    } catch (cause) {
      agent.booting = false
      return {
        agentId: agent.agentId,
        incarnation,
        targetHead,
        killMs,
        unconfirmedAtKill,
        bootMs: performance.now() - start,
        timeToHeadMs: Number.NaN,
        bootHead: 0,
        snapshot: undefined,
        error: cause instanceof Error ? (cause.stack ?? cause.message) : String(cause),
      }
    }
  }

  /** Closes the agent's Store. An abrupt kill loses unpushed events with the sandbox, as a real kill would. */
  const kill = async (agent: AgentRuntime) => {
    agent.writing = false
    const start = performance.now()
    const store = agent.store
    let unconfirmed = 0
    if (store !== undefined) {
      const states = await store._dev.syncStates().catch(() => undefined)
      agent.rebasesBefore += states?.leader.localHead.rebaseGeneration ?? 0
      unconfirmed = (states?.leader.pending.length ?? 0) + (states?.session.pending.length ?? 0)
    }
    agent.store = undefined
    const scope = agent.scope
    agent.scope = undefined
    if (scope !== undefined) {
      // An abrupt kill closes with a non-success exit, which skips the Store's orderly drain, so pending events die with
      // the sandbox. A graceful kill drains pending events to the backend first.
      await withTimeout(
        Effect.runPromise(Scope.close(scope, init.killMode === 'graceful' ? Exit.void : Exit.interrupt())),
        init.killMode === 'graceful' ? 40_000 : 10_000,
      ).catch((cause: unknown) => env.emit({ _tag: 'log', message: `kill ${agent.agentId}: ${String(cause)}` }))
    }
    return { killMs: performance.now() - start, unconfirmed }
  }

  const statusOf = async (agent: AgentRuntime): Promise<AgentStatus> => {
    const states = agent.store === undefined ? undefined : await agent.store._dev.syncStates().catch(() => undefined)
    return {
      agentId: agent.agentId,
      incarnation: agent.incarnation,
      booting: agent.booting === true || agent.store === undefined,
      upstreamHead: states?.leader.upstreamHead.global ?? 0,
      localHead: states?.leader.localHead.global ?? 0,
      pending: states?.leader.pending.length ?? 0,
      committed: agent.committed,
      rebases: agent.rebasesBefore + (states?.leader.localHead.rebaseGeneration ?? 0),
    }
  }

  // One timer drives every agent of the host. Credit accumulates with elapsed wall time, so a host that was briefly
  // blocked catches up, bounded to one second of events to avoid unrealistic bursts.
  let lastTick = performance.now()
  const writer = setInterval(() => {
    const now = performance.now()
    const elapsedSeconds = (now - lastTick) / 1000
    lastTick = now
    for (const agent of agents.values()) {
      if (agent.writing === false || agent.store === undefined) continue
      agent.writeCredit = Math.min(
        agent.writeCredit + elapsedSeconds * init.workload.ratePerAgent,
        init.workload.ratePerAgent,
      )
      while (agent.writeCredit >= 1) {
        agent.writeCredit -= 1
        const { text, event } = nextAgentEvent({
          agentId: agent.agentId,
          incarnation: agent.incarnation,
          op: agent.op++,
          ownTodoIds: agent.ownTodoIds,
          params: init.workload,
          random,
        })
        try {
          agent.commitTimes.set(text, performance.now())
          agent.store.commit(event)
          agent.committed += 1
        } catch {
          agent.commitTimes.delete(text)
        }
      }
    }
  }, 20)

  const reporter = setInterval(() => {
    void Promise.all([...agents.values()].map(statusOf)).then((statuses) => {
      const latencies = ackLatencies
      const visible = visibleLatencies
      ackLatencies = []
      visibleLatencies = []
      env.emit({
        _tag: 'report',
        hostIndex: init.hostIndex,
        at: Date.now(),
        agents: statuses,
        push: { ...counters, failures: { ...counters.failures } },
        ackLatenciesMs: latencies,
        visibleLatenciesMs: visible,
        rssBytes: process.memoryUsage().rss,
      })
    })
  }, init.reportIntervalMs)

  const handle = async (command: HostCommand): Promise<HostReply> => {
    switch (command._tag) {
      case 'start-agents': {
        knownHead = Math.max(knownHead, command.targetHead)
        const records = await Promise.all(
          [...agents.values()].map((agent) => boot(agent, command.targetHead, { killMs: 0, unconfirmed: 0 })),
        )
        return { _tag: 'booted', records }
      }
      case 'head':
        knownHead = Math.max(knownHead, command.head)
        return { _tag: 'ok' }
      case 'restart': {
        const agent = agents.get(command.agentId)
        if (agent === undefined) throw new Error(`Unknown agent ${command.agentId}`)
        if (agent.booting === true) return { _tag: 'ok' }
        agent.booting = true
        void (async () => {
          const killed = await kill(agent)
          await sleep(command.downtimeMs)
          const targetHead = await localTargetHead()
          const record = await boot(agent, targetHead, killed)
          env.emit({ _tag: 'restart-done', hostIndex: init.hostIndex, record })
        })()
        return { _tag: 'ok' }
      }
      case 'stop-writers':
        stopped = true
        for (const agent of agents.values()) agent.writing = false
        return { _tag: 'ok' }
      case 'digest': {
        const entries: DigestEntry[] = []
        for (const agent of agents.values()) {
          if (agent.store === undefined) continue
          const status = await statusOf(agent)
          entries.push({
            participant: `${agent.agentId}#${agent.incarnation}`,
            upstreamHead: status.upstreamHead,
            ...todoStateDigest(agent.store.query(todosTable)),
          })
        }
        return { _tag: 'digests', entries }
      }
      case 'shutdown':
        clearInterval(writer)
        clearInterval(reporter)
        await Promise.all([...agents.values()].map(kill))
        return { _tag: 'ok' }
      case 'init-agents':
      case 'init-checkpointer':
      case 'publish-now':
        throw new Error(`Agent host cannot handle ${command._tag}`)
    }
  }

  /** Raw leader and session sync states of every agent, for investigations. */
  const debugSyncStates = () =>
    Promise.all(
      [...agents.values()].map(async (agent) => ({
        agentId: agent.agentId,
        incarnation: agent.incarnation,
        states: agent.store === undefined ? undefined : await agent.store._dev.syncStates(),
      })),
    )

  return { handle, debugSyncStates }
}

/**
 * Boots and closes one offline Store so wasm compilation and module loading happen before any measured boot. Without
 * it the first boot of every host process includes several hundred milliseconds of process warm-up.
 */
export const warmUpLiveStore = async () => {
  const scope = await Effect.runPromise(Scope.make())
  await runIn(
    scope,
    createStore({
      schema: benchSchema,
      storeId: `warmup-${process.pid}`,
      disableDevtools: true,
      adapter: makeInMemoryAdapter({ clientId: 'warmup', sessionId: 'warmup' }),
    }),
  )
  await Effect.runPromise(Scope.close(scope, Exit.void))
}

interface AgentRuntime {
  readonly agentId: string
  incarnation: number
  scope: Scope.Closeable | undefined
  store: Store<typeof benchSchema> | undefined
  writing: boolean
  booting: boolean
  op: number
  committed: number
  /** Rebase generations of earlier incarnations; each incarnation's leader starts again at 0. */
  rebasesBefore: number
  readonly ownTodoIds: string[]
  /** Commit time per unique event text, until the event comes back confirmed in a pull. */
  readonly commitTimes: Map<string, number>
  writeCredit: number
}

interface SnapshotProbe {
  manifestSeqNum: number | undefined
  byteLength: number
  manifestMs: number
  downloadMs: number | undefined
}

/** Wraps a snapshot source to record what the booting client fetched and how long it took. */
const probeSnapshotSource =
  (make: Snapshot.SnapshotSourceConstructor, probe: SnapshotProbe): Snapshot.SnapshotSourceConstructor =>
  (args) =>
    Effect.map(make(args), (source) => ({
      getManifest: (request) =>
        Effect.suspend(() => {
          const start = performance.now()
          return source.getManifest(request).pipe(
            Effect.tap((manifest) =>
              Effect.sync(() => {
                probe.manifestMs = performance.now() - start
                if (Option.isSome(manifest) === true) {
                  probe.manifestSeqNum = manifest.value.seqNum
                  probe.byteLength = manifest.value.byteLength
                }
              }),
            ),
          )
        }),
      download: (manifest) =>
        Effect.suspend(() => {
          const start = performance.now()
          return source.download(manifest).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                probe.downloadMs = performance.now() - start
              }),
            ),
          )
        }),
    }))

const runIn = <A, E>(scope: Scope.Scope, effect: Effect.Effect<A, E, Scope.Scope | OtelTracer.OtelTracer>) =>
  Effect.runPromise(effect.pipe(Scope.provide(scope), Effect.provide(OtelLiveDummy)))

/** Compact description of a Store's leader and session sync states for error messages. */
export const describeSyncStates = (states: {
  readonly leader: {
    readonly localHead: SeqNum
    readonly upstreamHead: SeqNum
    readonly pending: ReadonlyArray<unknown>
  }
  readonly session: {
    readonly localHead: SeqNum
    readonly upstreamHead: SeqNum
    readonly pending: ReadonlyArray<unknown>
  }
}) =>
  `leader local ${seq(states.leader.localHead)} upstream ${seq(states.leader.upstreamHead)} pending ${states.leader.pending.length}; ` +
  `session local ${seq(states.session.localHead)} upstream ${seq(states.session.upstreamHead)} pending ${states.session.pending.length}`

type SeqNum = { readonly global: number; readonly client: number; readonly rebaseGeneration: number }

const seq = (value: SeqNum) => `e${value.global}.${value.client}r${value.rebaseGeneration}`

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

const withTimeout = <A>(promise: Promise<A>, ms: number) =>
  Promise.race([
    promise,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`Timed out after ${ms}ms`)), ms)),
  ])
