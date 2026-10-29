import {
  EventlogSqliteDb,
  MaterializationJournal,
  OtelLiveDummy,
  type Snapshot,
  StateHead,
  StateSqliteDb,
  type SyncBackend,
} from '@livestore/common'
import {
  type Checkpointer,
  LeaderThreadCtx,
  makeCheckpointer,
  makeLeaderThreadLayer,
  ShutdownChannel,
} from '@livestore/common/leader-thread'
import { sqliteDbFactory } from '@livestore/sqlite-wasm/browser'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import {
  Context,
  Effect,
  Exit,
  FetchHttpClient,
  Layer,
  Option,
  Scope,
  SubscriptionRef,
  WebChannel,
} from '@livestore/utils/effect'

import { sleep } from './agent-host.ts'
import type { CheckpointerHostInit, DigestEntry, HostCommand, HostEvent, HostReply } from './protocol.ts'
import { benchSchema, commitTimeOf, instrumentSyncBackend, makePushCounters, todoStateDigest } from './workload.ts'

export interface CheckpointerHostEnv {
  readonly makeBackend: SyncBackend.SyncBackendConstructor<any>
  readonly sink: Snapshot.SnapshotSink
  readonly emit: (event: HostEvent) => void
}

/**
 * Node checkpointer: an ordinary LiveStore leader (in-memory SQLite, live pull, never pushes) running the core
 * `makeCheckpointer` loop, which publishes state snapshots to the given sink.
 *
 * The leader is built directly with `makeLeaderThreadLayer` because `makeCheckpointer` needs the leader context and
 * the state database, which a Store does not expose.
 */
export const makeCheckpointerHost = async (init: CheckpointerHostInit, env: CheckpointerHostEnv) => {
  const scope = await Effect.runPromise(Scope.make())
  const run = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>) =>
    Effect.runPromise(effect.pipe(Scope.provide(scope), Effect.provide(OtelLiveDummy)))

  let followLatencies: number[] = []
  const counters = makePushCounters()
  const makeBackend = instrumentSyncBackend(env.makeBackend, counters, (events) => {
    const now = Date.now()
    for (const event of events) {
      if (event.seqNum % init.sampleEvery !== 0) continue
      const committedAt = commitTimeOf(event)
      if (committedAt !== undefined) followLatencies.push(now - committedAt)
    }
  })
  const leader = await run(bootNodeLeader({ storeId: init.storeId, clientId: 'checkpointer', makeBackend }))
  const checkpointer: Checkpointer = await run(
    makeCheckpointer({
      schema: benchSchema,
      storeId: init.storeId,
      sink: env.sink,
      trigger: init.trigger,
      pollInterval: init.pollIntervalMs,
    }).pipe(
      Effect.provideService(LeaderThreadCtx, leader.ctx),
      Effect.provideService(StateSqliteDb.StateSqliteDb, leader.dbState),
    ),
  )

  const upstreamHead = () =>
    Effect.runPromise(leader.ctx.syncProcessor.syncState.get).then((state) => state.upstreamHead.global)

  const reporter = setInterval(() => {
    void Promise.all([Effect.runPromise(SubscriptionRef.get(checkpointer.stats)), upstreamHead()]).then(
      ([stats, head]) => {
        const latest = Option.getOrUndefined(stats.latestManifest)
        const follow = followLatencies
        followLatencies = []
        const lastPublish = Option.getOrUndefined(stats.lastPublish)
        env.emit({
          _tag: 'checkpointer-report',
          at: Date.now(),
          head,
          publishedCount: stats.publishedCount,
          failedCount: stats.failedCount,
          latestSnapshotSeqNum: latest?.seqNum ?? 0,
          snapshotLag: head - (latest?.seqNum ?? 0),
          lastPublish,
          followLatenciesMs: follow,
          rssBytes: process.memoryUsage().rss,
        })
      },
    )
  }, init.reportIntervalMs)

  const handle = async (command: HostCommand): Promise<HostReply> => {
    switch (command._tag) {
      case 'start-agents': {
        // Waits until the checkpointer has applied the backend log up to the given head.
        while ((await upstreamHead()) < command.targetHead) await sleep(20)
        return { _tag: 'ok' }
      }
      case 'publish-now': {
        const start = performance.now()
        const manifest = await Effect.runPromise(checkpointer.publishNow)
        return {
          _tag: 'published',
          seqNum: Option.getOrUndefined(manifest)?.seqNum,
          totalMs: performance.now() - start,
        }
      }
      case 'digest': {
        const rows = leader.dbState
          .select<{ id: string; text: string; completed: number }>('SELECT id, text, completed FROM todos')
          .map((row) => ({ id: row.id, text: row.text, completed: row.completed === 1 }))
        const entry: DigestEntry = {
          participant: 'checkpointer',
          upstreamHead: await upstreamHead(),
          ...todoStateDigest(rows),
        }
        return { _tag: 'digests', entries: [entry] }
      }
      case 'head':
      case 'stop-writers':
        return { _tag: 'ok' }
      case 'shutdown':
        clearInterval(reporter)
        await Effect.runPromise(Scope.close(scope, Exit.void))
        return { _tag: 'ok' }
      case 'init-agents':
      case 'init-checkpointer':
      case 'restart':
        throw new Error(`Checkpointer host cannot handle ${command._tag}`)
    }
  }

  return { handle }
}

/** Boots a headless leader on fresh in-memory databases (Node wasm SQLite) with live pull and no devtools. */
export const bootNodeLeader = ({
  storeId,
  clientId,
  makeBackend,
}: {
  storeId: string
  clientId: string
  makeBackend: SyncBackend.SyncBackendConstructor<any>
}) =>
  Effect.gen(function* () {
    const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
    const makeSqliteDb = sqliteDbFactory({ sqlite3 })
    const dbState = yield* makeSqliteDb({ _tag: 'in-memory' })
    const dbEventlog = yield* makeSqliteDb({ _tag: 'in-memory' })
    const shutdownChannel = yield* WebChannel.queueChannelProxy({ schema: ShutdownChannel.All })

    const sqliteDbLayer = Layer.mergeAll(StateSqliteDb.layer(dbState), EventlogSqliteDb.layer(dbEventlog))
    const stateServicesLayer = Layer.mergeAll(StateHead.layer, MaterializationJournal.layer).pipe(
      Layer.provide(sqliteDbLayer),
    )
    const scope = yield* Effect.scope
    const context = yield* Layer.buildWithScope(
      makeLeaderThreadLayer({
        schema: benchSchema,
        storeId,
        clientId,
        syncPayloadEncoded: undefined,
        syncPayloadSchema: undefined,
        makeSqliteDb,
        syncOptions: { backend: makeBackend, livePull: true, onSyncError: 'ignore' },
        devtoolsOptions: { enabled: false },
        shutdownChannel: shutdownChannel.webChannel,
        // No client session reads from this leader, so it skips the session pull-item cache.
        headless: true,
      }).pipe(Layer.provide(Layer.mergeAll(sqliteDbLayer, stateServicesLayer)), Layer.provide(FetchHttpClient.layer)),
      scope,
    )
    return { ctx: Context.get(context, LeaderThreadCtx), dbState, dbEventlog }
  })
