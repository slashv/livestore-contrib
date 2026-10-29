import { createHash } from 'node:crypto'

import { SyncBackend } from '@livestore/common'
import type { LiveStoreEvent } from '@livestore/common/schema'
import { Cause, Effect, Exit, Stream } from '@livestore/utils/effect'

import { todoEvents, todoSchema } from '../corpus/applications/todo.ts'

/**
 * Shared workload definitions for the snapshot boot benchmark: the agent event mix, the preloaded history, the state
 * digest used by the convergence oracle, and push instrumentation.
 *
 * The workload reuses the scenario corpus' Todo schema: appends are `v1.TodoCreated`, updates are
 * `v1.TodoTextChanged` on one of the agent's own earlier todos. Every event text is unique, so a pushed event can be
 * matched to its commit time without changing the schema.
 */
export const benchSchema = todoSchema

export interface WorkloadParams {
  /** Events each agent commits per second while writing. */
  readonly ratePerAgent: number
  /** Fraction of commits that update one of the agent's own todos instead of appending a new one. */
  readonly updateRatio: number
  /** Approximate size of each event's text payload. */
  readonly textBytes: number
}

/**
 * Unique, padded event text: `<agent>:<incarnation>:<op>@<commit epoch ms>` followed by filler up to `textBytes`. The
 * embedded commit time lets every other participant on the same machine measure commit-to-visible latency.
 */
export const eventText = (label: string, textBytes: number) => label.padEnd(Math.max(label.length, textBytes), '.')

/** Commit epoch milliseconds embedded by {@link nextAgentEvent}, or `undefined` for history events. */
export const commitTimeOf = (event: LiveStoreEvent.Global.Encoded): number | undefined => {
  const text = (event.args as { text?: unknown } | undefined)?.text
  if (typeof text !== 'string') return undefined
  const match = /@(\d+)\./.exec(text) ?? /@(\d+)$/.exec(text)
  return match === null ? undefined : Number(match[1])
}

/** Builds the next agent event from the mix. `ownTodoIds` is mutated when the event appends a todo. */
export const nextAgentEvent = (args: {
  readonly agentId: string
  readonly incarnation: number
  readonly op: number
  readonly ownTodoIds: string[]
  readonly params: WorkloadParams
  readonly random: () => number
}) => {
  const text = eventText(`${args.agentId}:${args.incarnation}:${args.op}@${Date.now()}`, args.params.textBytes)
  if (args.ownTodoIds.length > 0 && args.random() < args.params.updateRatio) {
    const id = args.ownTodoIds[Math.floor(args.random() * args.ownTodoIds.length)]!
    return { text, event: todoEvents.textChanged({ id, text }) }
  }
  const id = `${args.agentId}-${args.incarnation}-${args.op}`
  args.ownTodoIds.push(id)
  return { text, event: todoEvents.created({ id, text }) }
}

/**
 * Encoded backend history written before the agents start: appends with a 20% update mix from a fixed set of
 * pseudo-writers, so the store's state grows with its history like the target agent workload.
 */
export const makeHistoryEvents = ({
  count,
  textBytes,
  writers = 10,
}: {
  readonly count: number
  readonly textBytes: number
  readonly writers?: number
}): LiveStoreEvent.Global.Encoded[] => {
  const random = seededRandom(1)
  const created: string[] = []
  return Array.from({ length: count }, (_, index) => {
    const seqNum = index + 1
    const writer = `history-${index % writers}`
    const text = eventText(`${writer}:history:${seqNum}`, textBytes)
    const update = created.length > 0 && random() < 0.2
    const id = update === true ? created[Math.floor(random() * created.length)]! : `${writer}-h-${seqNum}`
    if (update === false) created.push(id)
    return {
      name: update === true ? todoEvents.textChanged.name : todoEvents.created.name,
      args: { id, text },
      seqNum: seqNum as LiveStoreEvent.Global.Encoded['seqNum'],
      parentSeqNum: (seqNum - 1) as LiveStoreEvent.Global.Encoded['parentSeqNum'],
      clientId: writer,
      sessionId: `${writer}-session`,
    }
  })
}

export interface TodoRow {
  readonly id: string
  readonly text: string
  readonly completed: boolean
}

/** Canonical digest of the Todo state: rows ordered by id. Equal state at equal heads must give equal digests. */
export const todoStateDigest = (rows: ReadonlyArray<TodoRow>) => {
  const hash = createHash('sha256')
  for (const row of rows.toSorted((left, right) => (left.id < right.id ? -1 : left.id > right.id ? 1 : 0))) {
    hash.update(`${row.id}\u0000${row.text}\u0000${row.completed === true ? 1 : 0}\n`)
  }
  return { digest: hash.digest('hex').slice(0, 16), rows: rows.length }
}

/** Push counters of one process, accumulated over all its clients. */
export interface PushCounters {
  attempts: number
  succeeded: number
  /** Pushes interrupted by the leader, e.g. because a pull already confirmed or rebased the batch. */
  interrupted: number
  eventsPushed: number
  failures: Record<string, number>
}

export const makePushCounters = (): PushCounters => ({
  attempts: 0,
  succeeded: 0,
  interrupted: 0,
  eventsPushed: 0,
  failures: {},
})

/**
 * Wraps a sync provider constructor to count push attempts and their outcomes by error tag (a `ServerAheadError` is a
 * push rejected because another writer got there first), and to report every pulled page.
 *
 * The wrapper is applied to the constructor, not to a pre-built client, so the leader still provides the provider's
 * KeyValueStore: snapshot boot persists the backend identity there before the provider is constructed.
 */
export const instrumentSyncBackend =
  (
    make: SyncBackend.SyncBackendConstructor<any>,
    counters: PushCounters,
    onPulled?: (events: ReadonlyArray<LiveStoreEvent.Global.Encoded>) => void,
  ): SyncBackend.SyncBackendConstructor<any> =>
  (args) =>
    Effect.map(make(args), (backend) =>
      SyncBackend.of({
        ...backend,
        pull: (cursor, options) =>
          onPulled === undefined
            ? backend.pull(cursor, options)
            : backend
                .pull(cursor, options)
                .pipe(Stream.tap((item) => Effect.sync(() => onPulled(item.batch.map((entry) => entry.eventEncoded))))),
        push: (batch) =>
          Effect.suspend(() => {
            counters.attempts += 1
            return backend.push(batch)
          }).pipe(
            Effect.onExit((exit) =>
              Effect.sync(() => {
                if (Exit.isSuccess(exit) === true) {
                  counters.succeeded += 1
                  counters.eventsPushed += batch.length
                } else if (Cause.hasInterruptsOnly(exit.cause) === true) {
                  counters.interrupted += 1
                } else {
                  const tag = failureTag(Cause.squash(exit.cause))
                  counters.failures[tag] = (counters.failures[tag] ?? 0) + 1
                }
              }),
            ),
          ),
      }),
    )

const failureTag = (failure: unknown) =>
  typeof failure === 'object' && failure !== null && '_tag' in failure && typeof failure._tag === 'string'
    ? failure._tag
    : 'Unknown'

/** Small deterministic PRNG (mulberry32) so runs with the same seed issue the same event mix. */
export const seededRandom = (seed: number) => {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296
  }
}

/** Nearest-rank percentile of an unsorted sample; `undefined` for an empty sample. */
export const percentile = (values: ReadonlyArray<number>, p: number): number | undefined => {
  if (values.length === 0) return undefined
  const sorted = values.toSorted((left, right) => left - right)
  return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1))]
}
