import type { WorkloadParams } from './workload.ts'

/**
 * Messages between the snapshot boot benchmark controller and its participant hosts. A host is either an agent host
 * (several agent Stores in one Node process) or the checkpointer host. The same handlers run in-process (direct calls)
 * or in a child process (IPC), so the protocol only carries JSON.
 */

export type BenchBackendConfig = { readonly _tag: 'sync-cf-ws'; readonly url: string } | { readonly _tag: 'in-process' }

/**
 * `fs`: the Node realization (file-system store shared with the Node checkpointer).
 * `http`: the Cloudflare realization (`makeHttpSnapshotSource` against the Worker's snapshot routes).
 */
export type SnapshotSourceConfig =
  | { readonly _tag: 'fs'; readonly dir: string }
  | { readonly _tag: 'http'; readonly url: string }

export interface AgentHostInit {
  readonly _tag: 'init-agents'
  readonly hostIndex: number
  readonly storeId: string
  readonly backend: BenchBackendConfig
  /** Where agents read snapshots from; absent when they boot without snapshots. */
  readonly snapshot: SnapshotSourceConfig | undefined
  readonly snapshotTimeoutMs: number
  /** A boot that has not reached its target head after this long is recorded as failed. */
  readonly headTimeoutMs: number
  /** `abrupt` closes the Store without draining pending events (a killed sandbox); `graceful` drains them first. */
  readonly killMode: 'abrupt' | 'graceful'
  readonly agentIds: ReadonlyArray<string>
  readonly workload: WorkloadParams
  readonly seed: number
  readonly reportIntervalMs: number
  /** Only events whose global sequence number is a multiple of this are sampled for commit-to-visible latency. */
  readonly sampleEvery: number
}

export interface CheckpointerHostInit {
  readonly _tag: 'init-checkpointer'
  readonly storeId: string
  readonly backend: BenchBackendConfig
  readonly snapshotDir: string
  readonly trigger: { readonly minIntervalMs?: number; readonly minTailEvents?: number; readonly alpha?: number }
  readonly pollIntervalMs: number
  readonly reportIntervalMs: number
  readonly sampleEvery: number
}

export type HostCommand =
  | AgentHostInit
  | CheckpointerHostInit
  /** Boots every agent once (incarnation 0) and starts writing. */
  | { readonly _tag: 'start-agents'; readonly targetHead: number }
  /** Latest global head known to the controller; restarted agents measure time-to-head against it. */
  | { readonly _tag: 'head'; readonly head: number }
  /** Kills the agent's Store and boots a fresh sandbox (new empty in-memory databases, new clientId). */
  | { readonly _tag: 'restart'; readonly agentId: string; readonly downtimeMs: number }
  | { readonly _tag: 'stop-writers' }
  | { readonly _tag: 'digest' }
  | { readonly _tag: 'publish-now' }
  | { readonly _tag: 'shutdown' }

export interface AgentStatus {
  readonly agentId: string
  readonly incarnation: number
  readonly booting: boolean
  /** Backend-confirmed head of the agent's leader. */
  readonly upstreamHead: number
  readonly localHead: number
  readonly pending: number
  readonly committed: number
  readonly rebases: number
}

export interface RestartRecord {
  readonly agentId: string
  readonly incarnation: number
  /** Global head known when the restart began; time-to-head is measured until the client reached it. */
  readonly targetHead: number
  readonly killMs: number
  /** Events committed by the killed incarnation that the backend had not confirmed: lost with the sandbox. */
  readonly unconfirmedAtKill: number
  /** `createStore` duration, including snapshot fetch and install when used. */
  readonly bootMs: number
  /** From the start of the fresh boot until the session's upstream head reached `targetHead`. */
  readonly timeToHeadMs: number
  /** Upstream head right after boot: the snapshot head when a snapshot was installed, otherwise usually 0. */
  readonly bootHead: number
  readonly snapshot:
    | {
        readonly manifestSeqNum: number
        readonly byteLength: number
        readonly manifestMs: number
        readonly downloadMs: number | undefined
        readonly installed: boolean
      }
    | undefined
  readonly error: string | undefined
}

export interface HostReport {
  readonly _tag: 'report'
  readonly hostIndex: number
  readonly at: number
  readonly agents: ReadonlyArray<AgentStatus>
  readonly push: {
    readonly attempts: number
    readonly succeeded: number
    readonly interrupted: number
    readonly eventsPushed: number
    readonly failures: Readonly<Record<string, number>>
  }
  /** Commit-to-confirmed latencies (ms): until the agent pulled its own event back from the backend. */
  readonly ackLatenciesMs: ReadonlyArray<number>
  /**
   * Commit-to-visible latencies (ms): from another agent's commit until a live (not booting) agent of this host pulled
   * the event. Sampled every `sampleEvery` sequence numbers.
   */
  readonly visibleLatenciesMs: ReadonlyArray<number>
  readonly rssBytes: number
}

export interface CheckpointerReport {
  readonly _tag: 'checkpointer-report'
  readonly at: number
  readonly head: number
  readonly publishedCount: number
  readonly failedCount: number
  readonly latestSnapshotSeqNum: number
  readonly snapshotLag: number
  readonly lastPublish:
    | {
        readonly seqNum: number
        readonly byteLength: number
        readonly exportMs: number
        readonly hashMs: number
        readonly uploadMs: number
        readonly totalMs: number
        readonly tailEvents: number
      }
    | undefined
  /** Commit-to-applied latencies (ms) of sampled agent events at the checkpointer since the previous report. */
  readonly followLatenciesMs: ReadonlyArray<number>
  readonly rssBytes: number
}

export type HostEvent =
  | HostReport
  | CheckpointerReport
  | { readonly _tag: 'restart-done'; readonly hostIndex: number; readonly record: RestartRecord }
  | { readonly _tag: 'log'; readonly message: string }

export interface DigestEntry {
  readonly participant: string
  readonly upstreamHead: number
  readonly digest: string
  readonly rows: number
}

export type HostReply =
  | { readonly _tag: 'ok' }
  | { readonly _tag: 'booted'; readonly records: ReadonlyArray<RestartRecord> }
  | { readonly _tag: 'digests'; readonly entries: ReadonlyArray<DigestEntry> }
  | { readonly _tag: 'published'; readonly seqNum: number | undefined; readonly totalMs: number | undefined }

export type HostRequest = { readonly requestId: number; readonly command: HostCommand }

export type HostMessage =
  | { readonly requestId: number; readonly status: 'success'; readonly reply: HostReply }
  | { readonly requestId: number; readonly status: 'failure'; readonly error: string }
  | { readonly event: HostEvent }
