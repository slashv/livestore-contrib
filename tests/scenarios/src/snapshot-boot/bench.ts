import { fork, type ChildProcess } from 'node:child_process'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { makeWsSync } from '@livestore/sync-cf/client'
import { WranglerDevServer } from '@livestore/utils-dev/wrangler'
import { Effect, FetchHttpClient, KeyValueStore, Layer, Option, Schedule, Stream } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { sleep } from './agent-host.ts'
import type {
  CheckpointerReport,
  DigestEntry,
  HostCommand,
  HostEvent,
  HostMessage,
  HostReply,
  HostReport,
  RestartRecord,
  SnapshotSourceConfig,
} from './protocol.ts'
import { makeHistoryEvents, percentile } from './workload.ts'

/**
 * Snapshot boot multi-agent benchmark controller.
 *
 * Starts the scenario runner's local sync-cf Worker, preloads a backend history, optionally starts a Node
 * checkpointer, then runs N agents spread over K host processes. Agents write at a fixed rate and are periodically
 * killed and restarted as fresh sandboxes. The same workload runs once without snapshots (baseline) and once with
 * snapshot boot, and the run ends with a state convergence oracle against full-replay and snapshot-boot reference
 * clients. See `tests/scenarios/SNAPSHOT_BOOT.md`.
 */

type Mode = 'baseline' | 'snapshot'

interface BenchOptions {
  readonly agents: number
  readonly hosts: number
  readonly ratePerAgent: number
  readonly updateRatio: number
  readonly textBytes: number
  readonly history: number
  readonly durationMs: number
  readonly restartEveryMs: number
  readonly downtimeMs: number
  readonly modes: ReadonlyArray<Mode>
  readonly minIntervalMs: number
  readonly minTailEvents: number
  readonly seed: number
  readonly snapshotTimeoutMs: number
  readonly headTimeoutMs: number
  readonly killMode: 'abrupt' | 'graceful'
  /** `node`: Node checkpointer + file-system store. `cf`: checkpointer Durable Object + R2 + Worker snapshot routes. */
  readonly realization: 'node' | 'cf'
  readonly settleTimeoutMs: number
  readonly outDir: string
  readonly label: string
}

const main = async (options: BenchOptions, url: string) => {
  const loadAverageAtStart = os.loadavg()
  const runId = `${new Date().toISOString().replaceAll(':', '-').replace(/\..+/, '')}-${options.label}`
  const results: ModeResult[] = []
  for (const mode of options.modes) {
    console.log(`\n=== ${options.label} · ${mode} ===`)
    results.push(await runMode({ options, url, mode, runDir: path.join(options.outDir, runId, mode) }))
  }
  const output = {
    runId,
    options,
    environment: {
      node: process.version,
      platform: `${os.type()} ${os.release()} ${os.arch()}`,
      cpu: os.cpus()[0]?.model,
      cpus: os.cpus().length,
      memoryGiB: Math.round(os.totalmem() / 2 ** 30),
      // The benchmark shares the machine; the 1/5/15-minute load averages show how contended it was.
      loadAverageAtStart,
      loadAverageAtEnd: os.loadavg(),
      coreRevision: process.env.LIVESTORE_SCENARIO_SOURCE_REVISION ?? 'unknown',
    },
    results,
  }
  await fs.mkdir(path.join(options.outDir, runId), { recursive: true })
  const outputPath = path.join(options.outDir, runId, 'result.json')
  await fs.writeFile(outputPath, `${JSON.stringify(output, null, 2)}\n`)
  printSummary(results)
  console.log(
    `load average start ${loadAverageAtStart.map((v) => v.toFixed(0)).join('/')} end ${os
      .loadavg()
      .map((v) => v.toFixed(0))
      .join('/')}`,
  )
  console.log(`\nResult: ${outputPath}`)
  return results.every((result) => result.oracle.passed === true) ? 0 : 1
}

interface ModeResult {
  readonly mode: Mode
  readonly storeId: string
  readonly preload: { readonly events: number; readonly ms: number }
  readonly checkpointer:
    | {
        readonly catchUpMs: number
        readonly initialPublishMs: number | undefined
        readonly publishes: number
        readonly failures: number
        readonly snapshotBytes: number | undefined
        readonly exportMs: Stats
        readonly publishTotalMs: Stats
        readonly tailEventsPerPublish: Stats
        readonly snapshotLagEvents: Stats
        readonly followLatencyMs: Stats
        readonly rssMiB: number | undefined
      }
    | undefined
  readonly initialBoot: BootSummary
  readonly restarts: BootSummary
  readonly steady: {
    readonly seconds: number
    readonly committedPerSecond: number
    readonly confirmedPerSecond: number
    readonly headStart: number
    readonly headEnd: number
    readonly ackLatencyMs: Stats
    readonly visibleLatencyMs: Stats
    readonly push: {
      readonly attempts: number
      readonly succeeded: number
      readonly interrupted: number
      readonly eventsPushed: number
      readonly failures: Readonly<Record<string, number>>
      readonly rejectedRatio: number
    }
    readonly rebases: number
    readonly maxHostRssMiB: number
  }
  readonly reference: {
    readonly fullReplay: RestartRecord | undefined
    readonly snapshotBoot: RestartRecord | undefined
  }
  readonly oracle: {
    readonly passed: boolean
    readonly backendLog: { readonly head: number; readonly count: number; readonly contiguous: boolean }
    readonly maxHeadSeenByClients: number
    readonly head: number
    readonly digests: ReadonlyArray<DigestEntry>
    readonly message: string
  }
}

interface Stats {
  readonly n: number
  readonly p50: number | undefined
  readonly p95: number | undefined
  readonly p99: number | undefined
  readonly max: number | undefined
}

interface BootSummary {
  readonly count: number
  readonly errors: number
  readonly snapshotInstalled: number
  readonly timeToHeadMs: Stats
  readonly bootMs: Stats
  readonly downloadMs: Stats
  readonly snapshotBytes: Stats
  /** Events applied after boot: target head minus the head the boot started from. */
  readonly tailEvents: Stats
  /** Unconfirmed events lost per kill (abrupt kills only). */
  readonly lostOnKill: Stats
}

const runMode = async ({
  options,
  url,
  mode,
  runDir,
}: {
  options: BenchOptions
  url: string
  mode: Mode
  runDir: string
}): Promise<ModeResult> => {
  const storeId = `snapbench-${mode}-${Date.now()}`
  const snapshotDir = path.join(runDir, 'snapshots')
  const snapshotSource: SnapshotSourceConfig =
    options.realization === 'cf' ? { _tag: 'http', url } : { _tag: 'fs', dir: snapshotDir }
  await fs.mkdir(runDir, { recursive: true })
  const totalRate = options.agents * options.ratePerAgent
  const sampleEvery = Math.max(1, Math.ceil(totalRate / 500))
  const backend = { _tag: 'sync-cf-ws', url } as const

  // 1. History
  const preloadStart = performance.now()
  await pushHistory({ url, storeId, count: options.history, textBytes: options.textBytes })
  const preload = { events: options.history, ms: performance.now() - preloadStart }
  console.log(`preloaded ${options.history} events in ${(preload.ms / 1000).toFixed(1)}s`)

  const hostReports = new Map<number, HostReport>()
  const checkpointerReports: CheckpointerReport[] = []
  const restartRecords: RestartRecord[] = []
  let restartsInFlight = 0
  let steadyWindow: { start: number; end: number | undefined } | undefined
  const ackLatencies: number[] = []
  const visibleLatencies: number[] = []
  let globalHead = options.history

  const onEvent = (event: HostEvent) => {
    switch (event._tag) {
      case 'report':
        hostReports.set(event.hostIndex, event)
        for (const agent of event.agents) globalHead = Math.max(globalHead, agent.upstreamHead)
        if (steadyWindow !== undefined && steadyWindow.end === undefined) {
          ackLatencies.push(...event.ackLatenciesMs)
          visibleLatencies.push(...event.visibleLatenciesMs)
        }
        return
      case 'checkpointer-report':
        checkpointerReports.push(event)
        globalHead = Math.max(globalHead, event.head)
        return
      case 'restart-done':
        restartsInFlight -= 1
        restartRecords.push(event.record)
        if (event.record.error !== undefined)
          console.log(`restart error ${event.record.agentId}: ${event.record.error}`)
        return
      case 'log':
        console.log(`[host] ${event.message}`)
        return
    }
  }

  const hosts: HostProcess[] = []
  try {
    // 2. Checkpointer
    let checkpointer: HostProcess | undefined
    let catchUpMs = 0
    let initialPublishMs: number | undefined
    let cfPoller: NodeJS.Timeout | undefined
    if (mode === 'snapshot' && options.realization === 'cf') {
      // The Cloudflare checkpointer Durable Object was woken by the history pushes; wait until it caught up.
      const start = performance.now()
      while (true) {
        const stats = await fetchCheckpointerStats(url, storeId).catch(() => undefined)
        if (stats !== undefined && stats.head >= options.history) break
        if (performance.now() - start > options.headTimeoutMs)
          throw new Error('Cloudflare checkpointer did not catch up')
        await sleep(500)
      }
      catchUpMs = performance.now() - start
      const publishStart = performance.now()
      await fetch(`${url}/__bench/checkpointer/publish-now?storeId=${encodeURIComponent(storeId)}`)
      initialPublishMs = performance.now() - publishStart
      console.log(
        `cf checkpointer caught up in ${(catchUpMs / 1000).toFixed(1)}s, initial snapshot in ${initialPublishMs.toFixed(0)}ms`,
      )
      cfPoller = setInterval(() => {
        void fetchCheckpointerStats(url, storeId)
          .then((stats) =>
            onEvent({
              _tag: 'checkpointer-report',
              at: Date.now(),
              head: stats.head,
              publishedCount: stats.publishedCount,
              failedCount: stats.failedCount,
              latestSnapshotSeqNum: stats.latestManifest?.seqNum ?? 0,
              snapshotLag: stats.snapshotLag,
              lastPublish: stats.lastPublish ?? undefined,
              followLatenciesMs: [],
              rssBytes: 0,
            }),
          )
          .catch(() => {})
      }, 1000)
    } else if (mode === 'snapshot') {
      checkpointer = await spawnHost(onEvent, 'checkpointer')
      hosts.push(checkpointer)
      const start = performance.now()
      await checkpointer.request({
        _tag: 'init-checkpointer',
        storeId,
        backend,
        snapshotDir,
        trigger: { minIntervalMs: options.minIntervalMs, minTailEvents: options.minTailEvents },
        pollIntervalMs: 1000,
        reportIntervalMs: 1000,
        sampleEvery,
      })
      await checkpointer.request({ _tag: 'start-agents', targetHead: options.history })
      catchUpMs = performance.now() - start
      const published = await checkpointer.request({ _tag: 'publish-now' })
      initialPublishMs = published._tag === 'published' ? published.totalMs : undefined
      console.log(
        `checkpointer caught up in ${(catchUpMs / 1000).toFixed(1)}s, initial snapshot in ${initialPublishMs?.toFixed(0)}ms`,
      )
    }

    // 3. Agent hosts
    const agentHosts: HostProcess[] = []
    const hostCount = Math.min(options.hosts, options.agents)
    for (let hostIndex = 0; hostIndex < hostCount; hostIndex += 1) {
      const host = await spawnHost(onEvent, `agents-${hostIndex}`)
      hosts.push(host)
      agentHosts.push(host)
      const agentIds = Array.from({ length: options.agents }, (_, index) => index)
        .filter((index) => index % hostCount === hostIndex)
        .map((index) => `agent-${String(index).padStart(3, '0')}`)
      await host.request({
        _tag: 'init-agents',
        hostIndex,
        storeId,
        backend,
        snapshot: mode === 'snapshot' ? snapshotSource : undefined,
        snapshotTimeoutMs: options.snapshotTimeoutMs,
        headTimeoutMs: options.headTimeoutMs,
        killMode: options.killMode,
        agentIds,
        workload: {
          ratePerAgent: options.ratePerAgent,
          updateRatio: options.updateRatio,
          textBytes: options.textBytes,
        },
        seed: options.seed,
        reportIntervalMs: 1000,
        sampleEvery,
      })
      host.agentIds = agentIds
    }

    const broadcaster = setInterval(() => {
      for (const host of agentHosts) void host.request({ _tag: 'head', head: globalHead }).catch(() => {})
    }, 250)

    // 4. Initial cold start of every agent at the same time
    const initialStart = performance.now()
    const initialRecords = (
      await Promise.all(agentHosts.map((host) => host.request({ _tag: 'start-agents', targetHead: options.history })))
    ).flatMap((reply) => (reply._tag === 'booted' ? reply.records : []))
    console.log(
      `initial boot of ${options.agents} agents: ${((performance.now() - initialStart) / 1000).toFixed(1)}s ` +
        `(time-to-head p50 ${fmt(
          percentile(
            initialRecords.map((r) => r.timeToHeadMs),
            50,
          ),
        )}ms)`,
    )

    // 5. Steady phase with staggered restarts
    await sleep(2000)
    steadyWindow = { start: performance.now(), end: undefined }
    const headStart = globalHead
    const committedStart = sumCommitted(hostReports)
    const allAgents = agentHosts.flatMap((host) => host.agentIds.map((agentId) => ({ host, agentId })))
    const restartTimers: NodeJS.Timeout[] = []
    if (options.restartEveryMs > 0) {
      allAgents.forEach(({ host, agentId }, index) => {
        const offset = ((index + 0.5) / allAgents.length) * options.restartEveryMs
        const schedule = (delay: number) =>
          restartTimers.push(
            setTimeout(() => {
              if (steadyWindow?.end !== undefined) return
              restartsInFlight += 1
              void host.request({ _tag: 'restart', agentId, downtimeMs: options.downtimeMs })
              schedule(options.restartEveryMs)
            }, delay),
          )
        schedule(offset)
      })
    }
    const progress = setInterval(() => {
      const elapsed = (performance.now() - steadyWindow!.start) / 1000
      console.log(
        `t=${elapsed.toFixed(0)}s head=${globalHead} restarts=${restartRecords.length} in-flight=${restartsInFlight}` +
          (checkpointerReports.length > 0
            ? ` snapshots=${checkpointerReports.at(-1)!.publishedCount} cp-lag=${checkpointerReports.at(-1)!.snapshotLag}`
            : ''),
      )
    }, 10_000)
    await sleep(options.durationMs)
    for (const timer of restartTimers) clearTimeout(timer)
    clearInterval(progress)
    await Promise.all(hosts.map((host) => host.request({ _tag: 'stop-writers' })))
    steadyWindow.end = performance.now()
    const seconds = (steadyWindow.end - steadyWindow.start) / 1000
    const committedEnd = sumCommitted(hostReports)
    const headEnd = globalHead

    // 6. Settle: restarts finished, nothing pending, everyone at the same head
    const settleStart = performance.now()
    while (true) {
      await sleep(1000)
      const statuses = [...hostReports.values()].flatMap((report) => report.agents)
      const heads = new Set(statuses.map((status) => status.upstreamHead))
      const settled =
        restartsInFlight <= 0 &&
        statuses.every((status) => status.booting === false && status.pending === 0) &&
        heads.size === 1 &&
        (mode === 'baseline' || checkpointerReports.at(-1)?.head === [...heads][0])
      if (settled === true) break
      if (performance.now() - settleStart > options.settleTimeoutMs) {
        console.log(`settle timed out: heads=${[...heads].join(',')} in-flight=${restartsInFlight}`)
        for (const status of statuses) console.log(`  ${JSON.stringify(status)}`)
        console.log(`  checkpointer head ${checkpointerReports.at(-1)?.head}`)
        break
      }
    }
    clearInterval(broadcaster)
    if (cfPoller !== undefined) clearInterval(cfPoller)

    // 7. Reference clients on a quiet system: a full replay and (snapshot mode) a snapshot boot
    const referenceRecords: Record<'fullReplay' | 'snapshotBoot', RestartRecord | undefined> = {
      fullReplay: undefined,
      snapshotBoot: undefined,
    }
    for (const kind of mode === 'snapshot' ? (['fullReplay', 'snapshotBoot'] as const) : (['fullReplay'] as const)) {
      const host = await spawnHost(onEvent, `reference-${kind}`)
      hosts.push(host)
      await host.request({
        _tag: 'init-agents',
        hostIndex: 1000,
        storeId,
        backend,
        snapshot: kind === 'snapshotBoot' ? snapshotSource : undefined,
        snapshotTimeoutMs: options.snapshotTimeoutMs,
        headTimeoutMs: options.headTimeoutMs,
        killMode: options.killMode,
        agentIds: [`reference-${kind}`],
        workload: { ratePerAgent: 0, updateRatio: 0, textBytes: options.textBytes },
        seed: options.seed,
        reportIntervalMs: 60_000,
        sampleEvery,
      })
      await host.request({ _tag: 'stop-writers' })
      const reply = await host.request({ _tag: 'start-agents', targetHead: globalHead })
      referenceRecords[kind] = reply._tag === 'booted' ? reply.records[0] : undefined
    }

    // 8. Oracle: every live agent, the checkpointer and the reference clients hold the same state at the same head
    const digests = (
      await Promise.all(hosts.map((host) => host.request({ _tag: 'digest' }).catch(() => undefined)))
    ).flatMap((reply) => (reply?._tag === 'digests' ? reply.entries : []))
    const distinct = new Set(digests.map((entry) => `${entry.upstreamHead}/${entry.digest}`))
    const backendLog = await inspectBackendLog({ url, storeId })
    const oracle = {
      passed:
        distinct.size === 1 &&
        // Every agent, the reference client(s), and the Node checkpointer when there is one.
        digests.length >= options.agents + (mode === 'snapshot' ? 2 : 1) + (checkpointer === undefined ? 0 : 1) &&
        backendLog.contiguous === true &&
        backendLog.head === digests[0]?.upstreamHead &&
        restartRecords.every((record) => record.error === undefined),
      backendLog,
      maxHeadSeenByClients: globalHead,
      head: digests[0]?.upstreamHead ?? 0,
      digests,
      message:
        (distinct.size === 1
          ? `${digests.length} participants converged at head ${digests[0]?.upstreamHead} (${digests[0]?.rows} rows)`
          : `divergent states: ${[...distinct].join(' | ')}`) +
        `; backend log head ${backendLog.head} (${backendLog.count} events, contiguous=${backendLog.contiguous})` +
        `; max upstream head reported during the run ${globalHead}` +
        `; failed boots ${restartRecords.filter((record) => record.error !== undefined).length}`,
    }
    console.log(`oracle: ${oracle.passed === true ? 'PASS' : 'FAIL'} — ${oracle.message}`)

    const steadyRestarts = restartRecords
    const reports = [...hostReports.values()]
    const push = sumPush(reports)
    const lastCheckpointer = checkpointerReports.at(-1)
    const publishes = uniqueBy(
      checkpointerReports.flatMap((report) => (report.lastPublish === undefined ? [] : [report.lastPublish])),
      (publish) => publish.seqNum,
    )

    return {
      mode,
      storeId,
      preload,
      checkpointer:
        mode === 'snapshot'
          ? {
              catchUpMs,
              initialPublishMs,
              publishes: lastCheckpointer?.publishedCount ?? 0,
              failures: lastCheckpointer?.failedCount ?? 0,
              snapshotBytes: publishes.at(-1)?.byteLength,
              exportMs: stats(publishes.map((publish) => publish.exportMs)),
              publishTotalMs: stats(publishes.map((publish) => publish.totalMs)),
              tailEventsPerPublish: stats(publishes.slice(1).map((publish) => publish.tailEvents)),
              snapshotLagEvents: stats(checkpointerReports.map((report) => report.snapshotLag)),
              followLatencyMs: stats(checkpointerReports.flatMap((report) => report.followLatenciesMs)),
              rssMiB: lastCheckpointer === undefined ? undefined : lastCheckpointer.rssBytes / 2 ** 20,
            }
          : undefined,
      initialBoot: summarizeBoots(initialRecords),
      restarts: summarizeBoots(steadyRestarts),
      steady: {
        seconds,
        committedPerSecond: (committedEnd - committedStart) / seconds,
        confirmedPerSecond: (headEnd - headStart) / seconds,
        headStart,
        headEnd,
        ackLatencyMs: stats(ackLatencies),
        visibleLatencyMs: stats(visibleLatencies),
        push: {
          ...push,
          rejectedRatio: push.attempts === 0 ? 0 : (push.failures.ServerAheadError ?? 0) / push.attempts,
        },
        rebases: reports.flatMap((report) => report.agents).reduce((sum, agent) => sum + agent.rebases, 0),
        maxHostRssMiB: Math.max(...reports.map((report) => report.rssBytes / 2 ** 20)),
      },
      reference: referenceRecords,
      oracle,
    }
  } finally {
    await Promise.all(hosts.map((host) => host.close()))
  }
}

interface HostProcess {
  readonly name: string
  agentIds: ReadonlyArray<string>
  readonly request: (command: HostCommand) => Promise<HostReply>
  readonly close: () => Promise<void>
}

const hostEntry = fileURLToPath(new URL('./host-process.ts', import.meta.url))

const spawnHost = async (onEvent: (event: HostEvent) => void, name: string): Promise<HostProcess> => {
  const child: ChildProcess = fork(hostEntry, [], {
    execArgv: ['--import', import.meta.resolve('tsx'), '--max-old-space-size=2048'],
    serialization: 'advanced',
    stdio: ['ignore', 'inherit', 'pipe', 'ipc'],
  })
  // LiveStore logs sync warnings to stderr; keep them out of the summary unless asked for.
  child.stderr?.on('data', (chunk: Buffer) => {
    if (process.env.SNAPSHOT_BENCH_HOST_LOGS === '1') process.stderr.write(`[${name}] ${chunk.toString()}`)
  })
  let nextRequestId = 0
  const pending = new Map<number, { resolve: (reply: HostReply) => void; reject: (error: Error) => void }>()
  child.on('message', (message: HostMessage) => {
    if ('event' in message) return onEvent(message.event)
    const entry = pending.get(message.requestId)
    if (entry === undefined) return
    pending.delete(message.requestId)
    if (message.status === 'success') entry.resolve(message.reply)
    else entry.reject(new Error(`${name}: ${message.error}`))
  })
  child.on('exit', (code) => {
    for (const entry of pending.values()) entry.reject(new Error(`${name} exited with ${code}`))
    pending.clear()
  })
  return {
    name,
    agentIds: [],
    request: (command) =>
      new Promise((resolve, reject) => {
        if (child.connected === false) return reject(new Error(`${name} is not connected`))
        const requestId = nextRequestId++
        pending.set(requestId, { resolve, reject })
        child.send({ requestId, command })
      }),
    close: async () => {
      if (child.exitCode !== null || child.connected === false) return
      const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()))
      child.send({ requestId: -1, command: { _tag: 'shutdown' } })
      await Promise.race([exited, sleep(15_000)])
      if (child.exitCode === null) child.kill('SIGKILL')
    },
  }
}

/** Writes the preloaded history straight to the backend, bypassing clients (the provider splits it into pushes). */
const pushHistory = ({
  url,
  storeId,
  count,
  textBytes,
}: {
  url: string
  storeId: string
  count: number
  textBytes: number
}) =>
  Effect.runPromise(
    Effect.gen(function* () {
      if (count === 0) return
      const backend = yield* makeWsSync({ url })({ storeId, clientId: 'history-writer', payload: undefined })
      const events = makeHistoryEvents({ count, textBytes })
      for (let offset = 0; offset < events.length; offset += 1000) {
        yield* backend.push(events.slice(offset, offset + 1000))
      }
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(FetchHttpClient.layer, KeyValueStore.layerMemory))),
  )

/**
 * Reads the whole backend log with a fresh provider client (non-live pull) and checks that it is a contiguous chain
 * `1..head` where each event's parent is its predecessor.
 */
const inspectBackendLog = ({ url, storeId }: { url: string; storeId: string }) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const backend = yield* makeWsSync({ url })({ storeId, clientId: 'log-inspector', payload: undefined })
      const pages = yield* backend.pull(Option.none(), { live: false }).pipe(Stream.runCollectReadonlyArray)
      const events = pages.flatMap((page) => page.batch.map((item) => item.eventEncoded))
      let contiguous = true
      events.forEach((event, index) => {
        if (event.seqNum !== index + 1 || event.parentSeqNum !== index) contiguous = false
      })
      return { head: events.at(-1)?.seqNum ?? 0, count: events.length, contiguous }
    }).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(FetchHttpClient.layer, KeyValueStore.layerMemory))),
  )

/** Stats of the Cloudflare checkpointer Durable Object, through the benchmark Worker's `/__bench` route. */
const fetchCheckpointerStats = async (url: string, storeId: string) => {
  const response = await fetch(`${url}/__bench/checkpointer/stats?storeId=${encodeURIComponent(storeId)}`)
  if (response.ok === false) throw new Error(`checkpointer stats failed (${response.status})`)
  return (await response.json()) as {
    readonly head: number
    readonly publishedCount: number
    readonly failedCount: number
    readonly snapshotLag: number
    readonly latestManifest: { readonly seqNum: number } | null
    readonly lastPublish: NonNullable<CheckpointerReport['lastPublish']> | null
  }
}

const sumCommitted = (reports: ReadonlyMap<number, HostReport>) =>
  [...reports.values()].flatMap((report) => report.agents).reduce((sum, agent) => sum + agent.committed, 0)

const sumPush = (reports: ReadonlyArray<HostReport>) => {
  const failures: Record<string, number> = {}
  let attempts = 0
  let succeeded = 0
  let interrupted = 0
  let eventsPushed = 0
  for (const report of reports) {
    attempts += report.push.attempts
    succeeded += report.push.succeeded
    interrupted += report.push.interrupted
    eventsPushed += report.push.eventsPushed
    for (const [tag, count] of Object.entries(report.push.failures)) failures[tag] = (failures[tag] ?? 0) + count
  }
  return { attempts, succeeded, interrupted, eventsPushed, failures }
}

const summarizeBoots = (records: ReadonlyArray<RestartRecord>): BootSummary => {
  const ok = records.filter((record) => record.error === undefined)
  return {
    count: records.length,
    errors: records.length - ok.length,
    snapshotInstalled: ok.filter((record) => record.snapshot?.installed === true).length,
    timeToHeadMs: stats(ok.map((record) => record.timeToHeadMs)),
    bootMs: stats(ok.map((record) => record.bootMs)),
    downloadMs: stats(
      ok.flatMap((record) => (record.snapshot?.downloadMs === undefined ? [] : [record.snapshot.downloadMs])),
    ),
    snapshotBytes: stats(ok.flatMap((record) => (record.snapshot === undefined ? [] : [record.snapshot.byteLength]))),
    tailEvents: stats(ok.map((record) => record.targetHead - record.bootHead)),
    lostOnKill: stats(records.map((record) => record.unconfirmedAtKill)),
  }
}

const stats = (values: ReadonlyArray<number>): Stats => ({
  n: values.length,
  p50: percentile(values, 50),
  p95: percentile(values, 95),
  p99: percentile(values, 99),
  // A loop, not `Math.max(...values)`: latency samples of a long run exceed the engine's argument limit.
  max: values.length === 0 ? undefined : values.reduce((max, value) => (value > max ? value : max), -Infinity),
})

const uniqueBy = <A>(values: ReadonlyArray<A>, key: (value: A) => unknown) => {
  const seen = new Set<unknown>()
  return values.filter((value) => {
    const k = key(value)
    if (seen.has(k) === true) return false
    seen.add(k)
    return true
  })
}

const fmt = (value: number | undefined, digits = 0) => (value === undefined ? '—' : value.toFixed(digits))

const printSummary = (results: ReadonlyArray<ModeResult>) => {
  const rows: Array<[string, ...string[]]> = [
    ['metric', ...results.map((result) => result.mode)],
    [
      'restarts (ok/errors/snapshot)',
      ...results.map(
        (r) => `${r.restarts.count - r.restarts.errors}/${r.restarts.errors}/${r.restarts.snapshotInstalled}`,
      ),
    ],
    ['restart time-to-head p50 ms', ...results.map((r) => fmt(r.restarts.timeToHeadMs.p50))],
    ['restart time-to-head p95 ms', ...results.map((r) => fmt(r.restarts.timeToHeadMs.p95))],
    ['restart createStore p50 ms', ...results.map((r) => fmt(r.restarts.bootMs.p50))],
    ['restart tail events p50', ...results.map((r) => fmt(r.restarts.tailEvents.p50))],
    [
      'unconfirmed events lost per kill p50/max',
      ...results.map((r) => `${fmt(r.restarts.lostOnKill.p50)}/${fmt(r.restarts.lostOnKill.max)}`),
    ],
    ['initial boot time-to-head p50 ms', ...results.map((r) => fmt(r.initialBoot.timeToHeadMs.p50))],
    ['reference full replay ms', ...results.map((r) => fmt(r.reference.fullReplay?.timeToHeadMs))],
    ['reference snapshot boot ms', ...results.map((r) => fmt(r.reference.snapshotBoot?.timeToHeadMs))],
    ['committed ev/s', ...results.map((r) => fmt(r.steady.committedPerSecond, 1))],
    ['confirmed ev/s', ...results.map((r) => fmt(r.steady.confirmedPerSecond, 1))],
    [
      'commit→confirmed p50/p95 ms',
      ...results.map((r) => `${fmt(r.steady.ackLatencyMs.p50)}/${fmt(r.steady.ackLatencyMs.p95)}`),
    ],
    [
      'commit→visible p50/p95 ms',
      ...results.map((r) => `${fmt(r.steady.visibleLatencyMs.p50)}/${fmt(r.steady.visibleLatencyMs.p95)}`),
    ],
    [
      'push attempts/ok/rejected/interrupted',
      ...results.map(
        (r) =>
          `${r.steady.push.attempts}/${r.steady.push.succeeded}/${r.steady.push.failures.ServerAheadError ?? 0}/${r.steady.push.interrupted}`,
      ),
    ],
    ['rebases', ...results.map((r) => String(r.steady.rebases))],
    [
      'snapshots published',
      ...results.map((r) => (r.checkpointer === undefined ? '—' : String(r.checkpointer.publishes))),
    ],
    ['snapshot bytes', ...results.map((r) => fmt(r.checkpointer?.snapshotBytes))],
    [
      'checkpointer snapshot lag p50/max ev',
      ...results.map((r) =>
        r.checkpointer === undefined
          ? '—'
          : `${fmt(r.checkpointer.snapshotLagEvents.p50)}/${fmt(r.checkpointer.snapshotLagEvents.max)}`,
      ),
    ],
    [
      'checkpointer follow p50/p95 ms',
      ...results.map((r) =>
        r.checkpointer === undefined
          ? '—'
          : `${fmt(r.checkpointer.followLatencyMs.p50)}/${fmt(r.checkpointer.followLatencyMs.p95)}`,
      ),
    ],
    ['oracle', ...results.map((r) => (r.oracle.passed === true ? 'pass' : 'FAIL'))],
  ]
  const widths = rows[0]!.map((_, column) => Math.max(...rows.map((row) => row[column]!.length)))
  for (const row of rows) console.log(row.map((cell, column) => cell.padEnd(widths[column]!)).join('  '))
}

const parseOptions = (args: ReadonlyArray<string>): BenchOptions => {
  if (args.includes('--help') === true) {
    console.log(`Usage: pnpm snapshot-bench [--core-path <path>] [options]

  --agents <n>            Concurrent agents (default 10)
  --hosts <n>             Agent host processes (default min(agents, 8))
  --rate <n>              Events per second per agent (default 10)
  --update-ratio <f>      Fraction of updates vs appends (default 0.2)
  --text-bytes <n>        Event payload size (default 200)
  --history <n>           Backend events written before agents start (default 20000)
  --duration <s>          Steady-phase length in seconds (default 60)
  --restart-every <s>     Each agent restarts every s seconds, staggered; 0 disables (default 20)
  --downtime <ms>         Sandbox downtime between kill and fresh boot (default 500)
  --mode <both|baseline|snapshot>  (default both)
  --realization <node|cf> node: Node checkpointer + file-system snapshot store (default);
                          cf: checkpointer Durable Object, R2 and the Worker's snapshot routes
  --kill <abrupt|graceful> abrupt: pending events die with the sandbox; graceful: drained first (default abrupt)
  --min-interval <ms>     Checkpointer minimum interval between snapshots (default 10000)
  --min-tail <n>          Checkpointer minimum tail events (default 100)
  --head-timeout <s>      A boot that does not reach its target head in time counts as failed (default 120)
  --settle-timeout <s>    Bound for the final settle before the oracle (default 180)
  --label <text>          Run label (default derived from parameters)
  --out <dir>             Output directory (default artifacts/snapshot-boot)`)
    process.exit(0)
  }
  const number = (name: string, fallback: number) => {
    const value = readOption(args, name)
    if (value === undefined) return fallback
    const parsed = Number(value)
    if (Number.isFinite(parsed) === false) throw new Error(`Invalid ${name} '${value}'`)
    return parsed
  }
  const agents = number('--agents', 10)
  const ratePerAgent = number('--rate', 10)
  const history = number('--history', 20_000)
  const mode = readOption(args, '--mode') ?? 'both'
  if (mode !== 'both' && mode !== 'baseline' && mode !== 'snapshot') throw new Error(`Invalid --mode '${mode}'`)
  return {
    agents,
    hosts: number('--hosts', Math.min(agents, 8)),
    ratePerAgent,
    updateRatio: number('--update-ratio', 0.2),
    textBytes: number('--text-bytes', 200),
    history,
    durationMs: number('--duration', 60) * 1000,
    restartEveryMs: number('--restart-every', 20) * 1000,
    downtimeMs: number('--downtime', 500),
    modes: mode === 'both' ? ['baseline', 'snapshot'] : [mode],
    minIntervalMs: number('--min-interval', 10_000),
    minTailEvents: number('--min-tail', 100),
    seed: number('--seed', 1),
    snapshotTimeoutMs: number('--snapshot-timeout', 60_000),
    headTimeoutMs: number('--head-timeout', 120) * 1000,
    killMode: readOption(args, '--kill') === 'graceful' ? 'graceful' : 'abrupt',
    realization: readOption(args, '--realization') === 'cf' ? 'cf' : 'node',
    settleTimeoutMs: number('--settle-timeout', 180) * 1000,
    outDir: path.resolve(readOption(args, '--out') ?? path.join(import.meta.dirname, '../../artifacts/snapshot-boot')),
    label: readOption(args, '--label') ?? `a${agents}-r${ratePerAgent}-h${history}`,
  }
}

/** Reads an option; a later occurrence overrides an earlier one. */
const readOption = (args: ReadonlyArray<string>, name: string): string | undefined => {
  const inline = args.findLast((arg) => arg.startsWith(`${name}=`))
  if (inline !== undefined) return inline.slice(name.length + 1)
  const index = args.lastIndexOf(name)
  if (index === -1) return undefined
  const value = args[index + 1]
  if (value === undefined || value.startsWith('--') === true) throw new Error(`Missing value for ${name}`)
  return value
}

const options = parseOptions(process.argv.slice(2))

const program = Effect.gen(function* () {
  const server = yield* WranglerDevServer.make({
    cwd: path.join(
      import.meta.dirname,
      '..',
      'backends',
      options.realization === 'cf' ? 'sync-cf-snapshots' : 'sync-cf',
    ),
    showLogs: process.env.SCENARIO_BACKEND_LOGS === '1',
    // Generous bounds: on a busy machine workerd can take well over the default 5s to answer its first request.
    readiness: { startupTimeout: '60 seconds', connectTimeout: '30 seconds', retrySchedule: Schedule.recurs(2) },
  })
  console.log(`sync-cf: ${server.url}`)
  const exitCode = yield* Effect.promise(() => main(options, server.url))
  process.exitCode = exitCode
}).pipe(Effect.scoped, Effect.provide(Layer.mergeAll(PlatformNode.NodeServices.layer, FetchHttpClient.layer)))

PlatformNode.NodeRuntime.runMain(program)
