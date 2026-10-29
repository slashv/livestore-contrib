import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

import { describe, expect, it } from 'vitest'

import { Effect, Exit, Option } from '@livestore/utils/effect'

import { makeFsSnapshotStore } from './fs-snapshot-store.ts'
import { commitTimeOf, makeHistoryEvents, nextAgentEvent, seededRandom, todoStateDigest } from './workload.ts'

const manifest = (seqNum: number) => ({
  manifestVersion: 1 as const,
  storeId: 'store',
  key: 'v1.state.events',
  backendId: 'backend',
  seqNum,
  byteLength: 3,
  sha256: 'unused-by-the-store',
  createdAt: Date.now(),
})

const tempDir = () => fs.mkdtemp(path.join(os.tmpdir(), 'fs-snapshot-store-'))

describe('file-system snapshot store', () => {
  it('publishes a blob before its manifest and serves both to other readers', async () => {
    const rootDir = await tempDir()
    const writer = makeFsSnapshotStore({ rootDir, storeId: 'store' })
    const reader = makeFsSnapshotStore({ rootDir, storeId: 'store' })

    expect(Option.isNone(await Effect.runPromise(reader.source.getManifest({ key: 'v1.state.events' })))).toBe(true)
    const published = await Effect.runPromise(
      writer.sink.publish({ manifest: manifest(10), bytes: new Uint8Array([1, 2, 3]) }),
    )
    const fetched = Option.getOrThrow(await Effect.runPromise(reader.source.getManifest({ key: 'v1.state.events' })))
    expect(fetched).toEqual(published)
    expect([...(await Effect.runPromise(reader.source.download(fetched)))]).toEqual([1, 2, 3])
  })

  it('only lets a key advance and keeps replaced blobs for the grace period', async () => {
    const rootDir = await tempDir()
    const store = makeFsSnapshotStore({ rootDir, storeId: 'store', graceMs: 0 })
    const first = await Effect.runPromise(
      store.sink.publish({ manifest: manifest(10), bytes: new Uint8Array([1, 1, 1]) }),
    )
    await Effect.runPromise(store.sink.publish({ manifest: manifest(20), bytes: new Uint8Array([2, 2, 2]) }))

    const older = await Effect.runPromiseExit(store.sink.publish({ manifest: manifest(15), bytes: new Uint8Array(3) }))
    expect(Exit.isFailure(older) && JSON.stringify(older.cause).includes('conflict')).toBe(true)

    // With a zero grace period the replaced blob is gone, so a client holding the old manifest falls back.
    const download = await Effect.runPromiseExit(store.source.download(first))
    expect(Exit.isFailure(download) && JSON.stringify(download.cause).includes('not-found')).toBe(true)
  })

  it('rejects manifests for another store', async () => {
    const store = makeFsSnapshotStore({ rootDir: await tempDir(), storeId: 'store' })
    const exit = await Effect.runPromiseExit(
      store.sink.publish({ manifest: { ...manifest(1), storeId: 'other' }, bytes: new Uint8Array(3) }),
    )
    expect(Exit.isFailure(exit)).toBe(true)
  })
})

describe('snapshot boot workload', () => {
  it('writes a contiguous history chain', () => {
    const events = makeHistoryEvents({ count: 50, textBytes: 16 })
    events.forEach((event, index) => {
      expect(event.seqNum).toBe(index + 1)
      expect(event.parentSeqNum).toBe(index)
    })
    expect(events.every((event) => commitTimeOf(event) === undefined)).toBe(true)
  })

  it('embeds the commit time in agent events and mixes appends with updates of own todos', () => {
    const ownTodoIds: string[] = []
    const random = seededRandom(3)
    const params = { ratePerAgent: 1, updateRatio: 0.5, textBytes: 64 }
    const events = Array.from({ length: 40 }, (_, op) =>
      nextAgentEvent({ agentId: 'agent-1', incarnation: 0, op, ownTodoIds, params, random }),
    )
    const names = new Set(events.map(({ event }) => event.name))
    expect(names).toEqual(new Set(['v1.TodoCreated', 'v1.TodoTextChanged']))
    expect(events.every(({ text }) => text.length === 64)).toBe(true)

    const encoded = {
      name: 'v1.TodoCreated',
      args: { id: 'x', text: events[0]!.text },
      seqNum: 1,
      parentSeqNum: 0,
      clientId: 'c',
      sessionId: 's',
    }
    expect(Math.abs(commitTimeOf(encoded as never)! - Date.now())).toBeLessThan(5_000)
  })

  it('digests state independently of row order', () => {
    const rows = [
      { id: 'b', text: 'two', completed: false },
      { id: 'a', text: 'one', completed: true },
    ]
    expect(todoStateDigest(rows)).toEqual(todoStateDigest(rows.toReversed()))
    expect(todoStateDigest(rows).digest).not.toBe(todoStateDigest([rows[0]!]).digest)
  })
})
