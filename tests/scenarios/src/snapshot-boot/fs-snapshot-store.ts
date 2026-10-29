import { randomUUID } from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'

import { Snapshot } from '@livestore/common'
import { Effect, Option, Schema } from '@livestore/utils/effect'

/**
 * File-system realization of the experimental snapshot sink/source contract, so a checkpointer and agents in separate
 * Node processes can share snapshots without object storage.
 *
 * Layout under `rootDir`:
 * - `blobs/<key>/<seqNum>-<uuid>.sqlite` — immutable snapshot blobs (the manifest's `objectKey` is this relative path)
 * - `manifests/<key>.json` — the current manifest pointer per snapshot key
 *
 * Mirrors the object-storage contract: the blob is written (tmp file + rename) before the manifest pointer moves, the
 * pointer is replaced atomically by rename, a key's position only advances, and a replaced blob is deleted only after
 * `graceMs`, so in-flight downloads of the previous manifest complete.
 *
 * Only one writer (the checkpointer) per key is supported; readers may live in any process.
 */
export const makeFsSnapshotStore = ({ rootDir, storeId, graceMs = 60_000 }: FsSnapshotStoreOptions) => {
  const manifestPath = (key: string) => path.join(rootDir, 'manifests', `${key}.json`)
  const blobPath = (objectKey: string) => path.join(rootDir, objectKey)
  /** Blobs replaced by a newer manifest, with the time they were replaced; pruned once older than the grace period. */
  const replaced: Array<{ objectKey: string; replacedAt: number }> = []

  const readManifest = (key: string) =>
    Effect.tryPromise({
      try: async () => {
        try {
          return Option.some(await fs.readFile(manifestPath(key), 'utf8'))
        } catch (cause) {
          if (isNotFound(cause) === true) return Option.none<string>()
          throw cause
        }
      },
      catch: (cause) => snapshotError('unavailable', `Could not read manifest for ${key}`, cause),
    }).pipe(
      Effect.flatMap((text) =>
        Option.match(text, {
          onNone: () => Effect.succeed(Option.none<Snapshot.SnapshotManifest>()),
          onSome: (json) =>
            Schema.decodeUnknownEffect(Schema.fromJsonString(Snapshot.SnapshotManifest))(json).pipe(
              Effect.map(Option.some),
              Effect.mapError((cause) => snapshotError('internal', `Invalid manifest for ${key}`, cause)),
            ),
        }),
      ),
    )

  const pruneReplaced = (now: number) =>
    Effect.promise(async () => {
      for (let index = replaced.length - 1; index >= 0; index -= 1) {
        const entry = replaced[index]!
        if (now - entry.replacedAt < graceMs) continue
        replaced.splice(index, 1)
        await fs.rm(blobPath(entry.objectKey), { force: true })
      }
    })

  const sink: Snapshot.SnapshotSink = {
    publish: ({ manifest, bytes }) =>
      Effect.gen(function* () {
        if (manifest.storeId !== storeId) {
          return yield* snapshotError('incompatible', `Wrong storeId ${manifest.storeId}`)
        }
        const previous = yield* readManifest(manifest.key)
        if (Option.isSome(previous) === true && previous.value.seqNum > manifest.seqNum) {
          return yield* snapshotError(
            'conflict',
            `Snapshot at ${manifest.seqNum} is older than the published one at ${previous.value.seqNum}`,
          )
        }

        const objectKey = path.join('blobs', manifest.key, `${manifest.seqNum}-${randomUUID()}.sqlite`)
        const published: Snapshot.SnapshotManifest = { ...manifest, objectKey }
        const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(Snapshot.SnapshotManifest))(published).pipe(
          Effect.mapError((cause) => snapshotError('internal', 'Could not encode manifest', cause)),
        )
        yield* Effect.tryPromise({
          try: async () => {
            // Blob first, pointer second: a reader never observes a manifest whose blob is missing.
            await writeAtomically(blobPath(objectKey), bytes)
            await writeAtomically(manifestPath(manifest.key), encoded)
          },
          catch: (cause) => snapshotError('unavailable', `Could not publish snapshot at ${manifest.seqNum}`, cause),
        })

        const now = Date.now()
        if (Option.isSome(previous) === true) replaced.push({ objectKey: previous.value.objectKey, replacedAt: now })
        yield* pruneReplaced(now)
        return published
      }),
    getManifest: ({ key }) => readManifest(key),
  }

  const source: Snapshot.SnapshotSource = {
    getManifest: ({ key }) => readManifest(key),
    download: (manifest) =>
      Effect.tryPromise({
        try: async () => {
          const buffer = await fs.readFile(blobPath(manifest.objectKey))
          return new Uint8Array(buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength))
        },
        catch: (cause) =>
          isNotFound(cause) === true
            ? snapshotError('not-found', `No blob at ${manifest.objectKey}`, cause)
            : snapshotError('unavailable', `Could not read blob ${manifest.objectKey}`, cause),
      }),
  }

  const makeSource: Snapshot.SnapshotSourceConstructor = () => Effect.succeed(source)

  return { sink, source, makeSource }
}

export interface FsSnapshotStoreOptions {
  readonly rootDir: string
  readonly storeId: string
  /** How long a replaced blob stays readable. */
  readonly graceMs?: number
}

const writeAtomically = async (target: string, data: Uint8Array | string) => {
  await fs.mkdir(path.dirname(target), { recursive: true })
  const temporary = `${target}.${randomUUID()}.tmp`
  await fs.writeFile(temporary, data)
  await fs.rename(temporary, target)
}

const isNotFound = (cause: unknown) =>
  typeof cause === 'object' && cause !== null && 'code' in cause && cause.code === 'ENOENT'

const snapshotError = (reason: Snapshot.SnapshotError['reason'], message: string, cause?: unknown) =>
  new Snapshot.SnapshotError({ reason, message, ...(cause === undefined ? {} : { cause }) })
