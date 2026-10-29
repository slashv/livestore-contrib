/// <reference types="@cloudflare/workers-types" />

import {
  type CheckpointerDoRpcInterface,
  getCheckpointerStub,
  makeCheckpointerDurableObject,
} from '@livestore/adapter-cloudflare'
import { Snapshot } from '@livestore/common'
import { type CfTypes, makeDurableObject, makeWorker, type SyncBackendRpcInterface } from '@livestore/sync-cf/cf-worker'

import { todoSchema } from '../../corpus/applications/todo-schema.ts'

/**
 * Local sync-cf Worker with the Cloudflare realization of experimental snapshot boot, for the snapshot boot benchmark:
 * the sync Durable Object stores manifests and wakes a checkpointer Durable Object on push, the checkpointer publishes
 * blobs to R2, and the Worker serves `GET /snapshot/manifest` and `GET /snapshot/blob` to `makeHttpSnapshotSource`.
 *
 * `/__bench/checkpointer/{stats,publish-now}?storeId=…` drive and observe the checkpointer.
 */

interface Env {
  SYNC_BACKEND_DO: CfTypes.DurableObjectNamespace<SyncBackendRpcInterface>
  CHECKPOINTER_DO: CfTypes.DurableObjectNamespace<CheckpointerDoRpcInterface>
  SNAPSHOT_BUCKET: CfTypes.R2Bucket
}

const snapshotKey = Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(todoSchema))

export class SyncBackendDO extends makeDurableObject({
  snapshots: { checkpointerBinding: 'CHECKPOINTER_DO', snapshotKeys: [snapshotKey], wakeIntervalMs: 1_000 },
}) {}

/** Same cost model defaults as the Node checkpointer of the benchmark. */
export class CheckpointerDO extends makeCheckpointerDurableObject<Env>({
  schema: todoSchema,
  syncBackendBinding: 'SYNC_BACKEND_DO',
  bucketBinding: 'SNAPSHOT_BUCKET',
  bindingName: 'CHECKPOINTER_DO',
  pollInterval: 1_000,
}) {}

const worker = makeWorker<Env>({
  syncBackendBinding: 'SYNC_BACKEND_DO',
  enableCORS: true,
  snapshots: { bucketBinding: 'SNAPSHOT_BUCKET' },
})

export default {
  async fetch(request: CfTypes.Request, env: Env, ctx: CfTypes.ExecutionContext): Promise<CfTypes.Response> {
    const url = new URL(request.url)
    if (url.pathname.startsWith('/__bench/checkpointer/') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) return new Response('Missing storeId', { status: 400 }) as unknown as CfTypes.Response
      const checkpointer = getCheckpointerStub({ namespace: env.CHECKPOINTER_DO, storeId, schema: todoSchema })
      if (url.pathname.endsWith('/stats') === true) {
        return Response.json(await checkpointer.stats(storeId)) as unknown as CfTypes.Response
      }
      if (url.pathname.endsWith('/publish-now') === true) {
        return Response.json(await checkpointer.publishNow(storeId)) as unknown as CfTypes.Response
      }
    }
    return worker.fetch(request, env, ctx)
  },
}
