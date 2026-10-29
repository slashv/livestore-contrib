# Snapshot boot multi-agent scenarios

Status: experimental, measured against an in-progress core branch. Related: the core
intent node `context/02-system/03-sync/04-snapshots/` (LS.SYS.SYNC.SNAP-\*) and RFC
`contributor-docs/rfcs/0005-snapshot-boot.md` in `livestorejs/livestore`.

## Why

The target workload is up to about 100 AI agents per system, each writing events at
a high rate, mostly appends. Agents run in fresh sandboxes that restart often, so
every restart is a cold start: the local databases are empty and the client replays
the store's whole history before it reaches the head. Snapshot boot (opt-in through
`syncOptions.snapshot = { source, timeout? }`) lets a fresh client install a state
snapshot that a checkpointer published and pull only the tail after it.

These scenarios measure what that buys under the agent workload, where it stops
helping, and whether restarted agents still converge.

## Topology

```
                      local sync-cf (wrangler dev, SQLite Durable Object)
              ▲ push/pull (WebSocket)        ▲ live pull                 ▲ non-live pull
              │                              │                           │
 agent host 1..K (Node processes)       checkpointer (Node process)   controller
 N agents spread over K hosts           leader + makeCheckpointer     history preload,
 each agent: Store on the in-memory     publishes to a                restart schedule,
 adapter (fresh sandbox per boot)       file-system SnapshotSink      head broadcast,
   └─ snapshot mode: FS SnapshotSource ───────┘ (shared directory)    oracle
```

- **Agent.** A real LiveStore Store on `makeInMemoryAdapter`, synced with
  `makeWsSync`. A restart closes the Store abruptly (pending events are lost with
  the sandbox, as with a killed process), waits `--downtime`, then creates a new
  Store with new empty databases and a new `clientId`. The restarted agent resumes
  writing once it reached the head.
- **Checkpointer** (snapshot mode only). A headless leader built with
  `makeLeaderThreadLayer` on in-memory wasm SQLite, live-pulling and never pushing,
  running the core `makeCheckpointer` loop with the default cost model
  (`alpha 0.5`, `minIntervalMs 10s`, `minTailEvents 100`).
- **Snapshot store.** `src/snapshot-boot/fs-snapshot-store.ts` realizes
  `SnapshotSink`/`SnapshotSource` on a shared directory: blob (tmp + rename) before
  manifest (tmp + rename), a key only advances, replaced blobs are deleted after a
  60 s grace period.
- **Backend.** The scenario runner's local `sync-cf` Worker and SQLite Durable
  Object under workerd (`src/backends/sync-cf`).

## Workload

- Event mix: 80% appends (`v1.TodoCreated`), 20% updates (`v1.TodoTextChanged`) of
  one of the agent's own earlier todos; 200-byte text payloads. State therefore
  grows with history, which is the realistic and the unfavourable case for
  snapshots: the snapshot is O(state) ≈ O(history) bytes.
- Rate: `--rate` events per second per agent, as separate commits.
- History: `--history` events are written straight to the backend before agents
  start (the same mix from ten pseudo-writers).
- Restarts: each agent restarts every `--restart-every` seconds, staggered evenly
  across agents, during a `--duration` steady phase.

## Metrics

| Metric                     | Definition                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| Time-to-head               | Fresh `createStore` start until the leader's backend-confirmed head and the session both reached the global head at restart |
| `createStore`              | Store creation alone; in snapshot mode it includes manifest fetch, download, verify and install                             |
| Tail events                | Target head minus the head the client booted at (snapshot head, or 0)                                                       |
| Lost on kill               | Leader-pending plus session-pending events of the killed incarnation                                                        |
| Committed / confirmed ev/s | Agent commits per second, and growth of the backend-confirmed head per second, during the steady phase                      |
| Commit→confirmed           | Commit until the agent pulled its own event back from the backend                                                           |
| Commit→visible             | Commit at one agent until a live agent on another host pulled it (sampled, same machine clock)                              |
| Push attempts/rejected     | Provider push calls and `ServerAheadError` rejections; most pushes end interrupted because the pull confirms them first     |
| Rebases                    | Sum of leader rebase generations over all incarnations                                                                      |
| Checkpointer lag           | Events between the checkpointer head and its latest snapshot; commit→applied latency at the checkpointer                    |
| Reference boots            | After the run, on a quiet system: one fresh full-replay client and one fresh snapshot-boot client                           |

Oracle (each mode): after writers stop and everything settles, every live agent,
the checkpointer and both reference clients have the same Todo state digest at the
same head, the backend log is a contiguous chain whose head equals that head, and
no boot failed (each must reach its head within `--head-timeout`).

## Scenarios and hypotheses

Every scenario runs twice on the same machine, once without snapshots (baseline)
and once with snapshot boot, each on a fresh backend store.

| ID  | Agents | Rate / agent | Total ev/s | History | Steady | Restart every | Snapshot realization |
| --- | ------ | ------------ | ---------- | ------- | ------ | ------------- | -------------------- |
| S1  | 10     | 10           | 100        | 5 000   | 120 s  | 60 s          | Node                 |
| S2  | 25     | 10           | 250        | 5 000   | 120 s  | 60 s          | Node                 |
| S3  | 50     | 5            | 250        | 5 000   | 120 s  | 60 s          | Node                 |
| S4  | 100    | 2            | 200        | 5 000   | 120 s  | 90 s          | Node                 |
| R1  | 10     | 50           | 500        | 5 000   | 120 s  | 60 s          | Node                 |
| CF1 | 10     | 10           | 100        | 5 000   | 120 s  | 60 s          | Cloudflare           |
| B1  | 1      | 1            | 1          | 10 000  | 5 s    | —             | Node                 |

B1 isolates cold-boot cost: one agent, no restarts, a 10 000-event history.
The history is smaller than a production store's on purpose: on the shared
benchmark machine a full replay costs about 3 ms per event (see Results), so a
larger history makes every baseline restart take minutes and the baseline runs
would mostly measure agents that never finish booting.

Hypotheses:

- **H1 time-to-head.** Without snapshots ≈ c·history (pull + materialize every
  event), so it grows with the run. With snapshots ≈ manifest + download(state)
  - install + tail, where the tail is bounded by the trigger (a few hundred to a
    few thousand events), so it stays roughly flat.
- **H2 throughput.** Snapshots raise committed throughput because agents spend
  less time booting (they write only after reaching the head) and the backend
  serves far fewer history pages. Confirmed throughput is bounded by the single
  Durable Object either way.
- **H3 contention.** Push rejections and rebases grow with the number of
  concurrent writers, independently of snapshots; a rebase storm shows up as
  commit→confirmed latency growing faster than the event rate.
- **H4 checkpointer.** One follower keeps up at these rates; snapshot lag stays
  around the trigger's tail threshold and each publish costs O(state).
- **H5 convergence.** Restarted agents converge with the others and with a
  full-replay reference in both modes.

## Running

From the repository root, with `repos/livestore` materialized and dependencies
installed (Minimal Setup is enough; see the setup notes below):

```sh
pnpm --dir tests/scenarios snapshot-bench \
  --core-path <livestore checkout with snapshot boot> \
  --agents 10 --rate 10 --history 10000 --duration 90 --restart-every 30
```

`--help` lists every option (`--hosts`, `--mode both|baseline|snapshot`,
`--kill abrupt|graceful`, `--downtime`, `--min-interval`, `--update-ratio`,
`--text-bytes`, timeouts). Results go to
`tests/scenarios/artifacts/snapshot-boot/<run>/result.json` (ignored by Git) and
a comparison table is printed. `SNAPSHOT_BENCH_HOST_LOGS=1` forwards participant
stderr.

The benchmark needs a core revision that exports the snapshot API
(`Snapshot` from `@livestore/common`, `makeCheckpointer` from
`@livestore/common/leader-thread`). A baseline-only run (`--mode baseline`) also
works on cores without it.

Setup used for the results below, in a fresh contrib worktree without devenv:

```sh
git clone --no-checkout <local livestore clone> repos/livestore
git -C repos/livestore checkout --detach "$(jq -r .members.livestore.commit megarepo.lock)"
DEVENV_TASK_PASSTHROUGH=1 <devenv pnpm> install --frozen-lockfile
```

## Results: quiet machine, after core fixes

Measured on 2026-10-29/30 on the same Apple M1 Pro (10 cores, 32 GB), Node
24.13.1, local `wrangler dev`, against core `claude/snapshot-boot-6d51c8` at
`620dce76b` (10 × 10 run) and `5f6974a49` (all later runs). The core now includes
the fixes for the stalls reported below: a WebSocket ping timeout now fails
in-flight RPC calls (the silent live-pull stall), rejected pushes no longer wait
for a pull that never comes, and an ended live pull is restarted. It also
includes per-call SQLite leak fixes and the restart pending-set and rebase fixes,
and raises the trigger's `replayCostPerEventMs` default to 0.5. The Node
checkpointer runs its leader with `headless: true`. The machine was mostly
idle: the 1-minute load average was 11–22 during these runs. Runs were strictly
sequential, one run per row, with the same commands as below (100 × 2:
`--mode snapshot --agents 100 --rate 2 --restart-every 90 --head-timeout 300
--settle-timeout 300`).

| Run      | Load (start) | Mode          | Restarts ok/failed | TTH p50 / p95   | Tail p50 | Init boot p50 | Committed ev/s | Confirmed ev/s | Commit→confirmed p50 / p95 | Push attempts / rejected | Rebases | Lost/kill p50 | Snapshots (size) | Oracle |
| -------- | ------------ | ------------- | ------------------ | --------------- | -------- | ------------- | -------------- | -------------- | -------------------------- | ------------------------ | ------- | ------------- | ---------------- | ------ |
| 10 × 10  | 12           | baseline      | 20 / 0             | 38.2 s / 58.3 s | 8 454    | 11.2 s        | 45.9           | 38.5           | 5.2 s / 42.6 s             | 1 723 / 295              | 17      | 7             | —                | pass   |
| 10 × 10  | 12           | snapshot      | 20 / 0             | 3.0 s / 10.0 s  | 114      | 0.1 s         | 89.5           | 39.8           | 5.7 s / 31.6 s             | 1 678 / 435              | 36      | 219           | 16 (2.3 MB)      | pass   |
| 25 × 10  | 19           | baseline      | 45 / 0             | 50.3 s / 62.8 s | 7 977    | 21.5 s        | 101.9          | 39.4           | 21.2 s / 38.7 s            | 1 414 / 470              | 61      | 200           | —                | pass   |
| 25 × 10  | 19           | snapshot      | 50 / 0             | 5.3 s / 12.5 s  | 120      | 0.3 s         | 224.1          | 32.6           | 17.1 s / 43.7 s            | 2 084 / 881              | 84      | 484           | 20 (2.8 MB)      | pass   |
| 50 × 5   | 18           | baseline      | 50 / 0             | 74.6 s / 91.9 s | 5 543    | 45.8 s        | 99.8           | 16.4           | 23.6 s / 52.7 s            | 1 635 / 470              | 87      | 192           | —                | pass   |
| 50 × 5   | 18           | snapshot      | 100 / 0            | 8.4 s / 16.6 s  | 100      | 0.4 s         | 205.8          | 22.6           | 18.7 s / 47.9 s            | 3 732 / 1 522            | 131     | 221           | 39 (2.6 MB)      | pass   |
| 100 × 2  | 16           | snapshot      | 133 / 0            | 17.0 s / 27.4 s | 50       | 0.7 s         | 167.7          | 14.6           | 28.4 s / 69.9 s            | 6 650 / 1 858            | 163     | 109           | 50 (2.7 MB)      | pass   |
| 10 × 10¹ | 14           | snapshot (CF) | 19 / 0             | 5.0 s / 10.9 s  | 148      | 0.4 s         | 78.9           | 37.3           | 3.9 s / 18.3 s             | 924 / 203                | 29      | 137           | 8 (1.6 MB)       | pass   |

1. 60 s steady phase, restart every 30 s (as CF1 below).

Reference clients after the runs (quiet backend, ≈ 10–14 k events): full replay
3.8–6.1 s, snapshot boot 0.0–0.1 s. One exception: the full-replay reference
after the 100-agent run took 76.5 s. The Node checkpointer published 16–50
snapshots per run (export p50 2–4 ms, publish p50 19–28 ms, tail per publish
p50 146–350 events). The Cloudflare checkpointer published 8 (publish p50
80 ms). The CF blob download p50 was 574 ms, against 4–47 ms for the
file-system source.

What changed compared with the shared-machine runs:

- **No stalls.** Every run passed the oracle. In each, every agent, the
  checkpointer and both reference clients ended at the backend head with
  identical state, the backend log was contiguous, and no boot missed its
  deadline. This includes 50 agents (both modes) and 100 agents with snapshots.
  The 50-agent baseline, which previously never booted, now completes.
- **Restart time-to-head.** With snapshots: 3.0 s (10 agents), 5.3 s (25),
  8.4 s (50), 17.0 s (100), each with a tail of 50–150 events. Without:
  38–75 s, replaying 5 500–8 500 events. That is a 9–13× improvement. At these
  sizes the snapshot download and install costs 50–120 ms; the rest is the
  backend and host CPUs being busy with the write load.
- **Committed throughput** roughly doubles with snapshots (45.9 → 89.5,
  101.9 → 224.1, 99.8 → 205.8 ev/s), because agents spend seconds instead of a
  minute unable to work after each restart.
- **Push contention is unchanged and is now the clear ceiling.** Confirmed
  throughput stays at 15–40 ev/s whatever the offered load, falling as writers
  increase: about 40 ev/s at 10–25 agents, 16–23 at 50, 15 at 100. That is 7–45%
  of what agents commit. 21–42% of pushes are rejected with `ServerAheadError`,
  and commit→confirmed p50 grows from 5 s (10 agents) to 28 s (100). The
  confirmation backlog is also what a killed sandbox loses (109–484 events per
  kill with snapshots). Snapshot boot does not address this. The many-writer
  push/rebase path against one Durable Object is the limit for the 100-agent
  target.

## Results: shared machine, before core fixes

Measured on 2026-10-29 on an Apple M1 Pro (10 cores, 32 GB), macOS (Darwin
27.0.0), Node 24.13.1, local `wrangler dev` (workerd), against the core worktree
`claude/snapshot-boot-6d51c8` (revisions `fe049d9`…`28f9867` plus uncommitted
work; each result file records its exact revision). The laptop was **heavily
shared**: other agents ran a VM and the core's own snapshot benchmarks
concurrently, so the 1-minute load average sat between 30 and 160 during these
runs (recorded per run). Absolute times are therefore pessimistic and noisy; the
baseline/snapshot ratios within one run are the meaningful signal. Each row is a
single run.

Commands (from `tests/scenarios`, `CORE=<snapshot core worktree>`):

```sh
pnpm snapshot-bench --core-path $CORE --agents 1 --history 10000 --duration 5 --restart-every 0 --rate 1   # B1
pnpm snapshot-bench --core-path $CORE --agents 10 --rate 10 --history 5000 --duration 120 --restart-every 60 \
  --head-timeout 300 --settle-timeout 180                                                                     # S1
pnpm snapshot-bench --core-path $CORE --agents 25 --rate 10 --history 5000 --duration 120 --restart-every 60 \
  --head-timeout 300 --settle-timeout 180                                                                     # S2
pnpm snapshot-bench --core-path $CORE --agents 50 --rate 5 --history 5000 --duration 120 --restart-every 60 \
  --head-timeout 300 --settle-timeout 300                                                                     # S3
pnpm snapshot-bench --core-path $CORE --mode snapshot --agents 10 --rate 10 --history 5000 --duration 60 \
  --restart-every 30                                                                                         # S1b
pnpm snapshot-bench --core-path $CORE --realization cf --mode snapshot --agents 10 --rate 10 --history 5000 \
  --duration 60 --restart-every 30                                                                           # CF1
```

### Cold boot (B1: one agent, 10 000-event history, load ≈ 100)

| Measure                                     | Without snapshots | With snapshots  |
| ------------------------------------------- | ----------------- | --------------- |
| Fresh agent time-to-head                    | 28.9 s            | 0.28 s          |
| Reference client after the run (≈10 006 ev) | 30.5 s            | 0.32 s          |
| Snapshot size / checkpointer catch-up       | —                 | 2.0 MB / 22.9 s |

Full replay costs ≈ 3 ms per event on this machine, about 100× a snapshot boot
of the same state. This confirms H1 at small scale.

### Multi-agent runs

TTH = restart time-to-head; tail = events applied after the boot position;
lost/kill = unconfirmed events that died with a killed sandbox (p50).

| Run | Agents × ev/s | Mode          | Restarts ok/failed | TTH p50 / p95   | Tail p50 | Init boot p50 | Committed ev/s | Confirmed ev/s | Commit→confirmed p50 / p95 | Push attempts / rejected | Rebases | Lost/kill | Snapshots (size) | Oracle |
| --- | ------------- | ------------- | ------------------ | --------------- | -------- | ------------- | -------------- | -------------- | -------------------------- | ------------------------ | ------- | --------- | ---------------- | ------ |
| S1  | 10 × 10       | baseline      | 10 / 0             | 155 s / 169 s   | 6 412    | 52.4 s        | 54.2           | 4.8            | 30.3 s / 39.1 s            | 351 / 98                 | 11      | 1 252     | —                | pass   |
| S1  | 10 × 10       | snapshot      | 18 / 0             | 26.9 s / 37.8 s | 809      | 0.8 s         | 63.0           | 15.2           | 12.3 s / 28.1 s            | 477 / 88                 | 29      | 327       | 2 (1.1 MB)       | FAIL¹  |
| S1b | 10 × 10       | snapshot      | 20 / 0             | 3.5 s / 8.4 s   | 150      | 0.1 s         | 78.7           | 42.8           | 5.3 s / 21.6 s             | 978 / 233                | 35      | 98        | 10 (1.7 MB)      | pass   |
| S2  | 25 × 10       | baseline      | 25 / 0             | 76.6 s / 93.6 s | 6 327    | 36.6 s        | 111.5          | 14.0           | 26.3 s / 60.9 s            | 716 / 237                | 36      | 688       | —                | pass   |
| S2  | 25 × 10       | snapshot      | 49 / 0             | 4.7 s / 11.6 s  | 69       | 0.4 s         | 220.2          | 25.6           | 13.1 s / 47.9 s            | 1 708 / 874              | 84      | 482       | 24 (2.9 MB)      | pass   |
| S3  | 50 × 5        | baseline      | 0 / 50²            | —               | —        | > 300 s       | 0              | 0              | —                          | 0 / 0                    | 0       | —         | —                | FAIL²  |
| S3  | 50 × 5        | snapshot      | 59 / 23³           | 9.5 s / 52.4 s  | 30       | 1.9 s         | 153.7          | 1.4⁴           | 16.6 s / 30.6 s            | 361 / 31                 | 170     | 257       | 3 (1.1 MB)       | FAIL³  |
| CF1 | 10 × 10       | snapshot (CF) | 18 / 0             | 6.5 s / 13.2 s  | 50       | 0.4 s         | 78.9           | 29.4           | 10.3 s / 24.6 s            | 472 / 190                | 34      | 144       | 7 (1.4 MB)       | pass   |

Load average at run start: S1 76, S1b ≈ 30, S2 44, S3 129, CF1 27.

1. All ten agents converged at the backend head (7 115), but the Node
   checkpointer's live pull stopped at 5 055, shortly after the agents started
   writing, without logging an error. Every later restart therefore installed the
   stale snapshot and replayed ≈ 800–2 000 events, which is why S1 snapshot TTH is
   26.9 s while the same setup in S1b (checkpointer kept following) is 3.5 s.
2. All 50 agents cold-started at once without snapshots. None reached the
   5 000-event head within 300 s (they were at ≈ 1 800–2 000), so they never
   started writing. That is ≈ 330 replayed events per second for all 50 clients
   combined, against one Durable Object on this machine.
3. 23 restarts missed the 300 s head deadline. After the writers stopped, 17
   agents and the checkpointer were stuck at stale upstream heads (5 030–5 419)
   while the backend log was at 5 716. Several held 30–135 pending events that
   were never pushed. They neither advanced nor logged an error during the 300 s
   settle window. The other agents converged at 5 716.
4. 50 agents committed ≈ 18 000 events locally in 120 s, but the backend accepted
   only 716 during the whole run.

### Checkpointer

| Run | Publishes | Export p50 | Publish total p50 | Tail per publish p50 | Snapshot lag p50 / max¹ | Follow latency p50 / p95 | RSS    |
| --- | --------- | ---------- | ----------------- | -------------------- | ----------------------- | ------------------------ | ------ |
| S1b | 10        | 3 ms       | 23 ms             | 413                  | 137 / 2 460             | 6.4 s / 21.0 s²          | 266 MB |
| S2  | 24        | 5 ms       | 37 ms             | 300                  | 104 / 2 504             | 49 s / 115 s²            | 107 MB |
| S3  | 3         | 31 ms      | 894 ms            | 1 672                | 30 / 3 228              | 7.5 s / 9.7 s            | 38 MB  |
| CF1 | 7         | 0 ms       | 914 ms            | 250                  | 50 / 695                | —                        | DO     |

1. The maximum includes the catch-up before the first publish.
2. Measured from the original commit time: it includes the time the event spent
   pending and being rebased at its writer.

### What the numbers say

- **H1 holds.** Restart time-to-head with snapshots is set by the tail, not the
  history: 3.5–6.5 s at 10 agents and 4.7 s at 25 agents, against 77–155 s
  without, which tracks the history length (6 300–6 400 events replayed per
  restart). Under load the snapshot download and install
  (`createStore` 32–150 ms for the Node file-system source) is negligible; the
  remaining seconds are pulling and applying the tail while the Durable Object and
  the host CPUs are saturated.
- **H2 holds for committed throughput.** Agents booting from snapshots spend
  little time unable to work: committed throughput rose from 111 to 220 ev/s at
  25 agents. Confirmed throughput also rose (14 → 26 ev/s at S2, 4.8 → 15–43 at
  S1), mostly because baseline restarts compete with writers for the Durable
  Object.
- **Push contention, not boot, is the ceiling (H3).** At every scale the backend
  confirmed only a fraction of what agents committed, with commit→confirmed
  latencies of 5–30 s (p50). Up to 51% of pushes were rejected
  (`ServerAheadError`), and every rejection costs a pull plus a rebase of the
  pending tail. At 50 writers the backend accepted 716 events in two minutes.
  Snapshots do not change this. The many-writer push/rebase protocol against a
  single Durable Object is the limit for the 100-agent target, and on this
  machine it degrades between 25 and 50 concurrent writers. The 100-agent row
  (S4) was dropped because S3 already showed the collapse.
- **Kills lose work.** An abruptly killed sandbox loses everything still
  unconfirmed: 98–1 252 events per kill (p50), growing with the confirmation
  backlog. `store.syncStatus()` / the session sync state cannot tell an agent
  that its events are durable: the session's "upstream" is the leader. During
  development, using the session upstream head as "confirmed" produced phantom
  heads that were later lost.
- **Liveness under load (H5 partly fails).** Restarted agents converge with
  full-replay and snapshot-boot references whenever the run settles (S1b, S2, CF1
  and S1's agents). But in S1 and S3 some followers silently stopped advancing
  their live pull. Affected were the Node checkpointer in both runs and 17 agents
  in S3. It is probably not specific to snapshot boot: the checkpointer never
  installs a snapshot, and during development a baseline run on the pinned main
  core (4 agents, no snapshots) also ended with three agents stuck 50 events
  behind the backend. A checkpointer that stops following makes every later boot
  pay for a growing tail.
- **Cloudflare realization (CF1)** works end to end: checkpointer Durable Object
  woken by pushes, R2 blobs, Worker manifest/blob routes and
  `makeHttpSnapshotSource`. Its blob download (p50 1.8 s for 1.4 MB through the
  Worker from local R2) and `createStore` (p50 3.6 s) are much slower than the
  Node file-system source (12–18 ms), so time-to-head was 6.5 s versus 3.5 s.
  The checkpointer DO runs in the same local workerd process as the sync DO.

### Limits of this setup

- A shared laptop. Every absolute number includes contention from unrelated
  work. Repeat on an idle machine or in CI before calibrating the trigger's cost
  constants.
- A single run per row, no warmups or alternation. The paired campaign method of
  `benchmark` does not apply yet, because this command is not a Scenario.
- The local Durable Object runs in a single workerd process on the same machine
  as all clients, so backend and clients compete for CPU.
- Time-to-head targets the global head known when the restart began, as
  broadcast by the controller every 250 ms from reports that are up to 1 s old.
- Agents are Stores on the in-memory adapter (no OPFS or Node persistence), so
  "fresh sandbox" is exact, but persisted restarts are not covered.
