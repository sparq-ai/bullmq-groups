# @sparq/bullmq-groups

Per-group FIFO and fairness for open-source BullMQ: an alternative to BullMQ Pro's groups. Jobs of one group (a tenant, an account, a user) start in the order they were added, at most `n` at a time, and groups take turns, so one busy group can't starve the others.

- **Pro-shaped API.** The inputs and outputs are the same as BullMQ Pro's groups: `queue.add(name, data, { group: { id } })`, `{ group: { concurrency } }` on the worker, `pauseGroup`, `getGroupJobs` and the rest. Code written for one runs on the other with a change of imports.
- **NestJS module.** `@sparq/bullmq-groups/nestjs` has the same API as `@nestjs/bullmq`: `BullModule.forRoot`, `registerQueue`, `@Processor`, `WorkerHost`, `@InjectQueue`, `@OnWorkerEvent`. An app moves over by changing an import path.

Not affiliated with or endorsed by Taskforce.sh. BullMQ and BullMQ Pro are theirs; this is an independent package built on open-source BullMQ's public API.

**Status: 0.1.x.** The test suite — 77 tests including crash, reconnect and failover scenarios — passes on BullMQ 5.40.0 through 6.3.15. Not yet run in production; if you adopt it, open an issue and tell us how it goes.

## Install

```bash
npm install @sparq/bullmq-groups bullmq
```

BullMQ is a peer dependency (`>=5.40.0 <7`). Node >= 18, Redis 7.

## Quick start

```ts
import { GroupQueue, GroupWorker } from '@sparq/bullmq-groups';

const connection = { url: process.env.REDIS_URL, maxRetriesPerRequest: null };

// Producer. A GroupQueue is a BullMQ Queue; the group goes in the job options, as in BullMQ Pro.
const queue = new GroupQueue('account-sync', { connection });
const job = await queue.add('sync', { accountId }, {
  group: { id: String(accountId) },
  removeOnComplete: true,
  removeOnFail: true,
});
// job is a GroupJob (BullMQ's Job, like Pro's JobPro): job.id, job.data, job.opts.group, job.gid

// Worker. A GroupWorker is a BullMQ Worker; any BullMQ worker option, method and event works.
const worker = new GroupWorker('account-sync', async job => {
  await syncAccount(job.data.accountId); // job.gid === job.opts.group.id
}, {
  connection,
  concurrency: 12,               // BullMQ's: jobs per process, across all groups
  group: { concurrency: 1 },     // Pro's: jobs per group at once, across all workers
});

worker.on('stuck', s => logger.warn('job running too long', s)); // { jobId, groupId, state, ageMs }
```

A job added without `group` is a plain BullMQ job on the same queue, as with Pro.

→ **[Getting started](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/getting-started.md)** · **[NestJS](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/nestjs.md)**

## Documentation

| | |
| --- | --- |
| [Getting started](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/getting-started.md) | Install, first grouped job, the two `concurrency` options |
| [NestJS](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/nestjs.md) | Same API as `@nestjs/bullmq`, by changing an import |
| [Migrating from BullMQ Pro](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/migrating-from-bullmq-pro.md) | Option-by-option mapping, what's unsupported |
| [Concepts](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/concepts.md) | How releases, nonces and the start check work |
| [Guarantees](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/guarantees.md) | Delivery, crash windows, the reconciler, edge cases |
| [Production](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/production.md) | Settings, alerts, known limits |
| [API reference](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/api/README.md) | Every class, method and type |
| [Benchmarks](https://github.com/sparq-ai/bullmq-groups/blob/main/bench/RESULTS.md) | Every number and its setup |

## How it works, briefly

Waiting jobs live in a Redis list per group, not in BullMQ. A job is handed to the queue's ordinary BullMQ wait list only when its group has a free slot.

So BullMQ never holds more than `n` jobs of one group, and its FIFO wait list therefore serves groups round-robin: when a group's job finishes, its next job joins the back of the queue, behind one job from every other busy group.

Every release carries a nonce, and a start check confirms a job is the release its group is waiting for before the processor runs. That is what makes resent commands, late adds and reused ids safe.

BullMQ's locks, retries, backoff, stalled-job recovery, events and metrics all work unchanged — `GroupQueue`, `GroupWorker` and `GroupJob` are subclasses of BullMQ's own classes.

→ [Concepts](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/concepts.md) for the full design, including why three other designs were rejected.

## Compared with BullMQ Pro

| BullMQ Pro | This package |
| --- | --- |
| `QueuePro`, `WorkerPro`, `JobPro` | `GroupQueue`, `GroupWorker`, `GroupJob`: subclasses of BullMQ's `Queue`, `Worker` and `Job` |
| `queue.add(name, data, { group: { id, priority, maxSize } })` returns a `JobPro` | Same, returns a `GroupJob` |
| `new WorkerPro(name, processor, { group: { concurrency } })`; unset means no per-group limit | Same, including the default |
| `pauseGroup`, `resumeGroup`, `setGroupConcurrency`, `getGroups`, `getGroupJobs`, `deleteGroup`, `repairMaxedGroup`, `GroupStatus`, ... | Same shapes. `GroupStatus` adds `Blocked`; `ttlSeconds` is not supported |
| `@taskforcesh/nestjs-bullmq-pro` | `@sparq/bullmq-groups/nestjs` |
| Group rate limits: `group.limit`, `rateLimitGroup`, `setGroupRateLimit` | **Not supported.** `group.limit` throws |
| Batches, `groupAffinity`, observables, group meta | **Not supported** (sandboxed processors do work) |
| Grouped jobs in flows, repeatable grouped jobs | **Not supported**: rejected at add |
| None | Only here: `failurePolicy: 'block'` with `retryBlockedGroup`, `skipBlockedGroup` and `getBlockedGroups`; `getStats`, `getLaggingGroups`, `getGroupCounts`, `getStuckJobs`; the events `stuck`, `blocked`, `repaired`, `skipped` and `rejected` |

**Order through retries** is the notable behavioural difference: here a retrying job keeps its group's slot, so at group concurrency 1 nothing behind it starts. Pro lets later jobs overtake it ([pro-support #68](https://github.com/taskforcesh/bullmq-pro-support/issues/68)).

→ [Migrating from BullMQ Pro](https://github.com/sparq-ai/bullmq-groups/blob/main/docs/migrating-from-bullmq-pro.md) for the full mapping and a migration checklist.

## Benchmarks

Headlines on a laptop (i5-11320H, Redis 7.4 in Docker):

| | Plain BullMQ | This package |
| --- | ---: | ---: |
| 999 one-job groups queued behind one group's 100,000 jobs | 60.3 s | **0.77 s** |
| Jobs of the same group running at once, 3,000 jobs/s | 1,233 | **0** |
| Redis memory per waiting job, 10 per group | 630 B | **387 B** |
| Drain throughput, one worker process, no-op jobs | 19,286/s | 5,394/s |
| Redis CPU per 1,000 jobs, add + run | 74 ms | 196 ms |

The trade is roughly 2.5× the Redis work per job, for ordering and fairness plain BullMQ cannot provide at any setting.

→ [bench/RESULTS.md](https://github.com/sparq-ai/bullmq-groups/blob/main/bench/RESULTS.md) for every number and its setup.

## Running it

```bash
npm install
npm run redis:up            # throwaway Redis 7 on 127.0.0.1:6390, persistence off (npm run redis:down)
npm test                    # 77 tests against BullMQ 5.81.5 and @nestjs/bullmq 10.2.3 (~2 min)
npm run test:min            # the same tests against BullMQ 5.40.0, the oldest supported, in a temp copy
npm run test:v6             # ... against 6.3.15 (with @nestjs/bullmq 11.0.5); any version: npm run test:bullmq -- <version>
npm run docs                # regenerate docs/api from the source
bash bench/all.sh           # whole benchmark matrix (~25 min, quiet machine). FLUSHES the Redis at port 6390.
npx tsx bench/report.ts     # bench/results/*.json -> bench/RESULTS.md
```

```
src/
  group-queue.ts      GroupQueue: BullMQ Queue + Pro's group methods, admin and observability API
  group-worker.ts     GroupWorker: BullMQ Worker + start check + acks + reconciler + delayed promotion
  group-job.ts        GroupJob: BullMQ Job + gid, state and removal of jobs waiting in a group
  nestjs/index.ts     @nestjs/bullmq with GroupQueue/GroupWorker (import path @sparq/bullmq-groups/nestjs)
  lua/*.lua           enqueue, ack, markAdded, promote, control, lease, kick, claim, remove, status, repair (+ shared helpers)
test/                 node:test suites, one per concern, plus edge-cases, pro-api and nestjs
bench/                runner, five approaches, results and report
docs/                 guides and the generated API reference
```

## License

MIT
