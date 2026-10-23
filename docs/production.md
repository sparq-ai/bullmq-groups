# Production

Settings, alerts and the mistakes that cost you ordering.

## Settings that matter

### Locks

```ts
new GroupWorker('account-sync', processor, {
  connection,
  group: { concurrency: 1 },
  lockDuration: 120_000,    // 60-120 s
  stalledInterval: 60_000,  // 30-60 s
  maxStalledCount: 1,       // >= 1, with idempotent jobs
});
```

BullMQ renews the lock every half `lockDuration` while a job runs.

With 10 min / 10 min / 0 instead, a pod killed in the middle of a long job **holds its group for 10-20 minutes**. The job is then failed and, with `removeOnFail`, gone; under `failurePolicy: 'block'` the group stays blocked. A rolling deploy that kills pods 30 s after SIGTERM does this to every group with a long job running.

### Deploys

Give pods a termination grace at least as long as the longest job, or accept that the job reruns.

## Three mistakes that break ordering

### 1. A plain BullMQ worker on a grouped queue

**Never let one consume a grouped queue.** It takes released jobs without the start check and without acking them. Consequences:

- the group is held for a grace period
- a paused group's job runs anyway
- a job finished before its confirmation can run twice

Migrating an existing queue? Move it under a **new name** and drain the old one.

With the NestJS module every processor is a `GroupWorker`, so this only matters for processes still using `@nestjs/bullmq` or `bullmq` directly.

### 2. Workers of one queue declaring different `group` options

Settings are written to Redis when a worker starts, so **the last worker to start wins**. Give every worker of a queue the same options.

Workers without `group` options do not write them, so they cannot change them — that part is safe.

### 3. `maxStartedAttempts` with `pauseGroup`

`maxStartedAttempts` counts every start, including a paused group's held job being looked at again. Do not combine the two.

## Observability

```ts
await queue.getStats();
// { waitingGroups, activeGroups, waiting, inflight, delayed,
//   pausedGroups, blockedGroups, oldestWaitingMs }

await queue.getLaggingGroups(0, 19);       // 20 most-behind: { id, lagMs, waiting, active }
await queue.getGroupCounts('account-123'); // { waiting, prioritized, active, concurrency,
                                           //   paused, blockedJobId, headReadyAt }
await queue.getGroups();                   // Pro's: [{ id, status }]
await queue.getGroupsCountByStatus();      // Pro's: { waiting, paused, limited, maxed } (+ blocked)
await queue.getBlockedGroups();            // [{ groupId, jobId, jobIds }]
await queue.getStuckJobs(15 * 60_000);     // in flight longer than that
```

### Alerts worth having

| Signal | Why |
| --- | --- |
| `oldestWaitingMs` above a threshold | a group is falling behind. Paused and blocked groups count too, so exclude them while any exist |
| `blockedGroups > 0` | work has stopped for a group and needs an operator |
| `stuck` event | a job has been running longer than `stuckAfterMs` (default 15 min) |
| `repaired` event | the reconciler found a group that nothing had released — usually lost writes in a failover |
| `rejected` event | BullMQ refused a released job and it was dropped |

### Reading the numbers correctly

- **Events are local.** They are emitted only in the process that saw them — the `GroupWorker`, or the `GroupQueue` that dispatched — not across processes. BullMQ's `QueueEvents` still works for released jobs.
- **`inflight`, `activeGroups` and `active`** count released jobs that have not finished: queued in BullMQ, held while paused, retrying, or running.
- **Lag is the age of each group's *next* job**, not of its oldest job.
- **`getStuckJobs` and `getBlockedGroups` read every record.** Poll them from one process — about 220 ms for 100,000 jobs in flight. `getGroupsByStatus` and `getGroupsCountByStatus` read every waiting group.
- **BullMQ's own metrics and bull-board keep working**, but they only see *released* jobs: at most `n` per group, one per group with no limit. `getJobCounts()`, `getWaiting()` and the rest do not count jobs waiting in a group — use `getGroupsJobsCount()`.

## Blocked groups

Under `failurePolicy: 'block'`, a group stops after a job exhausts its attempts. The failed job is kept in BullMQ (`removeOnFail: false`) so it can be retried.

```ts
const blocked = await queue.getBlockedGroups();
// [{ groupId, jobId, jobIds }]

await queue.retryBlockedGroup('acct-1');  // fresh attempts; returns the job id, or null
await queue.skipBlockedGroup('acct-1');   // give up on it, move the group on
```

`retryBlockedGroup` returns `null` when nothing was blocked, or when the job is gone from BullMQ — the group then moves on by itself.

The policy in force when a job is *released*, or *starts*, is the one that decides.

## Known limits

- **Priority** cannot reorder a job already released into BullMQ. There is no priority across groups.
- **Order at group concurrency `n` > 1** is only kept within a window of `n`. A retrying or crashed job holds only its own slot.
- **Crash recovery** reruns the job only with `maxStalledCount` >= 1, and takes up to about `lockDuration` + 2 × `stalledInterval`.
- **Lock loss and failover** are BullMQ's limits, and Pro shares them. See [Guarantees](./guarantees.md#delivery).
- **A very late resent `add`.** An `add` whose reply was lost is resent by ioredis after the reconnect. If its job already finished in the meantime, the resend adds it again — exactly as a resent `Queue.add` re-creates a removed job in plain BullMQ.
- **No stalled-group signal.** A group held by a job in a long retry backoff looks like a busy group. A per-group "last progress" time and a `getStalledGroups()` would cover it; not built.
- **Paused groups cost Redis work.** A released job of a paused group is re-checked every `recheckMs` (5 s): about 37 Redis commands each time, so 1,000 paused groups cost about 200 checks per second. Each check counts as a BullMQ start (attempts are not used up) and takes a limiter token. After resume the job waits up to `recheckMs` and rejoins the back of the wait list.
- **A job retried from a dashboard** waits for a free slot of its group, so a group with a steady backlog can delay it until the backlog drains. Use `retryBlockedGroup` for blocked jobs.
- **Pro features not built:** group rate limits (`group.limit`, `rateLimitGroup` and friends), batches, observables, group meta, `ttlSeconds` on `setGroupConcurrency`. Third-party API limits are often per customer, so a per-group rate limit is the most likely next feature: a token bucket checked in `release()`, plus a delayed kick when a group is limited.
- **No limit is slower for a single busy group.** Without `group.concurrency`, a group's next job is released when the previous one *starts*, so one group alone drains about 2,357 jobs/s against plain BullMQ's 19,286/s. Many groups at once are not slowed this way.
- **No cross-queue grouping.** Groups are per queue. Two queues writing the same tenant's data can still run at the same time for that tenant; that needs a lock across queues, which neither this package nor Pro provides.
- **Redis Cluster is untested.** Keys share the queue prefix, so a hash-tagged prefix should be enough; `obliterate()` scans one node.
- **NestJS hooks are global.** They apply to every queue in the process, and a queue registered before the module loads stays plain. Flow producers stay BullMQ's. See [NestJS](./nestjs.md).
- **Per-job overhead** is about 2.5× plain BullMQ's Redis CPU, plus one round trip per job start for the start check.

## Cost

From [bench/RESULTS.md](../bench/RESULTS.md), on a laptop (i5-11320H, Redis 7.4 in Docker):

| | Plain BullMQ | This package |
| --- | ---: | ---: |
| Redis CPU per 1,000 jobs, add + run | 74 ms | 196 ms |
| Redis commands per job (skew run) | 23 | 49 |
| Drain throughput, one worker process, no-op jobs | 19,286/s | 5,394/s |
| Redis memory per waiting job, 10 per group | 630 B | **387 B** |
| Redis memory per waiting job, 1 per group | 630 B | 1,087 B |
| Redis connections, 1,000 groups, one worker process | 4 | 7 |
| Jobs of one group running at once, 3,000 jobs/s | 1,233 | **0** |
| 999 one-job groups behind one group's 100,000 jobs | 60.3 s | **0.77 s** |

The trade is roughly 2.5× the Redis work per job, in exchange for ordering and fairness that plain BullMQ cannot provide at any setting.

## Next

- [Guarantees](./guarantees.md) — what survives a crash
- [Releasing](./releasing.md) — how a version reaches npm
- [API reference](./api/README.md)
