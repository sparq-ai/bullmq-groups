# Migrating from BullMQ Pro

The inputs and outputs match BullMQ Pro's groups. Code written for one runs on the other with a change of imports.

> Not affiliated with or endorsed by Taskforce.sh. BullMQ and BullMQ Pro are theirs; this is an independent package built on open-source BullMQ's public API.

## The import swap

```diff
-import { QueuePro, WorkerPro, JobPro } from '@taskforcesh/bullmq-pro';
+import { GroupQueue, GroupWorker, GroupJob } from '@sparq/bullmq-groups';
```

```diff
-import { BullModule } from '@taskforcesh/nestjs-bullmq-pro';
+import { BullModule } from '@sparq/bullmq-groups/nestjs';
```

`GroupQueue`, `GroupWorker` and `GroupJob` are subclasses of BullMQ's `Queue`, `Worker` and `Job`.

## What maps directly

| BullMQ Pro | This package |
| --- | --- |
| `queue.add(name, data, { group: { id, priority, maxSize } })` → `JobPro` | Same, returns a `GroupJob` |
| `queue.addBulk([{ name, data, opts: { group } }])` → `JobPro[]` | Same. `maxSize` is not supported in `addBulk`, as in Pro |
| `job.opts.group`, `job.gid` | Same. `job.opts.group.r` is an internal release nonce |
| `new WorkerPro(name, processor, { group: { concurrency } })` | Same, including the default: unset means no per-group limit |
| `pauseGroup`, `resumeGroup` → `boolean` | Same |
| `setGroupConcurrency`, `getGroupConcurrency`, `removeGroupConcurrency` / `deleteGroupConcurrency` | Same. `getGroupConcurrency` returns `Infinity` for no limit. `ttlSeconds` is **not** supported |
| `getGroups`, `getGroupsCount`, `getGroupStatus`, `getGroupsByStatus`, `getGroupsCountByStatus`, `GroupStatus` | Same shapes. `GroupStatus` adds `Blocked`; `Limited` never occurs |
| `getGroupJobs`, `getGroupJobsCount`, `getGroupsJobsCount`, `getGroupActiveCount`, `getCountsPerPriorityForGroup` | Same |
| `deleteGroup`, `deleteGroups`, `repairMaxedGroup`, `GroupMaxSizeExceededError` | Same |
| `@taskforcesh/nestjs-bullmq-pro` | `@sparq/bullmq-groups/nestjs` |

## What is not supported

| Pro feature | Status |
| --- | --- |
| Group rate limits: `group.limit`, `rateLimitGroup`, `setGroupRateLimit`, `getGroupRateLimitTtl` | **Not supported.** `group.limit` throws at construction |
| Batches, `groupAffinity`, observables, group meta | **Not supported.** Sandboxed processors do work |
| Grouped jobs in flows, repeatable grouped jobs | **Not supported**, rejected at add |
| `ttlSeconds` on `setGroupConcurrency` | **Not supported** |

A per-group rate limit is the most likely next feature — third-party API limits are often per customer. The shape would be a token bucket checked in `release()`, plus a delayed kick when a group is limited.

## What only exists here

- `failurePolicy: 'block'`, with `retryBlockedGroup`, `skipBlockedGroup` and `getBlockedGroups`
- `getStats`, `getLaggingGroups`, `getGroupCounts`, `getStuckJobs`
- the events `stuck`, `blocked`, `repaired`, `skipped` and `rejected`

## Differences in behaviour

### Order through retries

**This is the one that will surprise you, and it favours this package.**

Here, a retrying job keeps its group's slot, so at group concurrency 1 nothing behind it starts. Pro lets later jobs overtake it.

Pro's maintainers have acknowledged this since March 2024 — [pro-support #68](https://github.com/taskforcesh/bullmq-pro-support/issues/68), quoted in [#107](https://github.com/taskforcesh/bullmq-pro-support/issues/107).

If your code *relies* on Pro's behaviour — later jobs proceeding while one retries — moving here will serialise them instead.

### Ids

Group ids are stored as strings; numbers are converted.

Generated job ids are strings like `g<process><time><counter>`, not BullMQ's numeric counter. Ids follow BullMQ's rules: no `:`, not an integer, not one of BullMQ's key names.

### Counts

`getJobCounts()`, `getWaiting()` and BullMQ's other getters count **only jobs BullMQ holds** — that is, released jobs: at most `n` per group.

Jobs waiting in a group are counted by `getGroupsJobsCount()`, and found by `getJob()`, `getGroupJobs()` and `remove()`.

Pro's groups also keep their jobs out of the wait list, so this is not a new behaviour — but any dashboard or alert reading `getJobCounts()` will under-report.

## Migration checklist

1. Swap the imports.
2. Check whether anything depends on Pro's retry-overtaking behaviour.
3. Remove any use of group rate limits, batches, observables or group meta — these throw or are rejected rather than silently ignored.
4. Repoint dashboards and alerts that read `getJobCounts()` at `getGroupsJobsCount()` and `getStats()`.
5. **Move the queue to a new name and drain the old one.** A plain BullMQ worker must never consume a grouped queue — see [Production](./production.md#1-a-plain-bullmq-worker-on-a-grouped-queue).
6. Add alerts on `oldestWaitingMs`, `blockedGroups`, and the `stuck` / `repaired` / `rejected` events.

## Next

- [NestJS](./nestjs.md)
- [Guarantees](./guarantees.md) — where the two packages differ under failure
- [API reference](./api/README.md)
