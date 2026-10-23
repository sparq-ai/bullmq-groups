# Getting started

Install the package, run a grouped job, and understand what happened.

## Requirements

| | |
| --- | --- |
| Node | >= 18 |
| Redis | 7.x (6.2 works; the scripts use no 7-only commands) |
| BullMQ | >= 5.40.0 < 7, as a peer dependency |

BullMQ is a peer dependency, so you install it yourself and keep control of the version.

```bash
npm install @sparq/bullmq-groups bullmq
```

## A Redis to develop against

```bash
docker run -d --rm --name redis -p 127.0.0.1:6379:6379 \
  redis:7-alpine redis-server --save '' --appendonly no
```

Persistence is off deliberately. A background RDB save forks the Redis process, which stalls it for long enough to matter when you are watching latency.

## The smallest working example

```ts
import { GroupQueue, GroupWorker } from '@sparq/bullmq-groups';

const connection = { host: '127.0.0.1', port: 6379, maxRetriesPerRequest: null };

const queue = new GroupQueue('account-sync', { connection });

await queue.add('sync', { accountId: 'acct-1' }, { group: { id: 'acct-1' } });
await queue.add('sync', { accountId: 'acct-1' }, { group: { id: 'acct-1' } });
await queue.add('sync', { accountId: 'acct-2' }, { group: { id: 'acct-2' } });

const worker = new GroupWorker('account-sync', async job => {
  console.log(job.gid, job.data);
}, {
  connection,
  concurrency: 12,            // BullMQ's: jobs per process, across all groups
  group: { concurrency: 1 },  // this package's: jobs per group at once, across all workers
});
```

`maxRetriesPerRequest: null` is BullMQ's requirement for a worker connection, not ours.

Both accounts start at once. `acct-1`'s second job does not start until its first finishes.

## What actually happened

The two `concurrency` options do different jobs, and confusing them is the most common early mistake.

| Option | Scope | Meaning |
| --- | --- | --- |
| `concurrency: 12` | one worker process | how many jobs this process runs at once, across every group |
| `group: { concurrency: 1 }` | the whole queue, all workers | how many jobs of **one group** may run at once |

A grouped job does not go into BullMQ's wait list when you add it. It goes into a Redis list belonging to its group. It is handed to BullMQ only when its group has a free slot.

That one difference produces both guarantees:

- **FIFO within a group** — at `group.concurrency: 1`, BullMQ never holds more than one job of a group, so there is nothing for a later job to overtake.
- **Fairness across groups** — when a group's job finishes, its next job joins the *back* of BullMQ's wait list, behind one job from every other busy group. Groups take turns.

A job added **without** `group` is an ordinary BullMQ job on the same queue, unaffected by any of this.

## Leaving out `group.concurrency`

Omitting it matches BullMQ Pro's default, and it is a different mode rather than a bigger number:

```ts
const worker = new GroupWorker('account-sync', processor, { connection });
```

A group now runs any number of jobs at once. They **start** in order and groups still take turns, but nothing serialises them. Use it when you want fairness without head-of-line blocking.

One trade-off worth knowing up front: in this mode a group's next job is released when the previous one *starts*, so a single busy group on its own drains at roughly 2,357 jobs/s against plain BullMQ's 19,286/s. Many groups running at once are not slowed this way. See [Benchmarks](../bench/RESULTS.md).

## Handling failure

A failing job keeps its group's slot through every attempt and backoff. At `group.concurrency: 1` nothing behind it starts — head-of-line blocking, on purpose, because that is what ordering means.

After the last attempt, `failurePolicy` decides:

```ts
new GroupWorker('account-sync', processor, {
  connection,
  group: { concurrency: 1, failurePolicy: 'block' },
});
```

| Policy | After the final attempt |
| --- | --- |
| `continue` (default) | the job goes to BullMQ's failed set, the group moves on |
| `block` | the group stops until `retryBlockedGroup()` or `skipBlockedGroup()` |

`block` is for work where skipping one job corrupts everything after it. It needs an operator path — see [Production](./production.md).

## Watching it

```ts
const stats = await queue.getStats();
// { waitingGroups, activeGroups, waiting, inflight, delayed,
//   pausedGroups, blockedGroups, oldestWaitingMs }

await queue.getLaggingGroups(0, 19);  // 20 most-behind groups
await queue.getGroupCounts('acct-1');
```

`oldestWaitingMs` is the single most useful alert: it is the age of the oldest group's *next* job, so it rises when any group falls behind.

## Shutting down

```ts
await worker.close();
```

Waits for running jobs **and** their group bookkeeping, so each group's next job is already in BullMQ for the next pod to pick up.

`worker.close(true)` waits for neither and drops the group connection immediately, so it returns even during a Redis outage. Another worker's reconciler finishes what it left behind.

## Next

- [NestJS](./nestjs.md) — the same thing through `@nestjs/bullmq`
- [Migrating from BullMQ Pro](./migrating-from-bullmq-pro.md) — an option-by-option mapping
- [Concepts](./concepts.md) — how releases, nonces and the start check work
- [Guarantees](./guarantees.md) — what survives a crash, and what does not
- [Production](./production.md) — lock settings, alerts, the mistakes that cost you ordering
- [API reference](./api/README.md) — every class, method and type
