# Documentation

Per-group FIFO and fairness for open-source BullMQ.

New here? Start with [Getting started](./getting-started.md).

## Guides

| | |
| --- | --- |
| [Getting started](./getting-started.md) | Install, run a grouped job, understand the two `concurrency` options |
| [NestJS](./nestjs.md) | `@sparq/bullmq-groups/nestjs`, the same API as `@nestjs/bullmq` |
| [Migrating from BullMQ Pro](./migrating-from-bullmq-pro.md) | Option-by-option mapping, what is unsupported, behavioural differences |
| [Concepts](./concepts.md) | How releases, nonces and the start check produce the guarantees |
| [Guarantees](./guarantees.md) | Delivery semantics, crash windows, the reconciler, edge cases |
| [Production](./production.md) | Lock settings, alerts, the three mistakes that break ordering, known limits |
| [Releasing](./releasing.md) | How a version reaches npm |
| [Benchmarks](../bench/RESULTS.md) | Every number and its setup |

## API reference

Generated from the source with TypeDoc. Run `npm run docs` to regenerate.

**[Full reference](./api/README.md)**

| | |
| --- | --- |
| [`GroupQueue`](./api/classes/GroupQueue.md) | Producer and admin API: `add`, group getters, pause/resume, stats, blocked groups |
| [`GroupWorker`](./api/classes/GroupWorker.md) | Consumer: the start check, acks, the reconciler, delayed promotion |
| [`GroupJob`](./api/classes/GroupJob.md) | BullMQ's `Job` plus `gid`, group state and removal while waiting |
| [`GroupStatus`](./api/enumerations/GroupStatus.md) | Group states, including this package's `Blocked` |
| [`GroupQueueOptions`](./api/interfaces/GroupQueueOptions.md) · [`GroupWorkerOptions`](./api/interfaces/GroupWorkerOptions.md) · [`GroupJobsOptions`](./api/interfaces/GroupJobsOptions.md) | Options |
| [`GroupStats`](./api/interfaces/GroupStats.md) · [`GroupLag`](./api/interfaces/GroupLag.md) · [`GroupCounts`](./api/interfaces/GroupCounts.md) | Observability shapes |

## Finding things quickly

| Question | Where |
| --- | --- |
| Why isn't my second job running? | [Getting started → what actually happened](./getting-started.md#what-actually-happened) |
| What happens when a job fails forever? | [Getting started → handling failure](./getting-started.md#handling-failure), [Production → blocked groups](./production.md#blocked-groups) |
| What happens if a worker is `SIGKILL`ed? | [Guarantees → worker crash mid-job](./guarantees.md#worker-crash-mid-job) |
| Why is `getJobCounts()` under-reporting? | [Migrating → counts](./migrating-from-bullmq-pro.md#counts) |
| What should I alert on? | [Production → observability](./production.md#observability) |
| What does this cost me? | [Production → cost](./production.md#cost) |
| Can I use group rate limits? | [Migrating → what is not supported](./migrating-from-bullmq-pro.md#what-is-not-supported) |
| How do I publish a new version? | [Releasing](./releasing.md) |
