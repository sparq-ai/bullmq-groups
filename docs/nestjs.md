# NestJS

`@sparq/bullmq-groups/nestjs` has the same API as `@nestjs/bullmq`. An app moves over by changing an import path.

The module, decorators and injection tokens are `@nestjs/bullmq`'s own — exactly as they are in BullMQ Pro's NestJS package.

## How it works

`@nestjs/bullmq` has two public hooks, `BullModule.queueClass` and `BullModule.workerClass`, which exist for BullMQ Pro. Importing `BullModule` from this package points them at `GroupQueue` and `GroupWorker`, so every queue and processor in the app gets groups.

- A queue that never receives a grouped job **behaves and costs exactly as before**. Its worker opens no extra connection, and its only background work is one `EXISTS` every 5 s.
- `@InjectQueue`, `@QueueEventsListener`, `registerQueueAsync`, `forRootAsync` and bull-board all keep working.

## Setup

```ts
// app.module.ts
import { BullModule } from '@sparq/bullmq-groups/nestjs';

@Module({
  imports: [
    BullModule.forRoot({
      connection: { url: process.env.REDIS_URL, maxRetriesPerRequest: null },
    }),
  ],
})
export class AppModule {}
```

```ts
// account.module.ts
@Module({
  imports: [BullModule.registerQueue({ name: QueueNames.ACCOUNT_SYNC })],
  providers: [AccountSyncProcessor, AccountSyncService],
})
export class AccountModule {}
```

## A processor

```ts
// account-sync.processor.ts
import { GroupJob, OnWorkerEvent, Processor, WorkerHost } from '@sparq/bullmq-groups/nestjs';

@Processor(QueueNames.ACCOUNT_SYNC, {
  concurrency: 12,
  group: { concurrency: 1 },  // one sync per account at a time; accounts take turns
  lockDuration: 120_000,
  stalledInterval: 60_000,
})
export class AccountSyncProcessor extends WorkerHost {
  async process(job: GroupJob<{ accountId: string }>): Promise<void> {
    await this.accountSyncService.sync(job.data.accountId);
  }

  @OnWorkerEvent('blocked')  // this package's own worker events work here too
  onBlocked(groupId: string, jobId: string) {
    this.logger.warn(`account ${groupId} blocked by job ${jobId}`);
  }
}
```

`@OnWorkerEvent` reaches the package's events — `stuck`, `blocked`, `repaired`, `skipped`, `rejected` — as well as all of BullMQ's.

## Producing jobs

```ts
// account-sync.service.ts
import { GroupQueue, InjectQueue } from '@sparq/bullmq-groups/nestjs';

@Injectable()
export class AccountSyncService {
  constructor(
    @InjectQueue(QueueNames.ACCOUNT_SYNC) private readonly queue: GroupQueue,
  ) {}

  addSync(accountId: string) {
    return this.queue.add('sync', { accountId }, {
      group: { id: accountId },
      removeOnComplete: true,
    });
  }
}
```

Type injected queues as `GroupQueue` and jobs as `GroupJob` to reach the group API.

## Moving an existing queue over

1. Import `BullModule`, `Processor`, `WorkerHost`, `InjectQueue` and `OnWorkerEvent` from `@sparq/bullmq-groups/nestjs` instead of `@nestjs/bullmq`. Type injected queues as `GroupQueue` and jobs as `GroupJob`.
2. Add `group: { id }` to the `add()` calls that should be grouped — a tenant or user id. Other adds stay plain jobs.
3. Add `group: { concurrency: n }` to the queue's `@Processor` options, or leave it out for Pro's default (no per-group limit, groups take turns).
4. **Give the queue a new name and let the old one drain.** A plain worker must never take a grouped queue's jobs — see [Production](./production.md#1-a-plain-bullmq-worker-on-a-grouped-queue).
5. Alert on `getStats().oldestWaitingMs` and the `blocked`, `stuck` and `rejected` events — see [Production](./production.md#observability).

## Two things to know

**The hooks are global.** Import `BullModule` from here in **every** module that registers queues. A queue registered before this file loads stays a plain `Queue`.

**`registerFlowProducer` still gives BullMQ's `FlowProducer`.** Grouped jobs in flows are not supported.

## Tested against

- The test suite on Nest 10.4 with `@nestjs/bullmq` 10.2.3 and BullMQ 5.40.0, 5.67.1 and 5.81.5; and with `@nestjs/bullmq` 11.0.5 on BullMQ 6.3.15. Older `@nestjs/bullmq` does not accept BullMQ 6.
- A scratch Nest app built from the packed tarball, on Nest 10.4 with `@nestjs/bullmq` 10.2.3 and on Nest 11.2 with `@nestjs/bullmq` 12.0.0. It runs grouped jobs end to end.

## Next

- [Migrating from BullMQ Pro](./migrating-from-bullmq-pro.md)
- [Production](./production.md)
- [API reference](./api/README.md)
