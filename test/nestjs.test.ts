// The NestJS module: @nestjs/bullmq's API, with GroupQueue / GroupWorker underneath.
import 'reflect-metadata';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Injectable, Module } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { UnrecoverableError } from 'bullmq';
import type { Queue } from 'bullmq';
import {
  BullModule,
  GroupJob,
  GroupQueue,
  GroupWorker,
  InjectQueue,
  OnWorkerEvent,
  Processor,
  WorkerHost,
  getQueueToken,
} from '../src/nestjs';
import { Recorder, assertFifo, connection, deleteKeys, uniqueName, waitFor } from './helpers';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);

test('NestJS: forRootAsync + registerQueue + @Processor({ group }) run each tenant in order; a plain queue in the same app is untouched', async () => {
  const grouped = uniqueName('nest-grouped');
  const plain = uniqueName('nest-plain');
  const rec = new Recorder();
  const plainRuns: number[] = [];
  const completed: string[] = [];

  @Processor(grouped, { concurrency: 8, group: { concurrency: 1 } })
  class SyncProcessor extends WorkerHost {
    async process(job: GroupJob): Promise<number> {
      rec.start(job.gid!, job.data.seq);
      await new Promise(resolve => setTimeout(resolve, 5));
      rec.end(job.gid!, job.data.seq);
      return job.data.seq;
    }

    @OnWorkerEvent('completed')
    onCompleted(job: GroupJob): void {
      completed.push(job.id!);
    }
  }

  @Processor(plain, { concurrency: 2 })
  class PlainProcessor extends WorkerHost {
    async process(job: GroupJob): Promise<void> {
      plainRuns.push(job.data.i);
    }
  }

  @Injectable()
  class SyncService {
    constructor(@InjectQueue(grouped) readonly queue: GroupQueue) {}

    enqueue(tenant: string, seq: number) {
      return this.queue.add('sync', { seq }, { group: { id: tenant }, removeOnComplete: true });
    }
  }

  @Module({
    imports: [
      BullModule.forRootAsync({ useFactory: () => ({ connection }) }),
      BullModule.registerQueue({ name: grouped }, { name: plain }),
    ],
    providers: [SyncProcessor, PlainProcessor, SyncService],
  })
  class AppModule {}

  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  try {
    const service = app.get(SyncService);
    assert.ok(service.queue instanceof GroupQueue, '@InjectQueue gives a GroupQueue');
    assert.ok(app.get(SyncProcessor).worker instanceof GroupWorker, 'the processor runs on a GroupWorker');
    for (const seq of range(10)) for (const tenant of ['a', 'b', 'c']) await service.enqueue(tenant, seq);
    const plainQueue = app.get<Queue>(getQueueToken(plain));
    await plainQueue.addBulk(range(5).map(i => ({ name: 'p', data: { i } })));
    await waitFor(() => rec.done === 30 && plainRuns.length === 5, 'every job');
    assertFifo(rec, () => range(10));
    for (const [tenant, max] of rec.maxActive) assert.equal(max, 1, `${tenant} ran two jobs at once`);
    await waitFor(() => completed.length === 30, '@OnWorkerEvent(completed) for every grouped job');
    assert.equal((app.get(PlainProcessor).worker as unknown as { groupQueue?: unknown }).groupQueue, undefined, 'the plain queue opened no group state');
  } finally {
    await app.close();
    await deleteKeys(`bull:${grouped}:*`);
    await deleteKeys(`bull:${plain}:*`);
  }
});

test("NestJS: @OnWorkerEvent receives a GroupWorker's own events, and app.close() shuts workers down gracefully", async () => {
  const name = uniqueName('nest-events');
  const blocked: string[] = [];
  const ran: string[] = [];

  @Processor(name, { group: { concurrency: 1, failurePolicy: 'block' } })
  class FlakyProcessor extends WorkerHost {
    async process(job: GroupJob): Promise<void> {
      ran.push(job.data.tag);
      if (job.data.tag === 'bad') throw new UnrecoverableError('bad');
    }

    @OnWorkerEvent('blocked')
    onBlocked(groupId: string): void {
      blocked.push(groupId);
    }
  }

  @Module({
    imports: [BullModule.forRoot({ connection }), BullModule.registerQueue({ name })],
    providers: [FlakyProcessor],
  })
  class AppModule {}

  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  try {
    const queue = app.get<GroupQueue>(getQueueToken(name));
    await queue.add('n', { tag: 'bad' }, { group: { id: 'tenant-1' } });
    await queue.add('n', { tag: 'after' }, { group: { id: 'tenant-1' } });
    await waitFor(() => blocked.length === 1, "'blocked' delivered through @OnWorkerEvent");
    assert.deepEqual(blocked, ['tenant-1']);
    assert.deepEqual(ran, ['bad'], 'the tenant stopped behind its failed job');
    const [{ jobId }] = await queue.getBlockedGroups();
    assert.equal(await queue.skipBlockedGroup('tenant-1'), jobId);
    await waitFor(() => ran.includes('after'), 'the tenant moved on after the skip');
  } finally {
    await app.close();
    await deleteKeys(`bull:${name}:*`);
  }
});
