// The rarer failure modes: lost and resent commands, reused ids, refused options, races between the
// reconciler, retries and skips, pause, shutdown and Redis errors. Each test names the failure it guards against.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import IORedis, { ReplyError } from 'ioredis';
import { Queue, UnrecoverableError } from 'bullmq';
import { GroupJob, GroupQueue, GroupWorker, groupIdOf } from '../src';
import type { RejectedJob, SkippedJob, StuckJob } from '../src';
import { Harness, Recorder, connection, sleep, waitFor, addTo, addBulkTo, rawAdd, rawRemove } from './helpers';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);
const nonceOf = async (q: GroupQueue, jobId: string) => JSON.parse((await q.redis.hget(`${q.keyBase}inflight`, jobId))!).r as number;

test('a late or resent ack for an earlier release of a reused job id does not free the newer release', async () => {
  const h = new Harness('ack-nonce');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addTo(q, 'G', 'n', { v: 1 }, { jobId: 'sync-G' });
    const oldNonce = await nonceOf(q, 'sync-G');
    // Release 1 finishes and is removed from BullMQ (removeOnComplete: true in the consumer).
    assert.equal(await q._ack('sync-G', 'completed', oldNonce), 'ok');
    await (await q._bullJob('sync-G'))!.remove();
    // The cron re-adds the same id, and another job of the tenant queues behind it.
    assert.equal((await addTo(q, 'G', 'n', { v: 2 }, { jobId: 'sync-G' })).status, 'waiting');
    await addTo(q, 'G', 'n', { v: 3 });
    // ioredis resends the first release's ack after a reconnect.
    assert.equal(await q._ack('sync-G', 'completed', oldNonce), 'none');
    const counts = await q.getGroupCounts('G');
    assert.deepEqual([counts.active, counts.waiting], [1, 1], 'the newer release still holds the slot');
    assert.equal((await q.getJobCounts()).waiting, 1, 'the job behind it was not released');
  } finally {
    await h.close();
  }
});

test('a late add that re-creates a job that already ran is skipped by the worker, not run a second time', async () => {
  const h = new Harness('ghost');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const runs: number[] = [];
    let firstOpts: any;
    const skipped: SkippedJob[] = [];
    const w = h.worker(async job => {
      if (job.data.seq === 1) firstOpts = { ...job.opts };
      runs.push(job.data.seq);
    });
    w.on('skipped', s => skipped.push(s));
    const completed: string[] = [];
    w.on('completed', job => completed.push(job.id!));
    const { id } = await addTo(q, 'G', 'n', { seq: 1 }, { removeOnComplete: true });
    await waitFor(async () => runs.length === 1 && (await q.getStats()).inflight === 0, 'G:1 done');
    // The producer's original Queue.add, delayed by a network partition, lands now.
    await rawAdd(q, 'n', { seq: 1 }, { ...firstOpts, jobId: id });
    await addTo(q, 'G', 'n', { seq: 2 });
    await waitFor(() => runs.includes(2) && skipped.length === 1, 'G:2 and the skipped copy');
    await sleep(100);
    assert.deepEqual(runs, [1, 2], 'G:1 ran once');
    assert.deepEqual(skipped, [{ jobId: id, groupId: 'G', reason: 'ghost' }]);
    assert.equal(completed.filter(c => c === id).length, 1, "the skipped copy raises no second 'completed'");
    assert.equal(await q._bullJob(id), undefined, 'the skipped copy is removed from BullMQ');
  } finally {
    await h.close();
  }
});

test("a skipped copy whose completion failed does not hide the 'completed' of a later job with its id", async () => {
  const h = new Harness('skip-then-reuse');
  const moveToCompleted = GroupJob.prototype.moveToCompleted;
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const id = 'reused';
    // The copy's completion fails, as BullMQ's "Missing lock" after a lost lock does: BullMQ emits no 'completed' for it.
    let failOnce = true;
    GroupJob.prototype.moveToCompleted = function (this: GroupJob, ...args: Parameters<GroupJob['moveToCompleted']>) {
      if (failOnce && this.id === id) {
        failOnce = false;
        return Promise.reject(new Error(`Missing lock for job ${id}. moveToFinished`));
      }
      return moveToCompleted.apply(this, args);
    };
    const runs: string[] = [];
    const skipped: SkippedJob[] = [];
    const completed: string[] = [];
    const w = h.worker(async job => void runs.push(job.id!));
    w.removeAllListeners('error');
    w.on('error', () => undefined); // the injected Missing lock
    w.on('skipped', s => skipped.push(s));
    w.on('completed', job => completed.push(job.id!));
    await rawAdd(q, 'n', {}, { jobId: id, group: { id: 'G', r: 999 } } as any); // a copy no group released
    await waitFor(async () => skipped.length === 1 && (await q.getJobState(id)) === 'failed', 'the copy skipped, its completion failed');
    await rawRemove(q, id);

    // A real job reuses the id. Its 'completed' frees G at once, not at the reconciler's check 30 s later.
    await q.add('n', {}, { jobId: id, group: { id: 'G' } });
    await q.add('n', {}, { jobId: 'next', group: { id: 'G' } });
    await waitFor(() => runs.includes('next'), "G's next job", 3_000);
    assert.deepEqual(runs, [id, 'next']);
    assert.deepEqual(completed, [id, 'next']);
  } finally {
    GroupJob.prototype.moveToCompleted = moveToCompleted;
    await h.close();
  }
});

test('reconciler: an add that lands between its BullMQ read and its repair is not acked as removed', async () => {
  const h = new Harness('late-confirm');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    const realDispatch = (q as any).dispatch;
    (q as any).dispatch = async () => undefined; // G:1 released, its add slow
    const { id } = await addTo(q, 'G', 'n', { seq: 1 });
    (q as any).dispatch = realDispatch;
    await addTo(q, 'G', 'n', { seq: 2 });
    const nonce = await nonceOf(q, id);
    const w = h.worker(async () => undefined, { autorun: false, reconcileGraceMs: 100 });
    await sleep(150);
    // Whichever way the reconciler reads BullMQ, the original add and its confirmation land right after the read.
    for (const read of ['_bullJob', 'getJobState'] as const) {
      const real = (w.queue as any)[read].bind(w.queue);
      (w.queue as any)[read] = async (jobId: string) => {
        const result = await real(jobId);
        await q._redispatch(id, nonce);
        return result;
      };
    }
    await w.reconcile();
    const counts = await q.getGroupCounts('G');
    assert.deepEqual([counts.active, counts.waiting], [1, 1], 'G:1 still holds the slot');
    assert.equal(await q.getJobState(id), 'waiting');
    assert.equal((await q.getJobCounts()).waiting, 1, 'G:2 was not released next to G:1');
  } finally {
    await h.close();
  }
});

test('generated ids are unique across GroupQueue instances of one process', async () => {
  const h = new Harness('ids');
  try {
    const a = h.queue();
    const b = h.queue();
    await Promise.all([addTo(a, 'warm-a', 'n', {}), addTo(b, 'warm-b', 'n', {})]); // both connected, adds land in the same ms
    const results = await Promise.all(range(600).flatMap(i => [addTo(a, `g${i}`, 'n', {}), addTo(b, `g${i}`, 'n', {})]));
    assert.equal(results.filter(r => r.status === 'waiting').length, 1200);
    assert.equal(new Set(results.map(r => r.id)).size, 1200);
  } finally {
    await h.close();
  }
});

test('delayed jobs of one group due in the same millisecond keep the order they were added in', async () => {
  const h = new Harness('delay-order');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addBulkTo(q, range(40).map(i => ({ groupId: 'G', name: 'n', data: { i }, opts: { delay: 150, jobId: `job-${i}` } })));
    await addBulkTo(q, range(40).map(i => ({ groupId: 'H', name: 'n', data: { i }, opts: { delay: 150 } })));
    const order = new Map<string, number[]>();
    h.worker(async job => {
      const g = groupIdOf(job)!;
      order.set(g, [...(order.get(g) ?? []), job.data.i]);
    }, { delayedPollMs: 50 });
    await waitFor(() => (order.get('G')?.length ?? 0) + (order.get('H')?.length ?? 0) === 80, 'all delayed jobs');
    assert.deepEqual(order.get('G'), range(40), 'custom ids');
    assert.deepEqual(order.get('H'), range(40), 'generated ids');
  } finally {
    await h.close();
  }
});

test('options BullMQ would refuse throw at add time; nothing is stored and the group keeps going', async () => {
  const h = new Harness('invalid-opts');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addTo(q, 'G', 'n', { seq: 1 }); // keeps G busy, so the bad jobs would have had to wait
    await assert.rejects(addTo(q, 'G', 'n', { big: 'x'.repeat(100) }, { sizeLimit: 10 }), /exceeds the limit/);
    // BullMQ refuses a bad backoff jitter from 5.54.0 on; older versions accept it, and so does the package.
    const jitter = { backoff: { type: 'fixed', delay: 1, jitter: 2 } };
    const probe = new Queue(`${q.name}-probe`, { connection });
    const bullRefusesJitter = await probe.add('n', {}, jitter).then(() => false, () => true);
    await probe.obliterate({ force: true });
    await probe.close();
    if (bullRefusesJitter) await assert.rejects(addTo(q, 'G', 'n', {}, jitter), /[Jj]itter/);
    await assert.rejects(addTo(q, 'G', 'n', {}, { jobId: 'events' }), /BullMQ key name/);
    await assert.rejects(addBulkTo(q, [{ groupId: 'G', name: 'n', data: { seq: 2 } }, { groupId: 'G', name: 'n', data: {}, opts: { sizeLimit: 1 } }]), /exceeds/);
    assert.equal((await q.getGroupCounts('G')).waiting, 0, 'a rejected addBulk stores nothing');
    const rec = new Recorder();
    h.worker(async job => {
      rec.start('G', job.data.seq);
      rec.end('G', job.data.seq);
    });
    await addTo(q, 'G', 'n', { seq: 3 });
    await waitFor(() => rec.done === 2, 'G:1 and G:3');
  } finally {
    await h.close();
  }
});

test('a released job BullMQ refuses is dropped from its group with a rejected event; the group moves on', async () => {
  const h = new Harness('refused');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    q.on('error', () => undefined);
    await addTo(q, 'G', 'n', { seq: 1 });
    // A job stored without the add-time check (written straight to Redis) that BullMQ will refuse.
    const payload = JSON.stringify({ n: 'n', d: { big: 'x'.repeat(100) }, o: { sizeLimit: 10 }, g: 'G', t: Date.now() });
    await q.redis.gq_enqueue(`${q.keyBase}meta`, q.keyBase, 1, ['bad-1', 'G', payload, 0, 0, '', 0]);
    await addTo(q, 'G', 'n', { seq: 3 });
    const runs: number[] = [];
    const rejected: RejectedJob[] = [];
    const w = h.worker(async job => {
      runs.push(job.data.seq);
    });
    w.removeAllListeners('error');
    w.on('error', () => undefined);
    w.on('rejected', r => rejected.push(r));
    await waitFor(() => runs.length === 2, 'G:1 and G:3');
    assert.deepEqual(runs, [1, 3]);
    assert.equal(rejected[0]?.jobId, 'bad-1');
    await waitFor(async () => (await q.getStats()).inflight === 0, 'no slot leaked');
  } finally {
    await h.close();
  }
});

test('re-adding an id BullMQ still holds (finished job kept) is a duplicate, and does not stall the group', async () => {
  const h = new Harness('kept-id');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const seen: number[] = [];
    h.worker(async job => {
      seen.push(job.data.v);
    });
    await addTo(q, 'G', 'n', { v: 1 }, { jobId: 'tenant-1' }); // BullMQ keeps completed jobs by default
    await waitFor(async () => seen.length === 1 && (await q.getStats()).inflight === 0, 'v1');
    assert.equal((await addTo(q, 'G', 'n', { v: 2 }, { jobId: 'tenant-1' })).status, 'duplicate');
    const t0 = Date.now();
    await addTo(q, 'G', 'n', { v: 3 });
    await waitFor(() => seen.includes(3), 'v3', 3_000);
    assert.ok(Date.now() - t0 < 2_000, 'the group was not held for a grace period');
    assert.deepEqual(seen, [1, 3]);
  } finally {
    await h.close();
  }
});

test('deduplication: { id } works like BullMQ simple mode: duplicate while pending or running, free again once done', async () => {
  const h = new Harness('dedup');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addTo(q, 'G', 'n', { v: 0 }); // occupies the group
    const first = await addTo(q, 'G', 'sync', { v: 1 }, { deduplication: { id: '101' } });
    const again = await addTo(q, 'G', 'sync', { v: 2 }, { deduplication: { id: '101' } });
    assert.equal(first.status, 'waiting');
    assert.deepEqual(again, { id: first.id, status: 'duplicate' });
    await assert.rejects(addTo(q, 'G', 'sync', {}, { deduplication: { id: '101', ttl: 5 } as any }), /only \{ id \}/);
    const seen: number[] = [];
    h.worker(async job => {
      seen.push(job.data.v);
    });
    await waitFor(async () => seen.length === 2 && (await q.getStats()).inflight === 0, 'both jobs');
    assert.equal((await addTo(q, 'G', 'sync', { v: 3 }, { deduplication: { id: '101' } })).status, 'waiting');
    await waitFor(() => seen.length === 3, 'the re-added job');
    assert.deepEqual(seen, [0, 1, 3]);
  } finally {
    await h.close();
  }
});

test("BullMQ's url connection option reaches the package's own connection too", async () => {
  const h = new Harness('url');
  const q = new GroupQueue(h.name, { connection: { url: `redis://127.0.0.1:${connection.port}` } as any });
  try {
    const added = await Promise.race([addTo(q, 'G', 'n', {}), sleep(3_000).then(() => 'timeout')]);
    assert.notEqual(added, 'timeout', 'add() hung: the group connection went to another Redis');
    assert.equal((await q.getGroupCounts('G')).active, 1);
  } finally {
    await q.close();
    await h.close();
  }
});

test('bad group options throw from the constructor instead of an unhandled rejection later', () => {
  const worker = (opts: object) => () => new GroupWorker('x', null, { connection, autorun: false, ...opts });
  assert.throws(worker({ group: { concurrency: 1.5 } }), /positive integer/);
  assert.throws(worker({ group: { concurrency: 0 } }), /positive integer/);
  assert.throws(worker({ group: { failurePolicy: 'BLOCK' } }), /'continue' or 'block'/);
  assert.throws(worker({ reconcileGraceMs: 0 }), /positive integer/);
  assert.throws(worker({ group: { limit: { max: 10, duration: 1000 } } }), /rate limiting\) is not supported/);
  assert.throws(() => new GroupQueue('x', { connection, groupDefaults: { concurrency: 1.5 } }), /positive integer/);
});

test("failurePolicy is read when a job is released, not when the producer started: 'block' keeps the failed job retryable", async () => {
  const h = new Harness('live-policy');
  try {
    const producer = h.queue({ groupConcurrency: 1 }); // started while the policy was 'continue'
    await addTo(producer, 'warm-up', 'n', {});
    h.queue({ failurePolicy: 'block' }); // an admin script switches it
    await waitFor(async () => (await producer.redis.hget(`${producer.keyBase}meta`, 'failurePolicy')) === 'block', 'policy switch');
    const { id } = await addTo(producer, 'G', 'n', { seq: 1 }, { removeOnFail: true });
    await addTo(producer, 'G', 'n', { seq: 2 });
    const runs: number[] = [];
    let fail = true;
    h.worker(async job => {
      if (groupIdOf(job) !== 'G') return;
      runs.push(job.data.seq);
      if (job.data.seq === 1 && fail) throw new UnrecoverableError('bad');
    });
    await waitFor(async () => (await producer.getBlockedGroups()).length === 1, 'G blocked');
    assert.equal(await producer.getJobState(id), 'failed', 'kept in BullMQ despite removeOnFail: true');
    fail = false;
    assert.equal(await producer.retryBlockedGroup('G'), id);
    await waitFor(() => runs.length >= 3, 'retry, then G:2');
    assert.deepEqual(runs, [1, 1, 2]);
  } finally {
    await h.close();
  }
});

test("'block' at group concurrency 2: both failed jobs block the group until each is retried or skipped", async () => {
  const h = new Harness('block-n2');
  try {
    const q = h.queue({ groupConcurrency: 2, failurePolicy: 'block' });
    for (const seq of [1, 2, 3, 4]) await addTo(q, 'G', 'n', { seq });
    const runs: number[] = [];
    h.worker(async job => {
      runs.push(job.data.seq);
      if (job.data.seq <= 2) throw new UnrecoverableError('bad');
    });
    await waitFor(async () => (await q.getBlockedGroups())[0]?.jobIds.length === 2, 'both failures recorded');
    await q.skipBlockedGroup('G');
    await sleep(300);
    assert.deepEqual(runs.sort(), [1, 2], 'still blocked by the other failed job');
    await q.skipBlockedGroup('G');
    await waitFor(() => runs.length === 4, 'G:3 and G:4');
    await waitFor(async () => (await q.getStats()).inflight === 0, 'no slot leaked');
    assert.equal((await q.getGroupCounts('G')).active, 0);
  } finally {
    await h.close();
  }
});

test("switching failurePolicy from 'block' to 'continue' lets already-blocked groups move on", async () => {
  const h = new Harness('policy-switch');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
    await addTo(q, 'G', 'n', { seq: 1 });
    await addTo(q, 'G', 'n', { seq: 2 });
    const runs: number[] = [];
    h.worker(async job => {
      runs.push(job.data.seq);
      if (job.data.seq === 1) throw new UnrecoverableError('bad');
    }, { reconcileIntervalMs: 100 });
    await waitFor(async () => (await q.getBlockedGroups()).length === 1, 'G blocked');
    await q.redis.hset(`${q.keyBase}meta`, 'failurePolicy', 'continue');
    await waitFor(() => runs.includes(2), 'G:2 after the switch');
    assert.equal((await q.getStats()).blockedGroups, 0);
  } finally {
    await h.close();
  }
});

test('retryBlockedGroup returns null and moves the group on when the failed job is gone from BullMQ', async () => {
  const h = new Harness('retry-gone');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
    q.on('error', () => undefined);
    const { id } = await addTo(q, 'G', 'n', { seq: 1 });
    await addTo(q, 'G', 'n', { seq: 2 });
    const runs: number[] = [];
    h.worker(async job => {
      runs.push(job.data.seq);
      if (job.data.seq === 1) throw new UnrecoverableError('bad');
    });
    await waitFor(async () => (await q.getBlockedGroups()).length === 1, 'G blocked');
    await (await q._bullJob(id))!.remove(); // someone cleaned the failed set
    assert.equal(await q.retryBlockedGroup('G'), null);
    await waitFor(() => runs.includes(2), 'G:2');
  } finally {
    await h.close();
  }
});

test('the same enqueue call arriving twice (ioredis resend) reports the first outcome and hands back the release', async () => {
  const h = new Harness('resend');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const args = ['J1', 'G', JSON.stringify({ n: 'n', d: {}, o: {}, g: 'G', t: 1 }), 0, 0, '', 0];
    const meta = `${q.keyBase}meta`;
    const first = (await q.redis.gq_enqueue(meta, q.keyBase, 1, args)) as any[];
    const second = (await q.redis.gq_enqueue(meta, q.keyBase, 1, args)) as any[];
    assert.deepEqual(first[0], ['waiting']);
    assert.deepEqual(second[0], ['waiting'], 'not reported as a duplicate of itself');
    assert.equal(second[2][1], 'J1', 'the release comes back so the caller can still hand it to BullMQ');
    assert.equal(second[2][2], first[2][2], 'same release nonce');
  } finally {
    await h.close();
  }
});

test('pauseGroup also holds a released job that has not started yet', async () => {
  const h = new Harness('pause-released');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    let unblock: () => void = () => undefined;
    const blocker = new Promise<void>(resolve => (unblock = resolve));
    const starts: string[] = [];
    h.worker(async job => {
      starts.push(groupIdOf(job)!);
      if (groupIdOf(job) === 'H') await blocker;
    }, { concurrency: 1, recheckMs: 100 });
    await addTo(q, 'H', 'n', {}); // holds the only worker slot
    await waitFor(() => starts.includes('H'), 'H running');
    await addTo(q, 'G', 'n', {}); // released into BullMQ, waiting for a worker
    await q.pauseGroup('G');
    unblock();
    await sleep(500);
    assert.deepEqual(starts, ['H'], 'G started after pauseGroup returned');
    await q.resumeGroup('G');
    await waitFor(() => starts.includes('G'), 'G after resume');
  } finally {
    await h.close();
  }
});

test("a failed job retried from a dashboard (job.retry) waits for a free slot instead of overlapping the group's running job", async () => {
  const h = new Harness('dashboard-retry');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const { id } = await addTo(q, 'G', 'n', { seq: 1 });
    await addTo(q, 'G', 'n', { seq: 2 });
    let open: () => void = () => undefined;
    const gate = new Promise<void>(resolve => (open = resolve));
    const rec = new Recorder();
    let failFirst = true;
    h.worker(async job => {
      rec.start('G', job.data.seq);
      try {
        if (job.data.seq === 1 && failFirst) {
          failFirst = false;
          throw new UnrecoverableError('bad');
        }
        if (job.data.seq === 2) await gate;
      } finally {
        rec.end('G', job.data.seq);
      }
    }, { recheckMs: 100 });
    try {
      await waitFor(() => rec.starts.get('G')?.length === 2, 'G:1 failed, G:2 running');
      await (await q._bullJob(id))!.retry('failed'); // bull-board's Retry button
      await sleep(400);
      assert.deepEqual(rec.starts.get('G'), [1, 2], 'the retried job must not start while G:2 runs');
    } finally {
      open();
    }
    await waitFor(() => rec.starts.get('G')?.length === 3, 'the retried job after G:2');
    await waitFor(async () => (await q.getStats()).inflight === 0, 'its slot is freed again');
    assert.equal(rec.maxActive.get('G'), 1);
  } finally {
    await h.close();
  }
});

test('addBulk takes 30,000 jobs in one call (stored 1,000 per script)', async () => {
  const h = new Harness('bulk');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const res = await addBulkTo(q, range(30_000).map(i => ({ groupId: `g${i % 100}`, name: 'n', data: { i } })));
    assert.equal(res.filter(r => r.status === 'waiting').length, 30_000);
    assert.equal((await q.getStats()).waiting, 30_000 - 100);
  } finally {
    await h.close();
  }
});

test('the reconciler checks released jobs without scanning BullMQ lists (no LPOS per check)', async () => {
  const h = new Harness('reconcile-cost');
  const admin = new IORedis(connection);
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    await addBulkTo(q, range(300).map(i => ({ groupId: `g${i}`, name: 'n', data: {} })));
    const w = h.worker(async () => undefined, { autorun: false, reconcileGraceMs: 100 });
    await sleep(150);
    const lpos = async () => Number(/cmdstat_lpos:calls=(\d+)/.exec(await admin.info('commandstats'))?.[1] ?? 0);
    const before = await lpos();
    const summary = await w.reconcile();
    assert.equal(summary.checked, 300);
    assert.equal((await lpos()) - before, 0);
  } finally {
    await admin.quit();
    await h.close();
  }
});

test("'stuck' is raised for a job running too long, not for jobs merely queued in BullMQ", async () => {
  const h = new Harness('stuck-running');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    await addBulkTo(q, range(20).map(i => ({ groupId: `g${i}`, name: 'n', data: {} })));
    const stuck: StuckJob[] = [];
    const w = h.worker(async () => sleep(700), { concurrency: 1, stuckAfterMs: 300, reconcileIntervalMs: 100, reconcileGraceMs: 100 });
    w.on('stuck', s => stuck.push(s));
    await sleep(1_300);
    assert.ok(stuck.length >= 1, 'the running job was reported');
    assert.ok(stuck.every(s => s.state === 'active'), 'only running jobs');
    assert.ok(new Set(stuck.map(s => s.jobId)).size <= 2, `queued jobs were reported: ${stuck.length} events`);
  } finally {
    await h.close();
  }
});

test('a new default group concurrency is applied to every waiting group right away, not by the reconciler', async () => {
  const h = new Harness('conc-change');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addBulkTo(q, range(3).flatMap(seq => range(2_000).map(g => ({ groupId: `g${g}`, name: 'n', data: { seq } }))));
    assert.equal((await q.getJobCounts()).waiting, 2_000);
    h.queue({ groupConcurrency: 2 }); // a deploy raises the default
    // Releases reorder the group index while the repair walks it; every group must still get its second slot.
    await waitFor(async () => (await q.getJobCounts()).waiting === 4_000, 'a second job of every group released', 10_000);
  } finally {
    await h.close();
  }
});

test('with maxStalledCount 0 a crashed job is failed, not re-run, and its group moves on', async () => {
  const h = new Harness('sigkill-msc0');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    for (const seq of [1, 2]) await addTo(q, 'G', 'n', { seq });
    const child = spawn(process.execPath, ['--import', 'tsx', join(__dirname, 'fixtures/hanging-worker.ts'), h.name, String(connection.port)], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    let output = '';
    child.stdout.on('data', chunk => (output += chunk));
    await waitFor(() => output.includes('started 1'), 'child to start G:1', 30_000);
    child.kill('SIGKILL');
    await new Promise(resolve => child.once('exit', resolve));
    const rec = new Recorder();
    h.worker(async job => {
      rec.start('G', job.data.seq);
      rec.end('G', job.data.seq);
    }, { stalledInterval: 300, lockDuration: 1_000, maxStalledCount: 0 });
    await waitFor(() => rec.done === 1, 'G:2', 20_000);
    await sleep(500);
    assert.deepEqual(rec.starts.get('G'), [2], 'G:1 is not re-run; the group moves on');
  } finally {
    await h.close();
  }
});

test('a Redis error while handing jobs to BullMQ (e.g. out of memory) never drops them: the reconciler adds them later', async () => {
  const h = new Harness('oom');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    q.on('error', () => undefined);
    const rejected: RejectedJob[] = [];
    q.on('rejected', r => rejected.push(r));
    const oom = () => Promise.reject(new (ReplyError as any)('OOM command not allowed when used memory > maxmemory.'));
    (q as any)._addReleased = oom;
    await addBulkTo(q, range(30).map(i => ({ groupId: `g${i % 10}`, name: 'n', data: { i } })));
    delete (q as any)._addReleased;
    assert.deepEqual(rejected, [], 'nothing was dropped');
    const runs: number[] = [];
    h.worker(async job => {
      runs.push(job.data.i);
    }, { reconcileIntervalMs: 100, reconcileGraceMs: 100 });
    await waitFor(() => runs.length === 30, 'all 30 jobs once Redis takes writes again');
    assert.equal(new Set(runs).size, 30);
  } finally {
    await h.close();
  }
});

test('retryBlockedGroup and skipBlockedGroup at the same time: the job never reruns after the group moved on', async () => {
  for (let round = 0; round < 3; round++) {
    const h = new Harness(`retry-skip-${round}`);
    try {
      const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
      const { id } = await addTo(q, 'G', 'n', { seq: 1 });
      await addTo(q, 'G', 'n', { seq: 2 });
      const starts: number[] = [];
      let fail = true;
      const processor = async (job: any) => {
        starts.push(job.data.seq);
        if (job.data.seq === 1 && fail) throw new UnrecoverableError('bad');
      };
      const first = h.worker(processor);
      await waitFor(async () => (await q.getBlockedGroups()).length === 1, 'G blocked');
      await first.close();
      fail = false;
      const [retried, skipped] = await Promise.all([q.retryBlockedGroup('G'), q.skipBlockedGroup('G')]);
      assert.equal([retried, skipped].filter(r => r === id).length, 1, 'exactly one operator action wins');
      h.worker(processor);
      await waitFor(() => starts.includes(2), 'G:2');
      await sleep(300);
      assert.ok(starts.lastIndexOf(1) < starts.indexOf(2), `G:1 ran after G:2: ${starts}`);
    } finally {
      await h.close();
    }
  }
});

test('a skip resent after a reconnect does not skip a second blocked job', async () => {
  const h = new Harness('skip-resend');
  try {
    const q = h.queue({ groupConcurrency: 2, failurePolicy: 'block' });
    for (const seq of [1, 2, 3]) await addTo(q, 'G', 'n', { seq });
    h.worker(async job => {
      if (job.data.seq <= 2) throw new UnrecoverableError('bad');
    });
    await waitFor(async () => (await q.getBlockedGroups())[0]?.jobIds.length === 2, 'two blocked jobs');
    const skipped = await q.skipBlockedGroup('G');
    const resent = (await q.redis.gq_control(`${q.keyBase}meta`, q.keyBase, 'skip-blocked', 'G', skipped!)) as any[];
    assert.equal(resent[0], 'none');
    assert.equal((await q.getBlockedGroups())[0]?.jobIds.length, 1, 'the other blocked job is still there');
  } finally {
    await h.close();
  }
});

test('a released job that a plain BullMQ worker ran and removed before the confirmation is not added again', async () => {
  const h = new Harness('plain-consumer');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    const realDispatch = (q as any).dispatch;
    (q as any).dispatch = async () => undefined;
    const { id } = await addTo(q, 'G', 'n', { seq: 1 });
    (q as any).dispatch = realDispatch;
    const nonce = await nonceOf(q, id);
    // An old plain worker took the job and removed it (removeOnComplete) before our confirmation ran.
    await rawAdd(q, 'n', { seq: 1 }, { jobId: id, group: { id: 'G', r: nonce } } as any);
    await rawRemove(q, id);
    await q._markAdded(id, nonce);
    const w = h.worker(async () => undefined, { autorun: false, reconcileGraceMs: 100 });
    await sleep(150);
    const summary = await w.reconcile();
    assert.deepEqual([summary.acked, summary.redispatched], [1, 0], 'freed, not re-added');
    assert.equal(await q.getJobState(id), 'unknown');
  } finally {
    await h.close();
  }
});

test("a failure acked twice under 'block' raises 'blocked' once", async () => {
  const h = new Harness('blocked-once');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
    const { id } = await addTo(q, 'G', 'n', {});
    const nonce = await nonceOf(q, id);
    const events: string[] = [];
    q.on('blocked', (_g: string, jobId: string) => events.push(jobId));
    assert.equal(await q._ack(id, 'failed', nonce), 'blocked');
    assert.equal(await q._ack(id, 'failed', nonce), 'none');
    assert.deepEqual(events, [id]);
  } finally {
    await h.close();
  }
});

test('addBulk that fails after storing some chunks says which jobs were stored', async () => {
  const h = new Harness('bulk-partial');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const real = q.redis.gq_enqueue.bind(q.redis);
    let calls = 0;
    (q.redis as any).gq_enqueue = (...args: any[]) => (++calls === 2 ? Promise.reject(new Error('Connection is closed.')) : real(...(args as [string, string])));
    const err = await addBulkTo(q, range(2_500).map(i => ({ groupId: `g${i}`, name: 'n', data: {} }))).catch(e => e);
    assert.match(err.message, /stored the first 1000 of 2500/);
    assert.equal(err.results.length, 1000);
  } finally {
    await h.close();
  }
});

test("after switching failurePolicy to 'continue', a blocked group with nothing waiting is released too", async () => {
  const h = new Harness('idle-blocked');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
    await addTo(q, 'G', 'n', {});
    h.worker(async () => {
      throw new UnrecoverableError('bad');
    }, { reconcileIntervalMs: 100 });
    await waitFor(async () => (await q.getBlockedGroups()).length === 1, 'G blocked');
    await q.redis.hset(`${q.keyBase}meta`, 'failurePolicy', 'continue');
    await waitFor(async () => (await q.getStats()).blockedGroups === 0 && (await q.getGroupCounts('G')).active === 0, 'G released');
  } finally {
    await h.close();
  }
});

test('a Redis client from another copy of ioredis (pnpm, or a nested install) is accepted, as BullMQ accepts it', async () => {
  // Load ioredis a second time: an app gets its own copy when its ioredis is not deduplicated with ours.
  const cached = Object.keys(require.cache).filter(k => /[\\/]node_modules[\\/]ioredis[\\/]/.test(k));
  const saved = cached.map(k => [k, require.cache[k]] as const);
  cached.forEach(k => delete require.cache[k]);
  let OtherIORedis: typeof IORedis;
  try {
    OtherIORedis = require('ioredis');
  } finally {
    saved.forEach(([k, m]) => (require.cache[k] = m));
  }
  const shared = new OtherIORedis({ ...connection, maxRetriesPerRequest: null });
  assert.equal(shared instanceof IORedis, false, 'a client of a separate ioredis class');
  const name = `other-ioredis-${process.pid}`;
  const q = new GroupQueue(name, { connection: shared });
  const w = new GroupWorker(name, async job => job.gid, { connection: shared, group: { concurrency: 1 } });
  try {
    const ran: string[] = [];
    w.on('completed', (_job, gid) => ran.push(gid));
    await q.add('n', {}, { group: { id: 'G' } });
    await waitFor(() => ran.length === 1, 'the grouped job');
    assert.deepEqual(ran, ['G']);
  } finally {
    await w.close();
    await q.obliterate({ force: true });
    await q.close();
    await shared.quit();
  }
});

test('a worker without group options does not overwrite the group settings other workers wrote', async () => {
  const h = new Harness('no-group-options');
  try {
    const q = h.queue();
    const meta = `${q.keyBase}meta`;
    const rec = new Recorder();
    const processor = async (job: GroupJob) => {
      rec.start(job.gid!, job.data.seq);
      await sleep(20);
      rec.end(job.gid!, job.data.seq);
    };
    h.worker(processor, { group: { concurrency: 1, failurePolicy: 'block' } });
    await waitFor(async () => (await q.redis.hget(meta, 'concurrency')) === '1', 'the declaring worker wrote its settings');
    // A second app, or a NestJS processor without `group`, on the same queue.
    const plain = h.worker(processor, { group: undefined });
    void plain.queue; // its group side starts, as when its probe finds group state
    for (let seq = 1; seq <= 8; seq++) await addTo(q, 'G', 'n', { seq });
    await waitFor(() => rec.done === 8, 'G:1..8');
    assert.equal(rec.maxActive.get('G'), 1, 'G never ran two jobs at once');
    assert.deepEqual(rec.starts.get('G'), [1, 2, 3, 4, 5, 6, 7, 8]);
    assert.equal(await q.redis.hget(meta, 'concurrency'), '1');
    assert.equal(await q.redis.hget(meta, 'failurePolicy'), 'block');
  } finally {
    await h.close();
  }
});

test('a worker without group options still writes its reconcileGraceMs, and nothing else', async () => {
  const h = new Harness('no-group-grace');
  try {
    const q = h.queue();
    const meta = `${q.keyBase}meta`;
    const w = h.worker(async () => undefined, { group: undefined, reconcileGraceMs: 2_000 });
    void w.queue; // its group side starts, as when its probe finds group state
    await waitFor(async () => (await q.redis.hget(meta, 'grace')) === '2000', 'the grace written', 3_000);
    assert.deepEqual(await q.redis.hmget(meta, 'concurrency', 'failurePolicy'), [null, null]);
  } finally {
    await h.close();
  }
});

test('removing a released job frees its group slot at once: queue.remove, job.remove and drain', async () => {
  const h = new Harness('remove-released');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    // No worker: G:1 is released into BullMQ and waits there; G:2 and G:3 wait in the group.
    const one = await addTo(q, 'G', 'n', { seq: 1 });
    const two = await addTo(q, 'G', 'n', { seq: 2 });
    const three = await addTo(q, 'G', 'n', { seq: 3 });
    await waitFor(async () => (await q._bullJob(one.id)) !== undefined, 'G:1 in BullMQ');
    assert.equal(await q.remove(one.id), 1);
    assert.ok(await q._bullJob(two.id), "G:2 released at once, not at the reconciler's next check");
    await (await q.getJob(two.id))!.remove();
    assert.ok(await q._bullJob(three.id), 'G:3 released at once after job.remove()');
    assert.equal((await q.getGroupCounts('G')).active, 1);

    // drain() takes G:3 out of BullMQ with it: every release is due for a reconciler check now.
    await q.drain();
    const scores = (await q.redis.zrange(`${q.keyBase}checks`, 0, -1, 'WITHSCORES')).filter((_, i) => i % 2 === 1);
    assert.ok(scores.length > 0 && scores.every(s => s === '0'), `checks due now, got ${scores}`);
    const w = h.worker(async () => undefined, { autorun: false });
    await w.reconcile();
    assert.equal((await q.getGroupCounts('G')).active, 0, 'G freed on the next reconciler pass');
    assert.equal((await q.getStats()).inflight, 0);
  } finally {
    await h.close();
  }
});

test('a removal that succeeded is reported as one when freeing the group slot fails', async () => {
  const h = new Harness('remove-ack-error');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const errors: string[] = [];
    q.on('error', err => errors.push(err.message));
    // No worker: G:1 and H:1 are released into BullMQ and wait there.
    const g = await addTo(q, 'G', 'n', { seq: 1 });
    const hJob = await addTo(q, 'H', 'n', { seq: 1 });
    await waitFor(async () => !!(await q._bullJob(g.id)) && !!(await q._bullJob(hJob.id)), 'G:1 and H:1 in BullMQ');
    // A Redis hiccup right after BullMQ removed the job. The reconciler frees the slots later.
    (q as unknown as { _ack(): Promise<string> })._ack = () => Promise.reject(new Error('Connection is closed.'));
    assert.equal(await q.remove(g.id), 1);
    await (await q.getJob(hJob.id))!.remove();
    assert.equal(await q._bullJob(g.id), undefined);
    assert.equal(await q._bullJob(hJob.id), undefined);
    assert.deepEqual(errors, ['Connection is closed.', 'Connection is closed.'], "reported through 'error'");
  } finally {
    await h.close();
  }
});

test('close(true) returns at once, even with group bookkeeping that never settles (a Redis outage)', async () => {
  const h = new Harness('force-close');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const w = h.worker(async () => undefined);
    await addTo(q, 'G', 'n', { seq: 1 });
    void w.queue;
    // To close(), a command stuck in ioredis's offline queue is a promise that never settles.
    (w as unknown as { track(p: Promise<unknown>): unknown }).track(new Promise(() => undefined));
    const outcome = await Promise.race([w.close(true).then(() => 'closed'), sleep(3000).then(() => 'hung')]);
    assert.equal(outcome, 'closed');
  } finally {
    await h.close();
  }
});
