import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { Worker } from 'bullmq';
import { groupIdOf } from '../src';
import { Harness, Recorder, connection, sleep, waitFor, addTo, rawRemove } from './helpers';

const fastReconcile = { reconcileGraceMs: 200, reconcileIntervalMs: 100 };

test('worker killed mid-job (SIGKILL): the stalled job re-runs before anything behind it in its group', async () => {
  const h = new Harness('sigkill');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    for (const seq of [1, 2, 3]) await addTo(q, 'G', 'n', { seq });

    const child = spawn(process.execPath, ['--import', 'tsx', join(__dirname, 'fixtures/hanging-worker.ts'), h.name, String(connection.port)], {
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    let output = '';
    child.stdout.on('data', chunk => (output += chunk));
    await waitFor(() => output.includes('started 1'), 'child to start G:1', 30_000);
    child.kill('SIGKILL');
    await new Promise(resolve => child.once('exit', resolve));

    // A healthy worker takes over. BullMQ's stalled checker puts G:1 back; G:2 must still wait for it.
    const rec = new Recorder();
    const attempts: number[] = [];
    h.worker(async job => {
      rec.start('G', job.data.seq);
      if (job.data.seq === 1) attempts.push(job.attemptsStarted);
      await sleep(10);
      rec.end('G', job.data.seq);
    }, { concurrency: 4, stalledInterval: 300, lockDuration: 1_000 });

    await waitFor(() => rec.done === 3, 'G:1..3 after recovery', 20_000);
    assert.deepEqual(rec.starts.get('G'), [1, 2, 3]);
    assert.equal(rec.maxActive.get('G'), 1);
    assert.ok(attempts[0] >= 2, `G:1 should be a second start, got attemptsStarted=${attempts[0]}`);
  } finally {
    await h.close();
  }
});

test('producer dies after releasing a job but before BullMQ got it: the reconciler adds it, order kept', async () => {
  const h = new Harness('lost-add');
  try {
    const crashing = h.queue({ groupConcurrency: 1, reconcileGraceMs: 200 });
    (crashing as any).dispatch = async () => undefined; // simulate dying right after the enqueue script
    const { id } = await addTo(crashing, 'G', 'n', { seq: 1 });
    assert.equal(await crashing.getJobState(id), 'unknown');

    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 200 });
    await addTo(q, 'G', 'n', { seq: 2 }); // waits behind the lost job, not ahead of it

    const rec = new Recorder();
    const w = h.worker(async job => {
      rec.start('G', job.data.seq);
      rec.end('G', job.data.seq);
    }, fastReconcile);
    let redispatched = 0;
    const original = w.reconcile.bind(w);
    w.reconcile = async () => {
      const r = await original();
      redispatched += r.redispatched;
      return r;
    };
    await waitFor(() => rec.done === 2, 'both jobs');
    assert.deepEqual(rec.starts.get('G'), [1, 2]);
    assert.ok(redispatched >= 1);
  } finally {
    await h.close();
  }
});

test('worker dies after BullMQ completed the job but before the group was told: the reconciler frees the group', async () => {
  for (const removeOnComplete of [false, true]) {
    const h = new Harness(`lost-ack-${removeOnComplete}`);
    try {
      const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 200 });
      await addTo(q, 'G', 'n', { seq: 1 }, { removeOnComplete });
      await addTo(q, 'G', 'n', { seq: 2 }, { removeOnComplete });

      // A plain BullMQ worker finishes G:1 without any group bookkeeping, like a process that died after moveToFinished.
      const plain = new Worker(h.name, async () => undefined, { connection });
      await waitFor(async () => (await q.getCompletedCount()) === 1 || (await q.getJobCounts()).waiting === 0, 'G:1 to finish');
      await plain.close();
      assert.equal((await q.getGroupCounts('G')).waiting, 1, 'G:2 is still held');

      const rec = new Recorder();
      h.worker(async job => {
        rec.start('G', job.data.seq);
        rec.end('G', job.data.seq);
      }, fastReconcile);
      await waitFor(() => rec.done === 1, `G:2 (removeOnComplete=${removeOnComplete})`);
      assert.deepEqual(rec.starts.get('G'), [2]);
    } finally {
      await h.close();
    }
  }
});

test('lost confirmation after a successful add: the reconciler confirms and the job runs once', async () => {
  const h = new Harness('lost-confirm');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    const realMarkAdded = q.redis.gq_markAdded.bind(q.redis);
    (q.redis as any).gq_markAdded = async () => 1; // the add reached BullMQ, the confirmation did not
    const { id } = await addTo(q, 'G', 'n', { seq: 1 });
    (q.redis as any).gq_markAdded = realMarkAdded;
    assert.equal(await q.getJobState(id), 'waiting');

    const w = h.worker(async () => undefined, { autorun: false, reconcileGraceMs: 100 });
    await sleep(150);
    const summary = await w.reconcile();
    assert.equal(summary.confirmed, 1);
    const rec = JSON.parse((await q.redis.hget(`${q.keyBase}inflight`, id))!);
    assert.equal(rec.s, 'A');
    assert.equal(await q.redis.hexists(`${q.keyBase}data`, id), 0, 'payload copy dropped once BullMQ has the job');
  } finally {
    await h.close();
  }
});

test('a released job removed by hand (e.g. from a dashboard) frees its group', async () => {
  const h = new Harness('manual-remove');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    const { id: first } = await addTo(q, 'G', 'n', { seq: 1 });
    const { id: second } = await addTo(q, 'G', 'n', { seq: 2 });
    await rawRemove(q, first); // as bull-board removes it: through BullMQ, not this package

    const w = h.worker(async () => undefined, { autorun: false, reconcileGraceMs: 100 });
    await sleep(150);
    const summary = await w.reconcile();
    assert.equal(summary.acked, 1);
    assert.equal(await q.getJobState(second), 'waiting', 'G:2 released into BullMQ');
  } finally {
    await h.close();
  }
});

test('changing the default group concurrency is picked up by the reconciler for groups already waiting', async () => {
  const h = new Harness('kick');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const w = h.worker(async () => undefined, { autorun: false });
    await (w.queue as any).ensureConfig(); // the worker has written its group settings
    for (const seq of [1, 2, 3, 4]) await addTo(q, 'G', 'n', { seq });
    assert.equal((await q.getJobCounts()).waiting, 1);
    await q.redis.hset(`${q.keyBase}meta`, 'concurrency', '3'); // another deployment raised the default
    const summary = await w.reconcile();
    assert.deepEqual(summary.repaired, ['G']);
    assert.equal((await q.getJobCounts()).waiting, 3);
  } finally {
    await h.close();
  }
});
