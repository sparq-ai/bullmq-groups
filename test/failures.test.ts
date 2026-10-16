import { test } from 'node:test';
import assert from 'node:assert/strict';
import { UnrecoverableError } from 'bullmq';
import { groupIdOf } from '../src';
import { Harness, Recorder, sleep, waitFor, addTo } from './helpers';

test('a retrying job holds its group: nothing behind it starts until it succeeds', async () => {
  const h = new Harness('retry-hol');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const opts = { attempts: 3, backoff: { type: 'fixed', delay: 100 } };
    for (const seq of [1, 2, 3]) await addTo(q, 'G', 'n', { seq }, opts);
    for (const seq of [1, 2, 3, 4, 5]) await addTo(q, 'H', 'n', { seq }, opts);

    const rec = new Recorder();
    let failuresLeft = 2;
    h.worker(async job => {
      const g = groupIdOf(job)!;
      rec.start(g, job.data.seq);
      await sleep(5);
      if (g === 'G' && job.data.seq === 2 && failuresLeft-- > 0) throw new Error('flaky');
      rec.end(g, job.data.seq);
    }, { concurrency: 4 });

    await waitFor(() => rec.done === 8, 'all 8 jobs to succeed');
    assert.deepEqual(rec.starts.get('G'), [1, 2, 2, 2, 3], 'G:3 started before G:2 finished retrying');
    // H keeps moving while G waits out its backoff.
    const log = rec.log;
    assert.ok(log.indexOf('H:5') < log.lastIndexOf('G:2'), `H should finish during G's backoff: ${log.join(' ')}`);
  } finally {
    await h.close();
  }
});

test("failurePolicy 'continue': after the last attempt the group moves on and the job is in BullMQ's failed set", async () => {
  const h = new Harness('fail-continue');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'continue' });
    const { id: badId } = await addTo(q, 'G', 'n', { seq: 1 }, { attempts: 2, backoff: { type: 'fixed', delay: 20 } });
    await addTo(q, 'G', 'n', { seq: 2 });
    const rec = new Recorder();
    h.worker(async job => {
      rec.start('G', job.data.seq);
      if (job.data.seq === 1) throw new Error('always fails');
      rec.end('G', job.data.seq);
    });
    await waitFor(() => rec.done === 1, 'job 2');
    assert.deepEqual(rec.starts.get('G'), [1, 1, 2]);
    assert.equal(await q.getJobState(badId), 'failed');
    await waitFor(async () => (await q.getStats()).inflight === 0, 'group to drain');
  } finally {
    await h.close();
  }
});

test("failurePolicy 'block': the group stops until an operator retries or skips", async () => {
  const h = new Harness('fail-block');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
    const { id: badId } = await addTo(q, 'G', 'n', { seq: 1 }, { attempts: 1, removeOnFail: true });
    await addTo(q, 'G', 'n', { seq: 2 });
    await addTo(q, 'H', 'n', { seq: 1 });

    const rec = new Recorder();
    let shouldFail = true;
    const w = h.worker(async job => {
      rec.start(groupIdOf(job)!, job.data.seq);
      if (groupIdOf(job) === 'G' && job.data.seq === 1 && shouldFail) throw new Error('bad payload');
      rec.end(groupIdOf(job)!, job.data.seq);
    });
    const blocked: string[] = [];
    w.on('blocked', (groupId: string) => blocked.push(groupId));

    await waitFor(async () => (await q.getBlockedGroups()).length === 1, 'G to be blocked');
    await waitFor(() => rec.ends.has('H'), 'H to run');
    assert.deepEqual(blocked, ['G']);
    await sleep(200);
    assert.deepEqual(rec.starts.get('G'), [1], 'G:2 must not start while G is blocked');
    // removeOnFail is overridden under 'block' so the job can still be retried.
    assert.equal(await q.getJobState(badId), 'failed');

    shouldFail = false;
    assert.equal(await q.retryBlockedGroup('G'), badId);
    await waitFor(() => (rec.ends.get('G') ?? []).length === 2, 'G:1 retry and then G:2');
    assert.deepEqual(rec.starts.get('G'), [1, 1, 2]);
    assert.equal((await q.getBlockedGroups()).length, 0);
  } finally {
    await h.close();
  }
});

test("failurePolicy 'block' + skip: the failed job stays failed and the group continues", async () => {
  const h = new Harness('fail-skip');
  try {
    const q = h.queue({ groupConcurrency: 1, failurePolicy: 'block' });
    const { id: badId } = await addTo(q, 'G', 'n', { seq: 1 });
    await addTo(q, 'G', 'n', { seq: 2 });
    const rec = new Recorder();
    h.worker(async job => {
      rec.start('G', job.data.seq);
      if (job.data.seq === 1) throw new UnrecoverableError('do not retry');
      rec.end('G', job.data.seq);
    });
    await waitFor(async () => (await q.getBlockedGroups()).length === 1, 'G to be blocked');
    assert.equal(await q.skipBlockedGroup('G'), badId);
    await waitFor(() => rec.done === 1, 'G:2');
    assert.deepEqual(rec.starts.get('G'), [1, 2]);
    assert.equal(await q.getJobState(badId), 'failed');
  } finally {
    await h.close();
  }
});

test('UnrecoverableError ends the job on the first attempt and frees the group', async () => {
  const h = new Harness('unrecoverable');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addTo(q, 'G', 'n', { seq: 1 }, { attempts: 5 });
    await addTo(q, 'G', 'n', { seq: 2 });
    const rec = new Recorder();
    h.worker(async job => {
      rec.start('G', job.data.seq);
      if (job.data.seq === 1) throw new UnrecoverableError('nope');
      rec.end('G', job.data.seq);
    });
    await waitFor(() => rec.done === 1, 'G:2');
    assert.deepEqual(rec.starts.get('G'), [1, 2]);
  } finally {
    await h.close();
  }
});
