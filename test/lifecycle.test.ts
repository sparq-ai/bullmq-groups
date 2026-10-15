import { test } from 'node:test';
import assert from 'node:assert/strict';
import { groupIdOf } from '../src';
import { Harness, Recorder, assertFifo, keysMatching, sleep, waitFor } from './helpers';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);

test('idle groups leave no keys behind: only the meta hash survives a full drain', async () => {
  const h = new Harness('cleanup');
  try {
    const q = h.queue({ groupConcurrency: 2 });
    await q.addBulk(range(5).flatMap(seq => range(100).map(g => ({ groupId: `g${g}`, name: 'n', data: { seq }, opts: { priority: seq % 2 } }))));
    assert.ok((await keysMatching(`bull:${h.name}:gq:*`)).length > 100);
    const rec = new Recorder();
    h.worker(async job => {
      rec.start(groupIdOf(job)!, job.data.seq);
      rec.end(groupIdOf(job)!, job.data.seq);
    }, { concurrency: 20 });
    await waitFor(() => rec.done === 500, 'all 500 jobs');
    await waitFor(async () => (await q.getStats()).inflight === 0, 'last acks');
    assert.deepEqual(await keysMatching(`bull:${h.name}:gq:*`), [`bull:${h.name}:gq:meta`]);
  } finally {
    await h.close();
  }
});

test('race between "last job of a group finishes" and "new job arrives": nothing lost, stuck or reordered', async () => {
  const h = new Harness('race');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const rec = new Recorder();
    for (let i = 0; i < 3; i++) {
      h.worker(async job => {
        rec.start(groupIdOf(job)!, job.data.seq);
        if (Math.random() < 0.5) await sleep(Math.random() * 3);
        rec.end(groupIdOf(job)!, job.data.seq);
      }, { concurrency: 10 });
    }
    // Groups keep going idle and getting new work at random moments while their last job finishes.
    const next = new Map<string, number>();
    const added: Promise<unknown>[] = [];
    const deadline = Date.now() + 3_000;
    let total = 0;
    while (Date.now() < deadline) {
      const g = `g${Math.floor(Math.random() * 40)}`;
      const seq = next.get(g) ?? 0;
      next.set(g, seq + 1);
      added.push(q.add(g, 'n', { seq })); // one connection, so scripts run in call order
      total++;
      if (total % 8 === 0) await sleep(Math.random() * 4);
    }
    await Promise.all(added);
    await waitFor(() => rec.done === total, `${total} jobs`, 30_000);
    assert.equal(rec.log.length, total, 'a job ran twice');
    assertFifo(rec, g => range(next.get(g)!));
    for (const [g, max] of rec.maxActive) assert.equal(max, 1, `${g} overlapped`);
    await waitFor(async () => (await q.getStats()).inflight === 0, 'last acks');
    const stats = await q.getStats();
    assert.equal(stats.waiting + stats.waitingGroups + stats.activeGroups, 0);
  } finally {
    await h.close();
  }
});

test('pause and resume one group while others keep running; a running job finishes when its group is paused', async () => {
  const h = new Harness('pause');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.pauseGroup('G');
    await q.addBulk(range(3).flatMap(seq => ['G', 'H'].map(g => ({ groupId: g, name: 'n', data: { seq } }))));
    const rec = new Recorder();
    let gate: () => void = () => undefined;
    const g1Running = new Promise<void>(resolve => (gate = resolve));
    let releaseG1: () => void = () => undefined;
    const g1CanFinish = new Promise<void>(resolve => (releaseG1 = resolve));
    h.worker(async job => {
      const g = groupIdOf(job)!;
      rec.start(g, job.data.seq);
      if (g === 'G' && job.data.seq === 1) {
        gate();
        await g1CanFinish;
      }
      rec.end(g, job.data.seq);
    });

    await waitFor(() => (rec.ends.get('H') ?? []).length === 3, 'H to finish');
    assert.equal(rec.starts.has('G'), false, 'paused group started a job');
    const counts = await q.getGroupCounts('G');
    assert.equal(counts.paused, true);
    assert.equal(counts.waiting, 3);

    await q.resumeGroup('G');
    await waitFor(() => rec.ends.get('G')?.[0] === 0, 'G:0');
    await g1Running;
    await q.pauseGroup('G'); // paused while G:1 runs
    releaseG1();
    await waitFor(() => rec.ends.get('G')?.length === 2, 'G:1 to finish after the pause');
    await sleep(150);
    assert.deepEqual(rec.starts.get('G'), [0, 1], 'G:2 must wait for resume');
    await q.resumeGroup('G');
    await waitFor(() => rec.ends.get('G')?.length === 3, 'G:2');
    assert.deepEqual(rec.starts.get('G'), [0, 1, 2]);
  } finally {
    await h.close();
  }
});

test('delayed jobs join the tail of their group when due', async () => {
  const h = new Harness('delayed');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const t0 = Date.now();
    await q.add('G', 'n', { tag: 'a', sleep: 400 }); // still running when d becomes due
    await q.add('G', 'n', { tag: 'd' }, { delay: 150 });
    const rec: { tag: string; at: number }[] = [];
    h.worker(async job => {
      rec.push({ tag: job.data.tag, at: Date.now() - t0 });
      if (job.data.sleep) await sleep(job.data.sleep);
    }, { delayedPollMs: 50 });
    await sleep(250); // d is due now and waiting behind a
    await q.add('G', 'n', { tag: 'b' });
    await q.add('G', 'n', { tag: 'x' }, { delay: 2_000 });
    await waitFor(() => rec.length === 3, 'a, d, b');
    assert.deepEqual(rec.map(r => r.tag), ['a', 'd', 'b']);
    assert.ok(rec[1].at >= 400, 'd waited for a');
    const stats = await q.getStats();
    assert.equal(stats.delayed, 1, 'x is still delayed');
  } finally {
    await h.close();
  }
});

test('priority inside a group: no priority first (FIFO), then lower numbers first, as in BullMQ', async () => {
  const h = new Harness('priority');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.pauseGroup('G');
    const plan: [string, number][] = [['a', 0], ['b', 0], ['c', 2], ['d', 1], ['e', 0], ['f', 1]];
    for (const [tag, priority] of plan) await q.add('G', 'n', { tag }, { priority });
    const order: string[] = [];
    h.worker(async job => {
      order.push(job.data.tag);
    });
    await q.resumeGroup('G');
    await waitFor(() => order.length === plan.length, 'all jobs');
    assert.deepEqual(order, ['a', 'b', 'e', 'd', 'f', 'c']);
  } finally {
    await h.close();
  }
});

test('adding a job id that is still waiting or running is a no-op; the id is reusable once it finished', async () => {
  const h = new Harness('dedupe');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.add('G', 'n', { v: 0 }); // occupies the group
    assert.equal((await q.add('G', 'n', { v: 1 }, { jobId: 'sync-shop-1', removeOnComplete: true })).status, 'waiting');
    assert.equal((await q.add('G', 'n', { v: 2 }, { jobId: 'sync-shop-1', removeOnComplete: true })).status, 'duplicate');
    const seen: number[] = [];
    h.worker(async job => {
      seen.push(job.data.v);
    });
    await waitFor(() => seen.length === 2, 'two jobs');
    await waitFor(async () => (await q.getStats()).inflight === 0, 'acks');
    assert.equal((await q.add('G', 'n', { v: 3 }, { jobId: 'sync-shop-1', removeOnComplete: true })).status, 'waiting');
    await waitFor(() => seen.length === 3, 'the re-added job');
    assert.deepEqual(seen, [0, 1, 3]);
  } finally {
    await h.close();
  }
});

test('graceful shutdown mid-drain: running jobs finish, the next job of each group is handed over, nothing lost', async () => {
  const h = new Harness('shutdown');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.addBulk(range(4).flatMap(seq => ['G', 'H'].map(g => ({ groupId: g, name: 'n', data: { seq } }))));
    const rec = new Recorder();
    const processor = async (job: any) => {
      rec.start(groupIdOf(job)!, job.data.seq);
      await sleep(150);
      rec.end(groupIdOf(job)!, job.data.seq);
    };
    const old = h.worker(processor, { concurrency: 2 });
    await waitFor(() => rec.starts.size === 2, 'both groups started');
    const closing = old.close(); // deploy: old pod drains...
    h.worker(processor, { concurrency: 2 }); // ...while the new pod starts
    await closing;
    assert.equal(rec.done >= 2, true, 'jobs that were running when close() was called finished');
    await waitFor(() => rec.done === 8, 'all 8 jobs across both workers');
    assertFifo(rec, () => range(4));
    for (const g of ['G', 'H']) assert.equal(rec.maxActive.get(g), 1);
  } finally {
    await h.close();
  }
});
