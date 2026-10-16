import { test } from 'node:test';
import assert from 'node:assert/strict';
import { groupIdOf } from '../src';
import { Harness, Recorder, assertFifo, assertWindowedFifo, sleep, waitFor, addBulkTo } from './helpers';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);

test('FIFO per group, one job per group at a time, across three workers', async () => {
  const h = new Harness('fifo');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const groups = range(50).map(i => `tenant-${i}`);
    const jobs = range(20).flatMap(seq => groups.map(g => ({ groupId: g, name: 'sync', data: { seq } })));
    await addBulkTo(q, jobs);

    const rec = new Recorder();
    const processor = async (job: any) => {
      const g = groupIdOf(job)!;
      rec.start(g, job.data.seq);
      await sleep(Math.random() * 4);
      rec.end(g, job.data.seq);
    };
    for (let i = 0; i < 3; i++) h.worker(processor, { concurrency: 8 });

    await waitFor(() => rec.done === 1000, 'all 1000 jobs');
    assertFifo(rec, () => range(20));
    for (const g of groups) assert.equal(rec.maxActive.get(g), 1, `${g} ran two jobs at once`);
    assert.equal(rec.log.length, 1000, 'a job ran more than once');
    // The last acks run just after the last processors return.
    await waitFor(async () => {
      const stats = await q.getStats();
      return stats.waiting + stats.inflight + stats.waitingGroups + stats.activeGroups === 0;
    }, 'every group slot freed');
  } finally {
    await h.close();
  }
});

test('group concurrency n: up to n jobs of a group run at once, released in order (start order only within the window)', async () => {
  const h = new Harness('conc');
  try {
    const q = h.queue({ groupConcurrency: 3 });
    const groups = range(10).map(i => `g${i}`);
    await addBulkTo(q, range(30).flatMap(seq => groups.map(g => ({ groupId: g, name: 'n', data: { seq } }))));

    const rec = new Recorder();
    h.worker(async job => {
      rec.start(groupIdOf(job)!, job.data.seq);
      await sleep(15);
      rec.end(groupIdOf(job)!, job.data.seq);
    }, { concurrency: 40 });

    await waitFor(() => rec.done === 300, 'all 300 jobs');
    assertWindowedFifo(rec, 3);
    for (const g of groups) assert.equal(rec.maxActive.get(g), 3, `${g} max active`);
  } finally {
    await h.close();
  }
});

test('per-group concurrency override applies to one group only', async () => {
  const h = new Harness('override');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.setGroupConcurrency('big', 4);
    await addBulkTo(q, range(20).flatMap(seq => ['big', 'small'].map(g => ({ groupId: g, name: 'n', data: { seq } }))));
    const rec = new Recorder();
    h.worker(async job => {
      rec.start(groupIdOf(job)!, job.data.seq);
      await sleep(15);
      rec.end(groupIdOf(job)!, job.data.seq);
    }, { concurrency: 10 });
    await waitFor(() => rec.done === 40, 'all 40 jobs');
    assert.equal(rec.maxActive.get('big'), 4);
    assert.equal(rec.maxActive.get('small'), 1);
    assert.equal((await q.getGroupCounts('big')).concurrency, 4);
  } finally {
    await h.close();
  }
});

test('fairness under skew: one group with a 2,000 job backlog does not starve 200 one-job groups', async () => {
  const h = new Harness('skew');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await addBulkTo(q, range(2000).map(seq => ({ groupId: 'hot', name: 'n', data: { seq } })));
    await addBulkTo(q, range(200).map(i => ({ groupId: `small-${i}`, name: 'n', data: { seq: 0 } })));

    const rec = new Recorder();
    h.worker(async job => {
      rec.start(groupIdOf(job)!, job.data.seq);
      await sleep(2);
      rec.end(groupIdOf(job)!, job.data.seq);
    }, { concurrency: 8 });

    await waitFor(() => range(200).every(i => rec.ends.has(`small-${i}`)), 'every small group');
    const hotDone = rec.ends.get('hot')?.length ?? 0;
    // The hot group may only run one job at a time, so the small groups get the other 7 slots.
    assert.ok(hotDone < 120, `hot group finished ${hotDone} jobs before the small groups were served`);
    assertFifo(rec, g => (g === 'hot' ? range(rec.starts.get('hot')!.length) : [0]));
  } finally {
    await h.close();
  }
});
