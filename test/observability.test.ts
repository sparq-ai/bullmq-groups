import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Harness, sleep, waitFor } from './helpers';
import type { StuckJob } from '../src';

test('per-group depth and lag, most-behind groups first, queue-wide stats', async () => {
  const h = new Harness('observe');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    // No worker: each group's first job sits in BullMQ, the rest wait in the group.
    for (const g of ['old', 'mid', 'new']) {
      for (let i = 0; i < 3; i++) await q.add(g, 'n', { i });
      await sleep(40);
    }
    await q.pauseGroup('mid');

    const groups = await q.getGroups();
    assert.deepEqual(groups.map(g => g.id), ['old', 'mid', 'new']);
    assert.deepEqual(groups.map(g => [g.waiting, g.active]), [[2, 1], [2, 1], [2, 1]]);
    assert.ok(groups[0].lagMs > groups[2].lagMs + 50);

    const counts = await q.getGroupCounts('mid');
    assert.equal(counts.waiting, 2);
    assert.equal(counts.active, 1);
    assert.equal(counts.paused, true);
    assert.equal(counts.concurrency, 1);
    assert.ok(counts.headReadyAt! <= Date.now());

    const stats = await q.getStats();
    assert.equal(stats.waitingGroups, 3);
    assert.equal(stats.activeGroups, 3);
    assert.equal(stats.waiting, 6);
    assert.equal(stats.inflight, 3);
    assert.equal(stats.pausedGroups, 1);
    assert.ok(stats.oldestWaitingMs >= groups[0].lagMs - 5);
    assert.equal((await q.bull.getJobCounts()).waiting, 3, 'BullMQ only ever sees one job per group');
  } finally {
    await h.close();
  }
});

test("a job in flight longer than stuckAfterMs raises 'stuck' and shows up in getStuckJobs", async () => {
  const h = new Harness('stuck');
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 100 });
    await q.add('slow-shop', 'n', {});
    const stuck: StuckJob[] = [];
    const w = h.worker(async () => sleep(900), { stuckAfterMs: 300, reconcileIntervalMs: 100, reconcileGraceMs: 100 });
    w.on('stuck', s => stuck.push(s));
    await waitFor(() => stuck.length > 0, "'stuck' event");
    assert.equal(stuck[0].groupId, 'slow-shop');
    assert.equal(stuck[0].state, 'active');
    assert.ok(stuck[0].ageMs >= 300);
    const listed = await q.getStuckJobs(300);
    assert.equal(listed[0]?.groupId, 'slow-shop');
  } finally {
    await h.close();
  }
});
