// The BullMQ Pro-shaped API: the same inputs and outputs as QueuePro / WorkerPro / JobPro.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { GroupJob, GroupMaxSizeExceededError, GroupStatus } from '../src';
import type { GroupWorker } from '../src';
import { Harness, Recorder, keysMatching, sleep, waitFor } from './helpers';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);

test('add() and addBulk() take { group: { id } } and return jobs, as QueuePro does; jobs without a group are plain BullMQ jobs', async () => {
  const h = new Harness('pro-add');
  try {
    const q = h.queue();
    const job = await q.add('sync', { tenant: 1 }, { group: { id: 'tenant-1' }, attempts: 3 });
    assert.ok(job instanceof GroupJob);
    assert.equal(job.name, 'sync');
    assert.deepEqual(job.data, { tenant: 1 });
    assert.deepEqual(job.opts.group, { id: 'tenant-1' });
    assert.equal(job.opts.attempts, 3);
    assert.equal(job.gid, 'tenant-1');
    assert.equal(typeof job.id, 'string');
    assert.ok(job.timestamp > 0);

    const numeric = await q.add('sync', {}, { group: { id: 7 as unknown as string } });
    assert.equal(numeric.gid, '7', 'numeric group ids are accepted, as in Pro, and stored as strings');

    const jobs = await q.addBulk([
      { name: 'a', data: { i: 0 }, opts: { group: { id: 'tenant-2' } } },
      { name: 'plain', data: { i: 1 } },
      { name: 'b', data: { i: 2 }, opts: { group: { id: 'tenant-2' } } },
    ]);
    assert.deepEqual(jobs.map(j => j.name), ['a', 'plain', 'b'], 'results come back in input order');
    assert.deepEqual(jobs.map(j => j.gid), ['tenant-2', undefined, 'tenant-2']);
    assert.equal(await jobs[1].getState(), 'waiting', 'the plain job went straight to BullMQ');
    assert.equal(await jobs[2].getState(), 'waiting', 'a job waiting in its group reports waiting');
    assert.ok(await q.getJob(jobs[2].id!), 'getJob finds a job still waiting in its group');

    const seen: { name: string; group: unknown; gid: unknown; isGroupJob: boolean }[] = [];
    h.worker(async j => {
      seen.push({ name: j.name, group: j.opts.group && { id: j.opts.group.id }, gid: j.gid, isGroupJob: j instanceof GroupJob });
    });
    await waitFor(() => seen.length === 5, 'all five jobs');
    const plain = seen.find(s => s.name === 'plain')!;
    assert.deepEqual(plain, { name: 'plain', group: undefined, gid: undefined, isGroupJob: true });
    assert.deepEqual(seen.find(s => s.name === 'b'), { name: 'b', group: { id: 'tenant-2' }, gid: 'tenant-2', isGroupJob: true });
    assert.ok(seen.findIndex(s => s.name === 'a') < seen.findIndex(s => s.name === 'b'), 'FIFO inside tenant-2');
  } finally {
    await h.close();
  }
});

test('without group.concurrency (the Pro default) a group runs jobs in parallel, starts them in order, and takes turns with other groups', async () => {
  const h = new Harness('pro-unlimited');
  try {
    const q = h.queue();
    const rec = new Recorder();
    const order: string[] = [];
    let w!: GroupWorker;
    const ready = new Promise<void>(resolve => {
      w = h.worker(
        async job => {
          rec.start(job.data.g, job.data.seq);
          order.push(job.data.g);
          await sleep(job.data.g === 'hot' ? 40 : 5);
          rec.end(job.data.g, job.data.seq);
        },
        { concurrency: 8, group: {}, autorun: false },
      );
      resolve();
    });
    await ready;
    await q.addBulk(range(60).map(seq => ({ name: 'n', data: { g: 'hot', seq }, opts: { group: { id: 'hot' } } })));
    await q.addBulk(range(30).map(i => ({ name: 'n', data: { g: `small-${i}`, seq: 0 }, opts: { group: { id: `small-${i}` } } })));
    assert.equal((await q.getJobCounts()).waiting, 31, 'one job per group in BullMQ: its turn');
    w.run().catch(() => undefined);
    await waitFor(() => rec.done === 90, 'all jobs', 30_000);
    assert.ok(rec.maxActive.get('hot')! > 1, `hot ran ${rec.maxActive.get('hot')} at once: no per-group limit`);
    assert.deepEqual(rec.starts.get('hot'), range(60), 'hot started its jobs in order');
    const lastSmall = Math.max(...range(30).map(i => order.indexOf(`small-${i}`)));
    assert.ok(lastSmall < 50, `every small tenant started within the first 50 starts (last at ${lastSmall}), not behind hot's 60`);
    assert.equal(await q.getGroupConcurrency('hot'), Infinity);
  } finally {
    await h.close();
  }
});

test('group.concurrency on the worker limits each group across workers, as WorkerPro does', async () => {
  const h = new Harness('pro-concurrency');
  try {
    const q = h.queue();
    const rec = new Recorder();
    const proc = async (job: any) => {
      rec.start(job.data.g, job.data.seq);
      await sleep(15);
      rec.end(job.data.g, job.data.seq);
    };
    h.worker(proc, { concurrency: 10, group: { concurrency: 2 } });
    h.worker(proc, { concurrency: 10, group: { concurrency: 2 } });
    await q.addBulk(range(20).flatMap(seq => ['A', 'B'].map(g => ({ name: 'n', data: { g, seq }, opts: { group: { id: g } } }))));
    await waitFor(() => rec.done === 40, 'all jobs');
    assert.equal(rec.maxActive.get('A'), 2);
    assert.equal(rec.maxActive.get('B'), 2);
    assert.equal(await q.getGroupConcurrency('A'), 2);
  } finally {
    await h.close();
  }
});

test('group.maxSize refuses the add with GroupMaxSizeExceededError, and is not supported by addBulk, as in Pro', async () => {
  const h = new Harness('pro-maxsize');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    // No worker: the first job is released to BullMQ, the next three wait in the group.
    for (const i of range(4)) await q.add('n', { i }, { group: { id: 'G', maxSize: 3 } });
    const err = await q.add('n', { i: 4 }, { group: { id: 'G', maxSize: 3 } }).catch(e => e);
    assert.ok(err instanceof GroupMaxSizeExceededError);
    assert.equal(err.groupId, 'G');
    assert.equal(await q.getGroupJobsCount('G'), 3, 'nothing was stored');
    await q.add('n', {}, { group: { id: 'H', maxSize: 3 } });
    await assert.rejects(q.addBulk([{ name: 'n', data: {}, opts: { group: { id: 'G', maxSize: 9 } } }]), /not supported by addBulk/);
  } finally {
    await h.close();
  }
});

test('group.priority orders a group as in Pro (unprioritized first, then 1, 2 ...); getCountsPerPriorityForGroup', async () => {
  const h = new Harness('pro-priority');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.pauseGroup('G');
    const plan: [string, number | undefined][] = [['p5', 5], ['a', undefined], ['p1', 1], ['b', undefined], ['p1b', 1]];
    for (const [tag, priority] of plan) await q.add('n', { tag }, { group: { id: 'G', priority } });
    assert.deepEqual(await q.getCountsPerPriorityForGroup('G', [0, 1, 5, 7]), { '0': 2, '1': 2, '5': 1, '7': 0 });
    const pending = await q.getGroupJobs('G');
    assert.deepEqual(pending.map(j => j.data.tag), ['a', 'b', 'p1', 'p1b', 'p5'], 'getGroupJobs lists them in run order');
    assert.deepEqual(pending.map(j => j.opts.group!.priority), [undefined, undefined, 1, 1, 5]);
    assert.equal(await pending[4].getState(), 'prioritized');
    const ran: string[] = [];
    h.worker(async job => {
      ran.push(job.data.tag);
    });
    await q.resumeGroup('G');
    await waitFor(() => ran.length === 5, 'all five');
    assert.deepEqual(ran, ['a', 'b', 'p1', 'p1b', 'p5']);
    await assert.rejects(q.add('n', {}, { group: { id: 'G' }, priority: 3 }), /use group.priority/);
  } finally {
    await h.close();
  }
});

test("pauseGroup / resumeGroup return Pro's booleans; a group can be paused before it has jobs", async () => {
  const h = new Harness('pro-pause');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    assert.equal(await q.resumeGroup('G'), false, 'not paused');
    assert.equal(await q.pauseGroup('G'), true);
    assert.equal(await q.pauseGroup('G'), false, 'already paused');
    assert.equal(await q.getGroupStatus('G'), GroupStatus.Paused, 'an empty paused group still reports paused');
    await q.add('n', {}, { group: { id: 'G' } });
    const ran: string[] = [];
    h.worker(async job => {
      ran.push(job.gid!);
    });
    await sleep(200);
    assert.deepEqual(ran, [], 'paused before its first job arrived');
    assert.equal(await q.resumeGroup('G'), true);
    assert.equal(await q.resumeGroup('G'), false);
    await waitFor(() => ran.length === 1, 'G ran after resume');
  } finally {
    await h.close();
  }
});

test("Pro's getters: group counts, statuses, groups by status, active counts, concurrency per group", async () => {
  const h = new Harness('pro-getters');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    // No worker: each group's first job is in flight (released), the rest wait in the group.
    for (const g of ['A', 'B', 'C']) for (const i of range(3)) await q.add('n', { i }, { group: { id: g } });
    await q.pauseGroup('B');
    assert.equal(await q.getGroupsCount(), 3);
    assert.equal(await q.getGroupsJobsCount(), 6);
    assert.equal(await q.getGroupJobsCount('A'), 2);
    assert.equal(await q.getGroupActiveCount('A'), 1);
    assert.deepEqual(
      (await q.getGroups()).sort((x, y) => x.id.localeCompare(y.id)),
      [
        { id: 'A', status: GroupStatus.Maxed },
        { id: 'B', status: GroupStatus.Paused },
        { id: 'C', status: GroupStatus.Maxed },
      ],
    );
    assert.equal(await q.getGroupStatus('A'), GroupStatus.Maxed);
    assert.equal(await q.getGroupStatus('nope'), null);
    assert.deepEqual(await q.getGroupsByStatus(GroupStatus.Paused), [{ id: 'B', count: 2 }]);
    assert.deepEqual(await q.getGroupsCountByStatus(), { waiting: 0, paused: 1, limited: 0, maxed: 2, blocked: 0 });

    assert.equal(await q.setGroupConcurrency('C', 3), 1, 'C had no limit of its own');
    assert.equal(await q.setGroupConcurrency('C', 3), 0);
    assert.equal(await q.getGroupConcurrency('C'), 3);
    assert.equal(await q.getGroupActiveCount('C'), 3, 'the new limit released two more right away');
    assert.equal(await q.getGroupStatus('C'), null, 'nothing left waiting in C');
    assert.equal(await q.removeGroupConcurrency('C'), 1);
    assert.equal(await q.removeGroupConcurrency('C'), 0);
    assert.equal(await q.getGroupConcurrency('C'), 1, 'back to the workers’ default');
    assert.deepEqual((await q.getGroupJobs('A', 1, 1)).map(j => j.data.i), [2], 'inclusive range, as in Pro');
  } finally {
    await h.close();
  }
});

test('getJob, job.remove() and queue.remove() see jobs still waiting in their group; deleteGroup, deleteGroups and drain clear them', async () => {
  const h = new Harness('pro-remove');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    const [first, second] = await Promise.all(range(3).map(i => q.add('n', { i }, { group: { id: 'G' } })));
    const delayed = await q.add('n', { i: 9 }, { group: { id: 'G' }, delay: 60_000 });
    assert.equal(await first.getState(), 'waiting', 'released into BullMQ');
    assert.equal(await delayed.getState(), 'delayed');
    const fetched = (await q.getJob(second.id!))!;
    assert.deepEqual([fetched.name, fetched.data, fetched.gid], ['n', { i: 1 }, 'G']);
    await fetched.remove();
    assert.equal(await q.getJob(second.id!), undefined);
    assert.equal(await q.remove(delayed.id!), 1);
    assert.equal(await q.getGroupJobsCount('G'), 1);

    await Promise.all(range(5).map(i => q.add('n', { i }, { group: { id: 'H' } })));
    await q.deleteGroup('H');
    assert.equal(await q.getGroupJobsCount('H'), 0);
    assert.equal(await q.getGroupActiveCount('H'), 1, 'the released job is not touched');
    await Promise.all(range(3).map(i => q.add('n', { i }, { group: { id: `K${i}` } })));
    await q.deleteGroups();
    assert.equal(await q.getGroupsJobsCount(), 0);

    const ran: number[] = [];
    h.worker(async job => {
      ran.push(job.data.i);
    });
    await waitFor(() => ran.length === 5, 'the released jobs of G, H and K0..K2');
    await sleep(100);
    assert.equal(ran.length, 5, 'deleted jobs never ran');

    await Promise.all(range(3).map(i => q.add('n', { i }, { group: { id: 'L' } })));
    await q.drain();
    assert.equal(await q.getGroupsJobsCount(), 0, 'drain() clears groups too');
  } finally {
    await h.close();
  }
});

test('repairMaxedGroup recounts a group that looks full with nothing running (a write lost in a failover)', async () => {
  const h = new Harness('pro-repair');
  try {
    const q = h.queue({ groupConcurrency: 1 });
    await q.add('n', { i: 0 }, { group: { id: 'G' } });
    await q.add('n', { i: 1 }, { group: { id: 'G' } });
    const ran: number[] = [];
    h.worker(async job => {
      ran.push(job.data.i);
    });
    await waitFor(async () => ran.length === 2 && (await q.getStats()).inflight === 0, 'G drained and acked');
    await q.redis.hset(`${q.keyBase}active`, 'G', '5'); // the decrement was lost
    await q.add('n', { i: 2 }, { group: { id: 'G' } });
    await sleep(200);
    assert.equal(ran.length, 2, 'stuck: G looks full');
    assert.equal(await q.getGroupStatus('G'), GroupStatus.Maxed);
    assert.deepEqual(await q.repairMaxedGroup('G'), { previous: 5, active: 0 });
    await waitFor(() => ran.length === 3, 'G:2 ran after the repair');
  } finally {
    await h.close();
  }
});

test('a worker without group options on a plain queue never opens group state', async () => {
  const h = new Harness('pro-lazy');
  try {
    const q = h.queue();
    const ran: number[] = [];
    const w = h.worker(
      async job => {
        ran.push(job.data.i);
      },
      { group: undefined, reconcileIntervalMs: 50 },
    );
    await q.addBulk(range(5).map(i => ({ name: 'n', data: { i } })));
    await waitFor(() => ran.length === 5, 'plain jobs');
    await sleep(150);
    assert.deepEqual(await keysMatching(`bull:${h.name}:gq:*`), []);
    assert.equal((w as any).groupQueue, undefined, 'no group connection was opened');
    // Once grouped jobs show up, the same worker picks them up and runs the group machinery.
    await q.add('n', { i: 5 }, { group: { id: 'G' } });
    await q.add('n', { i: 6 }, { group: { id: 'G' }, delay: 100 });
    await waitFor(() => ran.length === 7, 'grouped and delayed grouped jobs');
    assert.ok((w as any).groupQueue);
  } finally {
    await h.close();
  }
});

test("the queue's defaultJobOptions reach grouped jobs once: a default delay is not applied twice, a default priority not at all", async () => {
  const h = new Harness('pro-defaults');
  try {
    const q = h.queue({ groupConcurrency: 1, defaultJobOptions: { delay: 100, priority: 5, attempts: 2 } });
    const job = await q.add('n', {}, { group: { id: 'G' } });
    assert.equal(await job.getState(), 'delayed', 'the default delay applies to the group job');
    await sleep(150);
    await q._promoteDelayed(); // what a worker's promote loop does
    assert.equal(await q.getJobState(job.id!), 'waiting', 'released to BullMQ as waiting: not delayed again, not prioritized');
    const released = (await q._bullJob(job.id!))!;
    assert.equal(released.opts.attempts, 2, 'other defaults still apply');
  } finally {
    await h.close();
  }
});

test('a worker with group options that is registered but never run (autorun: false) opens nothing', async () => {
  const h = new Harness('pro-autorun');
  try {
    const w = h.worker(async () => undefined, { autorun: false });
    await sleep(100);
    assert.equal((w as any).groupQueue, undefined);
    assert.deepEqual(await keysMatching(`bull:${h.name}:gq:*`), []);
    w.run().catch(() => undefined);
    await waitFor(async () => (await keysMatching(`bull:${h.name}:gq:meta`)).length === 1, 'its group options written once it runs');
  } finally {
    await h.close();
  }
});

test('options a group job cannot take are refused at add time', async () => {
  const h = new Harness('pro-refuse');
  try {
    const q = h.queue();
    await assert.rejects(q.add('n', {}, { group: { id: 'G' }, repeat: { every: 1000 } }), /'repeat' is not supported/);
    await assert.rejects(q.add('n', {}, { group: { id: 'G' }, lifo: true }), /'lifo' is not supported/);
    await assert.rejects(q.add('n', {}, { group: { id: 'G', limit: 3 } as any }), /not supported: limit/);
    await assert.rejects(q.add('n', {}, { group: { id: '' } }), /non-empty/);
    await assert.rejects(q.add('n', {}, { group: { id: 'G', priority: 2_097_153 } }), /from 0 to 2097152/);
    assert.equal(await q.getGroupsJobsCount(), 0);
  } finally {
    await h.close();
  }
});
