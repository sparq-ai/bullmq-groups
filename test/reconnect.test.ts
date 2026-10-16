import { test } from 'node:test';
import assert from 'node:assert/strict';
import IORedis from 'ioredis';
import { groupIdOf } from '../src';
import { Harness, Recorder, assertFifo, connection, sleep, waitFor, addBulkTo } from './helpers';

const range = (n: number) => Array.from({ length: n }, (_, i) => i);

test('every client connection is killed three times mid-run: no job lost, duplicated, reordered or overlapped', async t => {
  const h = new Harness('reconnect');
  const admin = new IORedis(connection);
  try {
    const q = h.queue({ groupConcurrency: 1, reconcileGraceMs: 1_000 });
    q.on('error', () => undefined); // expected while connections are being killed
    await addBulkTo(q, range(15).flatMap(seq => range(150).map(g => ({ groupId: `g${g}`, name: 'n', data: { seq } }))));

    const rec = new Recorder();
    const errors: string[] = [];
    for (let i = 0; i < 2; i++) {
      const w = h.worker(async job => {
        rec.start(groupIdOf(job)!, job.data.seq);
        await sleep(1 + Math.random() * 3);
        rec.end(groupIdOf(job)!, job.data.seq);
      }, { concurrency: 16, reconcileIntervalMs: 300, reconcileGraceMs: 1_000 });
      w.removeAllListeners('error');
      w.on('error', err => errors.push(String(err?.message ?? err)));
    }

    let kills = 0;
    for (const threshold of [400, 1_000, 1_600]) {
      await waitFor(() => rec.done >= threshold, `${threshold} jobs`, 30_000);
      kills += Number(await admin.client('KILL', 'TYPE', 'normal', 'SKIPME', 'yes'));
    }
    await waitFor(() => rec.done >= 2_250, 'all 2,250 jobs', 60_000);
    await sleep(300);

    t.diagnostic(`connections killed: ${kills}; worker errors: ${JSON.stringify(Object.fromEntries(errors.reduce((m, e) => m.set(e, (m.get(e) ?? 0) + 1), new Map<string, number>())))}`);
    const completions = new Map<string, number>();
    for (const [g, ends] of rec.ends) for (const s of ends) completions.set(`${g}:${s}`, (completions.get(`${g}:${s}`) ?? 0) + 1);
    assert.equal(completions.size, 2_250, 'some job never completed');
    const twice = [...completions].filter(([, n]) => n > 1);
    assert.deepEqual(twice, [], 'a job completed twice');
    assertFifo(rec, () => range(15));
    for (const [g, max] of rec.maxActive) assert.equal(max, 1, `${g} overlapped`);
    await waitFor(async () => (await q.getStats()).inflight === 0, 'last acks', 20_000);
  } finally {
    await admin.quit();
    await h.close();
  }
});
