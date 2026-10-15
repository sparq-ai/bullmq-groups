import IORedis from 'ioredis';
import { DelayedError, Queue, Worker } from 'bullmq';
import { Approach, BenchJob, BenchProcessor, BENCH_REDIS, chunk, payloadFor } from '../lib';

const ENTER = `
-- KEYS: next-seq key, lock key. ARGV: seq, lock ttl ms
local nextSeq = tonumber(redis.call('GET', KEYS[1]) or '0')
if tonumber(ARGV[1]) ~= nextSeq then return 0 end
if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) then return 1 end
return 0`;
const LEAVE = `
redis.call('INCR', KEYS[1])
redis.call('DEL', KEYS[2])
return 1`;

/**
 * Approach C: everything in one plain queue; a job may only run when it is its group's turn
 * (sequence number + per-group lock). Out-of-turn jobs are pushed back with moveToDelayed.
 * Only group concurrency 1 is possible, and every out-of-turn pick is wasted Redis work.
 */
export function delayGate(queueName: string, backoffMs = 50): Approach {
  const queue = new Queue(queueName, { connection: BENCH_REDIS });
  const redis = new IORedis(BENCH_REDIS);
  redis.defineCommand('gateEnter', { numberOfKeys: 2, lua: ENTER });
  redis.defineCommand('gateLeave', { numberOfKeys: 2, lua: LEAVE });
  const workers: Worker[] = [];
  const keys = (group: string) => [`gate:${queueName}:${group}:next`, `gate:${queueName}:${group}:lock`];
  let wasted = 0;
  return {
    name: 'delay-gate',
    async enqueue(jobs: BenchJob[]) {
      for (const part of chunk(jobs, 1000)) {
        await queue.addBulk(part.map(j => ({ name: 'sync', data: { group: j.group, ...payloadFor(j) }, opts: { removeOnComplete: true } })));
      }
    },
    async startWorkers(processor: BenchProcessor, concurrency: number) {
      workers.push(
        new Worker(
          queueName,
          async (job, token) => {
            const [nextKey, lockKey] = keys(job.data.group);
            const entered = await (redis as any).gateEnter(nextKey, lockKey, job.data.seq, 60_000);
            if (!entered) {
              wasted++;
              await job.moveToDelayed(Date.now() + backoffMs + Math.floor(Math.random() * backoffMs), token);
              throw new DelayedError();
            }
            try {
              await processor({ group: job.data.group, seq: job.data.seq, t0: job.data.t0 });
            } finally {
              await (redis as any).gateLeave(nextKey, lockKey);
            }
          },
          { connection: BENCH_REDIS, concurrency },
        ),
      );
    },
    async stop() {
      await Promise.all(workers.map(w => w.close()));
      await queue.close();
      await redis.quit();
    },
    connections: () => 2 + workers.length * 2,
    get wastedPicks() {
      return wasted;
    },
  } as Approach & { wastedPicks: number };
}
