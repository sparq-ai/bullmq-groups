import { Queue, Worker } from 'bullmq';
import { Approach, BenchJob, BenchProcessor, BENCH_REDIS, chunk, payloadFor } from '../lib';

/** Baseline: one plain BullMQ queue. No per-group ordering and no fairness, just the cost floor. */
export function plain(queueName: string): Approach {
  const queue = new Queue(queueName, { connection: BENCH_REDIS });
  const workers: Worker[] = [];
  return {
    name: 'plain-bullmq',
    async enqueue(jobs: BenchJob[]) {
      for (const part of chunk(jobs, 1000)) {
        await queue.addBulk(part.map(j => ({ name: 'sync', data: { group: j.group, ...payloadFor(j) }, opts: { removeOnComplete: true } })));
      }
    },
    async startWorkers(processor: BenchProcessor, concurrency: number) {
      workers.push(
        new Worker(queueName, async job => processor({ group: job.data.group, seq: job.data.seq, t0: job.data.t0 }), {
          connection: BENCH_REDIS,
          concurrency,
        }),
      );
    },
    async stop() {
      await Promise.all(workers.map(w => w.close()));
      await queue.close();
    },
    connections: () => 1 + workers.length * 2,
  };
}
