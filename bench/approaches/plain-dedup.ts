import { Queue, Worker } from 'bullmq';
import { Approach, BenchJob, BenchProcessor, BENCH_REDIS, chunk, payloadFor } from '../lib';

/**
 * The usual way to keep plain BullMQ fair: one queue, every add deduplicated by group
 * ({ deduplication: { id: group }, removeOnComplete, removeOnFail }). A group has at most one job
 * waiting or running; any other add for it while that job exists is dropped.
 */
export function plainDedup(queueName: string): Approach & { dropped: number } {
  const queue = new Queue(queueName, { connection: BENCH_REDIS });
  const workers: Worker[] = [];
  const seen = new Set<string>();
  const approach = {
    name: 'plain-dedup',
    dropped: 0,
    async enqueue(jobs: BenchJob[]) {
      for (const part of chunk(jobs, 1000)) {
        const added = await queue.addBulk(
          part.map(j => ({
            name: 'sync',
            data: { group: j.group, ...payloadFor(j) },
            opts: { removeOnComplete: true, removeOnFail: true, deduplication: { id: j.group } },
          })),
        );
        // A deduplicated add returns the job it matched, so an id seen before means the add was dropped.
        for (const job of added) {
          if (seen.has(job.id!)) approach.dropped++;
          else seen.add(job.id!);
        }
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
  return approach;
}
