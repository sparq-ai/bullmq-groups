import IORedis from 'ioredis';
import { Queue, Worker } from 'bullmq';
import { Approach, BenchJob, BenchProcessor, BENCH_REDIS, payloadFor } from '../lib';

/**
 * Approach A: a dedicated BullMQ queue and a concurrency-1 worker per group.
 * FIFO and fairness come for free, but every group costs a worker with its own blocking
 * connection on every worker process, and there is no global concurrency limit.
 * Generous to the approach: workers are created up front for the groups the benchmark uses,
 * a real version would also need group discovery and idle-worker cleanup.
 */
export function queuePerGroup(queueName: string): Approach {
  // Producers and workers share one connection for normal commands; each worker still needs its own blocking one.
  const shared = new IORedis({ ...BENCH_REDIS, maxRetriesPerRequest: null });
  const queues = new Map<string, Queue>();
  const workers: Worker[] = [];
  const queueFor = (group: string) => {
    let q = queues.get(group);
    if (!q) {
      q = new Queue(`${queueName}-${group}`, { connection: shared });
      queues.set(group, q);
    }
    return q;
  };
  return {
    name: 'queue-per-group',
    async prepare(groups: string[]) {
      for (const g of groups) queueFor(g);
    },
    async enqueue(jobs: BenchJob[]) {
      const byGroup = new Map<string, BenchJob[]>();
      for (const j of jobs) {
        const list = byGroup.get(j.group);
        if (list) list.push(j);
        else byGroup.set(j.group, [j]);
      }
      await Promise.all(
        [...byGroup].map(([group, list]) =>
          queueFor(group).addBulk(list.map(j => ({ name: 'sync', data: payloadFor(j), opts: { removeOnComplete: true } }))),
        ),
      );
    },
    async startWorkers(processor: BenchProcessor) {
      for (const group of queues.keys()) {
        workers.push(
          new Worker(`${queueName}-${group}`, async job => processor({ group, seq: job.data.seq, t0: job.data.t0 }), {
            connection: shared,
            concurrency: 1,
          }),
        );
      }
    },
    async stop() {
      await Promise.all(workers.map(w => w.close()));
      await Promise.all([...queues.values()].map(q => q.close()));
      await shared.quit();
    },
    connections: () => 1 + workers.length,
  };
}
