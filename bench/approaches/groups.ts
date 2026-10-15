import { GroupQueue, GroupWorker } from '../../src';
import { Approach, BenchJob, BenchProcessor, BENCH_REDIS, chunk, payloadFor } from '../lib';

/** This package: jobs wait in their group and are released to one BullMQ queue only when the group has a free slot. */
export function groups(queueName: string, groupConcurrency = 1): Approach {
  const queue = new GroupQueue(queueName, { connection: BENCH_REDIS, groupConcurrency });
  const workers: GroupWorker[] = [];
  return {
    name: groupConcurrency === 1 ? 'groups' : `groups-c${groupConcurrency}`,
    async enqueue(jobs: BenchJob[]) {
      for (const part of chunk(jobs, 1000)) {
        await queue.addBulk(part.map(j => ({ groupId: j.group, name: 'sync', data: payloadFor(j), opts: { removeOnComplete: true } })));
      }
    },
    async startWorkers(processor: BenchProcessor, concurrency: number) {
      const w = new GroupWorker(
        queueName,
        async job => processor({ group: (job.opts as any).group.id, seq: job.data.seq, t0: job.data.t0 }),
        { connection: BENCH_REDIS, concurrency },
      );
      w.on('error', err => console.error('group worker error', err));
      workers.push(w);
    },
    async stop() {
      await Promise.all(workers.map(w => w.close()));
      await queue.close();
    },
    // GroupQueue: BullMQ queue + scripts connection. GroupWorker: BullMQ worker (2) + its own GroupQueue (2).
    connections: () => 2 + workers.length * 4,
  };
}
