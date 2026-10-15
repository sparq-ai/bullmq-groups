import IORedis from 'ioredis';
import { GroupQueue, GroupWorker } from '../src';
import type { GroupQueueOptions, GroupWorkerOptions } from '../src';
import type { Processor } from 'bullmq';

export const connection = { host: '127.0.0.1', port: Number(process.env.REDIS_PORT ?? 6390) };

export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

export function uniqueName(tag: string): string {
  return `t-${tag}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
}

export async function waitFor(check: () => boolean | Promise<boolean>, what: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await sleep(20);
  }
}

/** Records starts and ends per group to check FIFO order and concurrency from inside processors. */
export class Recorder {
  readonly starts = new Map<string, number[]>();
  readonly ends = new Map<string, number[]>();
  readonly maxActive = new Map<string, number>();
  readonly log: string[] = [];
  private readonly active = new Map<string, number>();
  done = 0;

  start(group: string, seq: number): void {
    const a = (this.active.get(group) ?? 0) + 1;
    this.active.set(group, a);
    this.maxActive.set(group, Math.max(a, this.maxActive.get(group) ?? 0));
    push(this.starts, group, seq);
    this.log.push(`${group}:${seq}`);
  }

  end(group: string, seq: number): void {
    this.active.set(group, (this.active.get(group) ?? 0) - 1);
    push(this.ends, group, seq);
    this.done++;
  }
}

function push(map: Map<string, number[]>, key: string, value: number): void {
  const list = map.get(key);
  if (list) list.push(value);
  else map.set(key, [value]);
}

/** Owns everything a test creates and tears it down, including the Redis keys. */
export class Harness {
  readonly name: string;
  private readonly closers: (() => Promise<unknown>)[] = [];

  constructor(tag: string) {
    this.name = uniqueName(tag);
  }

  queue(opts: Partial<GroupQueueOptions> = {}): GroupQueue {
    const q = new GroupQueue(this.name, { connection, ...opts });
    this.closers.push(() => q.close());
    return q;
  }

  worker(processor: Processor, opts: Partial<GroupWorkerOptions> = {}): GroupWorker {
    const w = new GroupWorker(this.name, processor, { connection, concurrency: 4, ...opts });
    w.on('error', err => {
      if (!String(err?.message).includes('Connection is closed')) console.error(`[${this.name}] worker error:`, err);
    });
    this.closers.push(() => w.close());
    return w;
  }

  defer(close: () => Promise<unknown>): void {
    this.closers.push(close);
  }

  async close(): Promise<void> {
    for (const close of this.closers.reverse()) await close().catch(() => undefined);
    await deleteKeys(`bull:${this.name}:*`);
  }
}

export async function deleteKeys(pattern: string): Promise<void> {
  const redis = new IORedis(connection);
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
    cursor = next;
    if (keys.length) await redis.del(...keys);
  } while (cursor !== '0');
  await redis.quit();
}

export async function keysMatching(pattern: string): Promise<string[]> {
  const redis = new IORedis(connection);
  const found: string[] = [];
  let cursor = '0';
  do {
    const [next, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
    cursor = next;
    found.push(...keys);
  } while (cursor !== '0');
  await redis.quit();
  return found.sort();
}

export function assertFifo(rec: Recorder, expected: Map<string, number[]> | ((group: string) => number[])): void {
  for (const [group, starts] of rec.starts) {
    const want = typeof expected === 'function' ? expected(group) : expected.get(group);
    const firstRuns = starts.filter((s, i) => starts.indexOf(s) === i);
    if (JSON.stringify(firstRuns) !== JSON.stringify(want)) {
      throw new Error(`group ${group} started out of order: ${firstRuns.join(',')} (expected ${want?.join(',')})`);
    }
  }
}

/**
 * With group concurrency n, jobs are released in order but up to n run at once, so they can start
 * in any order inside that window. Check that no job started before the job n places ahead of it.
 */
export function assertWindowedFifo(rec: Recorder, n: number): void {
  for (const [group, starts] of rec.starts) {
    const seen = new Set<number>();
    for (const seq of starts) {
      for (let earlier = 0; earlier <= seq - n; earlier++) {
        if (!seen.has(earlier)) throw new Error(`group ${group}: job ${seq} started before job ${earlier} (window ${n}): ${starts.join(',')}`);
      }
      seen.add(seq);
    }
  }
}
