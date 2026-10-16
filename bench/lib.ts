import IORedis from 'ioredis';

export const BENCH_REDIS = { host: process.env.REDIS_HOST ?? '127.0.0.1', port: Number(process.env.REDIS_PORT ?? 6390) };

/** One job as the benchmarks see it: which group it belongs to, its place in the group, when it was added. */
export interface BenchJob {
  group: string;
  seq: number;
  t0: number;
}

export type BenchProcessor = (job: BenchJob) => Promise<void>;

export interface Approach {
  readonly name: string;
  /** Create whatever must exist before workers start (only queue-per-group needs it). */
  prepare?(groups: string[]): Promise<void>;
  /** Add jobs, preserving array order within each group. */
  enqueue(jobs: BenchJob[]): Promise<void>;
  startWorkers(processor: BenchProcessor, concurrency: number): Promise<void>;
  stop(): Promise<void>;
  /** Number of Redis connections this approach opened, for the report. */
  connections(): number;
}

export interface RedisSnapshot {
  at: number;
  commands: number;
  calls: Record<string, number>;
  /** Microseconds Redis spent per command, scripts' inner commands included. */
  usec: Record<string, number>;
  usedMemory: number;
  cpuMs: number;
  keys: number;
  clients: number;
}

function parseInfo(text: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const line of text.split('\r\n')) {
    const i = line.indexOf(':');
    if (i > 0 && !line.startsWith('#')) out[line.slice(0, i)] = line.slice(i + 1);
  }
  return out;
}

export async function snapshot(redis: IORedis): Promise<RedisSnapshot> {
  const [stats, cmdstats, memory, cpu, clients, keys] = await Promise.all([
    redis.info('stats'),
    redis.info('commandstats'),
    redis.info('memory'),
    redis.info('cpu'),
    redis.info('clients'),
    redis.dbsize(),
  ]);
  const calls: Record<string, number> = {};
  const usec: Record<string, number> = {};
  for (const [k, v] of Object.entries(parseInfo(cmdstats))) {
    calls[k.replace('cmdstat_', '')] = Number(/calls=(\d+)/.exec(v)![1]);
    usec[k.replace('cmdstat_', '')] = Number(/usec=(\d+)/.exec(v)![1]);
  }
  const c = parseInfo(cpu);
  return {
    at: Date.now(),
    commands: Number(parseInfo(stats).total_commands_processed),
    calls,
    usec,
    usedMemory: Number(parseInfo(memory).used_memory),
    cpuMs: (Number(c.used_cpu_sys) + Number(c.used_cpu_user)) * 1000,
    keys,
    clients: Number(parseInfo(clients).connected_clients),
  };
}

export interface RedisDelta {
  commands: number;
  calls: number;
  scriptRuns: number;
  cpuMs: number;
  top: [string, number][];
  /** Commands Redis spent the most time on: [command, ms]. */
  topMs: [string, number][];
}

export function delta(a: RedisSnapshot, b: RedisSnapshot): RedisDelta {
  const per: [string, number][] = Object.keys(b.calls).map(k => [k, b.calls[k] - (a.calls[k] ?? 0)]);
  const scriptRuns = per.filter(([k]) => k === 'evalsha' || k === 'eval' || k === 'fcall').reduce((s, [, n]) => s + n, 0);
  return {
    commands: b.commands - a.commands,
    calls: per.reduce((s, [, n]) => s + n, 0),
    scriptRuns,
    cpuMs: b.cpuMs - a.cpuMs,
    top: per.filter(([, n]) => n > 0).sort((x, y) => y[1] - x[1]).slice(0, 12),
    topMs: Object.keys(b.usec)
      .map(k => [k, (b.usec[k] - (a.usec[k] ?? 0)) / 1000] as [string, number])
      .filter(([, t]) => t > 0)
      .sort((x, y) => y[1] - x[1])
      .slice(0, 8),
  };
}

export function percentiles(values: number[]): { p50: number; p95: number; p99: number; max: number; mean: number } {
  if (!values.length) return { p50: NaN, p95: NaN, p99: NaN, max: NaN, mean: NaN };
  const s = [...values].sort((x, y) => x - y);
  const at = (p: number) => s[Math.min(s.length - 1, Math.floor(p * (s.length - 1)))];
  return { p50: at(0.5), p95: at(0.95), p99: at(0.99), max: s[s.length - 1], mean: s.reduce((a, b) => a + b, 0) / s.length };
}

export const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));

export async function waitUntil(check: () => boolean, timeoutMs: number, pollMs = 5): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (!check()) {
    if (Date.now() > deadline) return false;
    await sleep(pollMs);
  }
  return true;
}

export function chunk<T>(items: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

/** A small, realistic payload: an external account id and a job kind, as a typical sync job carries. */
export function payloadFor(job: BenchJob): Record<string, unknown> {
  return { accountId: 7_000_000_000 + Number(job.group.replace(/\D/g, '') || 0), kind: 'account-sync', seq: job.seq, t0: job.t0 };
}
