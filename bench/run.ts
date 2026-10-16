/**
 * One benchmark run in a fresh process against a flushed Redis.
 *
 *   tsx bench/run.ts --scenario throughput --approach groups --groups 1000 --jobs 20 --concurrency 50
 *
 * Scenarios
 *   throughput  preload groups x jobs, then drain with one worker process
 *   latency     open loop: add --rate jobs/s for --duration s, measure add -> start latency
 *   skew        one hot group with --hot jobs added first, then --groups one-job groups
 *   memory      preload groups x jobs with no workers; Redis memory and keys per job
 *   backlog     one slow job per group and few workers, so released jobs wait in BullMQ past the
 *               reconciler's grace period; measures Redis CPU and latency seen by other clients
 *
 * WARNING: flushes the Redis database at REDIS_HOST:REDIS_PORT (default 127.0.0.1:6390).
 */
import { writeFileSync, mkdirSync } from 'fs';
import { join } from 'path';
import IORedis from 'ioredis';
import { Approach, BenchJob, BENCH_REDIS, delta, percentiles, sleep, snapshot, waitUntil } from './lib';
import { plain } from './approaches/plain';
import { plainDedup } from './approaches/plain-dedup';
import { groups } from './approaches/groups';
import { queuePerGroup } from './approaches/queue-per-group';
import { delayGate } from './approaches/delay-gate';

type Args = Record<string, string>;
const args: Args = {};
for (let i = 2; i < process.argv.length; i += 2) args[process.argv[i].replace(/^--/, '')] = process.argv[i + 1];
const num = (k: string, d: number) => (args[k] === undefined ? d : Number(args[k]));

const scenario = args.scenario ?? 'throughput';
const approachName = args.approach ?? 'groups';
const G = num('groups', 1000);
const J = num('jobs', 20);
const C = num('concurrency', 50);
const workMs = num('work', 0);
const timeoutS = num('timeout', 300);

function makeApproach(name: string, queueName: string): Approach {
  switch (name) {
    case 'plain':
      return plain(queueName);
    case 'plain-dedup':
      return plainDedup(queueName);
    case 'groups':
      return groups(queueName, 1);
    case 'groups-c4':
      return groups(queueName, 4);
    case 'groups-nolimit':
      return groups(queueName, null);
    case 'queue-per-group':
      return queuePerGroup(queueName);
    case 'delay-gate':
      return delayGate(queueName);
    default:
      throw new Error(`unknown approach ${name}`);
  }
}

/** Tracks per-job latency plus per-group order inversions and overlaps, whatever the approach. */
class Tracker {
  startLat: number[] = [];
  endLat: number[] = [];
  done = 0;
  inversions = 0;
  overlaps = 0;
  doneByGroup = new Map<string, number>();
  smallStartLat: number[] = [];
  private maxSeq = new Map<string, number>();
  private running = new Map<string, number>();

  processor = async (job: BenchJob) => {
    const start = Date.now();
    this.startLat.push(start - job.t0);
    if (job.group !== 'hot' && scenario === 'skew') this.smallStartLat.push(start - job.t0);
    const max = this.maxSeq.get(job.group) ?? -1;
    if (job.seq < max) this.inversions++;
    else this.maxSeq.set(job.group, job.seq);
    const r = (this.running.get(job.group) ?? 0) + 1;
    this.running.set(job.group, r);
    if (r > 1) this.overlaps++;
    if (workMs > 0) await sleep(workMs);
    this.running.set(job.group, (this.running.get(job.group) ?? 1) - 1);
    this.endLat.push(Date.now() - job.t0);
    this.doneByGroup.set(job.group, (this.doneByGroup.get(job.group) ?? 0) + 1);
    this.done++;
  };
}

function preloadJobs(nGroups: number, perGroup: number): BenchJob[] {
  const jobs: BenchJob[] = [];
  const t0 = Date.now();
  for (let seq = 0; seq < perGroup; seq++) for (let g = 0; g < nGroups; g++) jobs.push({ group: `g${g}`, seq, t0 });
  return jobs;
}

async function main() {
  const admin = new IORedis(BENCH_REDIS);
  await admin.flushdb();
  await admin.config('RESETSTAT');
  const queueName = `bench-${approachName}`;
  const approach = makeApproach(approachName, queueName);
  const tracker = new Tracker();
  const out: Record<string, unknown> = {
    scenario,
    approach: approach.name,
    params: { ...args, groups: G, jobs: J, concurrency: C, workMs },
    env: { node: process.version, bullmq: require('bullmq/package.json').version, redis: (await admin.info('server')).match(/redis_version:(\S+)/)?.[1] },
  };
  const base = await snapshot(admin);
  let total = 0;

  if (scenario === 'throughput' || scenario === 'memory') {
    const jobs = preloadJobs(G, J);
    total = jobs.length;
    await approach.prepare?.([...new Set(jobs.map(j => j.group))]);
    const t = Date.now();
    await approach.enqueue(jobs);
    const enqueueMs = Date.now() - t;
    const loaded = await snapshot(admin);
    out.enqueue = { ms: enqueueMs, jobsPerSec: Math.round(total / (enqueueMs / 1000)), redis: perJob(delta(base, loaded), total) };
    out.memory = {
      bytesPerJob: Math.round((loaded.usedMemory - base.usedMemory) / total),
      usedMemoryMB: +(loaded.usedMemory / 1048576).toFixed(1),
      keys: loaded.keys,
      keysPerGroup: +(loaded.keys / G).toFixed(2),
    };
    if (scenario === 'throughput') {
      await approach.startWorkers(tracker.processor, C);
      const tDrain = Date.now();
      const finished = await waitUntil(() => tracker.done >= total, timeoutS * 1000);
      const drainMs = Date.now() - tDrain;
      const drained = await snapshot(admin);
      out.drain = {
        finished,
        ms: drainMs,
        done: tracker.done,
        jobsPerSec: Math.round(tracker.done / (drainMs / 1000)),
        redis: perJob(delta(loaded, drained), tracker.done),
        clients: drained.clients,
        keysAfter: drained.keys,
      };
    }
  } else if (scenario === 'latency') {
    const rate = num('rate', 1000);
    const durationS = num('duration', 20);
    const groupIds = Array.from({ length: G }, (_, g) => `g${g}`);
    await approach.prepare?.(groupIds);
    await approach.startWorkers(tracker.processor, C);
    await sleep(500);
    const before = await snapshot(admin);
    const nextSeq = new Map<string, number>();
    const started = Date.now();
    const pending: Promise<void>[] = [];
    let sent = 0;
    // Open loop: add on schedule regardless of how the system keeps up, like webhooks arriving.
    while (Date.now() - started < durationS * 1000) {
      const due = Math.floor(((Date.now() - started) / 1000) * rate);
      while (sent < due) {
        const group = groupIds[Math.floor(Math.random() * G)];
        const seq = nextSeq.get(group) ?? 0;
        nextSeq.set(group, seq + 1);
        pending.push(approach.enqueue([{ group, seq, t0: Date.now() }]));
        sent++;
      }
      await sleep(2);
    }
    await Promise.all(pending);
    total = sent;
    const finished = await waitUntil(() => tracker.done >= total, timeoutS * 1000);
    const after = await snapshot(admin);
    out.latency = {
      finished,
      sent,
      done: tracker.done,
      offeredRate: rate,
      achievedRate: Math.round(tracker.done / ((Date.now() - started) / 1000)),
      startMs: percentiles(tracker.startLat),
      endMs: percentiles(tracker.endLat),
      redis: perJob(delta(before, after), tracker.done),
      clients: after.clients,
    };
  } else if (scenario === 'skew') {
    const hot = num('hot', 20000);
    const t0 = Date.now();
    const hotJobs: BenchJob[] = Array.from({ length: hot }, (_, seq) => ({ group: 'hot', seq, t0 }));
    const smallJobs: BenchJob[] = Array.from({ length: G }, (_, g) => ({ group: `s${g}`, seq: 0, t0 }));
    await approach.prepare?.(['hot', ...smallJobs.map(j => j.group)]);
    await approach.enqueue(hotJobs);
    await approach.enqueue(smallJobs.map(j => ({ ...j, t0: Date.now() })));
    total = hot + G;
    const before = await snapshot(admin);
    await approach.startWorkers(tracker.processor, C);
    const tRun = Date.now();
    const smallDone = () => tracker.smallStartLat.length >= G && tracker.done - (tracker.doneByGroup.get('hot') ?? 0) >= G;
    const finished = await waitUntil(smallDone, timeoutS * 1000, 10);
    const smallMs = Date.now() - tRun;
    const hotDone = tracker.doneByGroup.get('hot') ?? 0;
    const after = await snapshot(admin);
    out.skew = {
      smallGroupsFinished: finished,
      smallGroupsDoneMs: smallMs,
      smallDone: tracker.done - hotDone,
      hotDoneMeanwhile: hotDone,
      hotJobsPerSec: Math.round(hotDone / (smallMs / 1000)),
      smallStartMs: percentiles(tracker.smallStartLat),
      redis: perJob(delta(before, after), Math.max(1, tracker.done)),
      clients: after.clients,
    };
    if ('wastedPicks' in approach) (out.skew as any).wastedPicks = (approach as any).wastedPicks;
    if ('dropped' in approach) (out.skew as any).dropped = (approach as any).dropped;
  } else if (scenario === 'backlog') {
    // Released jobs pile up in BullMQ's wait list for longer than the reconciler's grace period, as
    // with minutes-long syncs. Several workers each run the reconciler; a probe client measures the
    // Redis latency everyone else sees.
    const durationS = num('duration', 75);
    const nWorkers = num('workers', 4);
    const jobs = preloadJobs(G, 1);
    total = jobs.length;
    await approach.prepare?.([...new Set(jobs.map(j => j.group))]);
    await approach.enqueue(jobs);
    for (let i = 0; i < nWorkers; i++) await approach.startWorkers(tracker.processor, C);
    const probe = new IORedis(BENCH_REDIS);
    const pings: number[] = [];
    const before = await snapshot(admin);
    const started = Date.now();
    while (Date.now() - started < durationS * 1000) {
      const t = performance.now();
      await probe.ping();
      pings.push(performance.now() - t);
      await sleep(10);
    }
    const after = await snapshot(admin);
    await probe.quit();
    const d = delta(before, after);
    out.backlog = {
      durationS,
      workers: nWorkers,
      done: tracker.done,
      waitingInBullmq: total - tracker.done,
      redisCpuPct: +((d.cpuMs / (durationS * 1000)) * 100).toFixed(1),
      pingMs: percentiles(pings),
      lposCalls: (after.calls.lpos ?? 0) - (before.calls.lpos ?? 0),
      lposMs: +(((after.usec.lpos ?? 0) - (before.usec.lpos ?? 0)) / 1000).toFixed(1),
      topMs: d.topMs,
      clients: after.clients,
    };
  } else {
    throw new Error(`unknown scenario ${scenario}`);
  }

  out.order = { inversions: tracker.inversions, overlaps: tracker.overlaps };
  out.workerRssMB = Math.round(process.memoryUsage().rss / 1048576);
  out.approachConnections = approach.connections();

  await Promise.race([approach.stop(), sleep(30_000)]);
  await admin.flushdb();
  await admin.quit();

  const dir = join(__dirname, 'results');
  mkdirSync(dir, { recursive: true });
  const label =
    args.label ??
    `${scenario}-${approachName}-g${G}${scenario === 'skew' ? `-hot${args.hot ?? 20000}` : `-j${J}`}${args.rate ? `-r${args.rate}` : ''}${args.variant ? `-${args.variant}` : ''}`;
  writeFileSync(join(dir, `${label}.json`), JSON.stringify(out, null, 2));
  console.log(JSON.stringify(out));
  process.exit(0);
}

function perJob(d: ReturnType<typeof delta>, jobs: number) {
  return {
    commandsPerJob: +(d.commands / jobs).toFixed(2),
    callsPerJob: +(d.calls / jobs).toFixed(2),
    scriptRunsPerJob: +(d.scriptRuns / jobs).toFixed(2),
    redisCpuMsPer1kJobs: +((d.cpuMs / jobs) * 1000).toFixed(1),
    top: d.top.slice(0, 8).map(([k, n]) => [k, +(n / jobs).toFixed(2)]),
  };
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
