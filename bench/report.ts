/** Turns bench/results/*.json into bench/RESULTS.md. Run after bench/all.sh. */
import { readdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import os from 'os';

const dir = join(__dirname, 'results');
const runs: any[] = readdirSync(dir)
  .filter(f => f.endsWith('.json'))
  .map(f => ({ file: f, ...JSON.parse(readFileSync(join(dir, f), 'utf8')) }))
  .map(r => ({ ...r, params: Object.fromEntries(Object.entries(r.params).map(([k, v]) => [k, typeof v === 'string' && v !== '' && !Number.isNaN(Number(v)) ? Number(v) : v])) }));

const ORDER = ['plain-bullmq', 'plain-dedup', 'groups', 'groups-c4', 'groups-nolimit', 'queue-per-group', 'delay-gate'];
const NAMES: Record<string, string> = {
  'plain-bullmq': 'Plain BullMQ (no groups)',
  'plain-dedup': 'Plain BullMQ, one job per group (deduplication)',
  groups: 'This package, group concurrency 1',
  'groups-c4': 'This package, group concurrency 4',
  'groups-nolimit': 'This package, no group limit (Pro default)',
  'queue-per-group': 'Queue per group',
  'delay-gate': 'Delay out-of-turn jobs',
};
const byApproach = (a: any, b: any) => ORDER.indexOf(a.approach) - ORDER.indexOf(b.approach) || String(a.params.variant ?? '').localeCompare(String(b.params.variant ?? ''));
/** Approach label for a run; a `--variant` (e.g. a code change being compared) is shown after it. */
const LABEL = new Proxy({} as Record<string, string>, { get: (_, approach: string) => NAMES[approach] ?? approach });
const label = (r: any) => `${LABEL[r.approach]}${r.params.variant ? ` (${r.params.variant})` : ''}`;
const n = (v: number | undefined, digits = 0) => (v === undefined || Number.isNaN(v) ? 'n/a' : v.toLocaleString('en-US', { maximumFractionDigits: digits }));
const ms = (v: number) => (v >= 10_000 ? `${n(v / 1000, 1)} s` : `${n(v)} ms`);

const lines: string[] = [];
const env = runs[0]?.env ?? {};
lines.push('# Benchmark results', '');
lines.push(
  `Machine: ${os.cpus()[0]?.model.trim()}, ${os.cpus().length} threads, ${Math.round(os.totalmem() / 2 ** 30)} GB RAM. ` +
    `Redis ${env.redis} in Docker on the same machine, persistence off. Node ${env.node}, BullMQ ${env.bullmq}. ` +
    'Producer and all workers run in one Node process per run; each run starts from an empty Redis. ' +
    'Every row comes from one run of bench/all.sh on 2026-10-16.',
  '',
  'Redis commands per job count every command Redis executed, including the ones inside Lua scripts. ' +
    'Script calls per job approximate network round trips, since BullMQ and this package do almost everything in scripts. ' +
    'Order inversions count jobs that started before an earlier job of the same group; overlaps count jobs that started while another job of their group was running.',
  '',
);

const throughput = runs.filter(r => r.scenario === 'throughput' && r.params.groups === 1000 && r.params.jobs === 20).sort(byApproach);
if (throughput.length) {
  lines.push('## Drain throughput: 1,000 groups x 20 jobs waiting, no-op jobs, worker concurrency 50', '');
  lines.push(
    'Redis cost is counted end to end (adding the jobs plus running them): plain BullMQ pays for its `add` up front, this package pays for it when a job is released.',
    '',
  );
  lines.push('| Approach | Drain jobs/s | Redis commands/job | Script calls/job | Redis CPU per 1k jobs | Redis connections | Order inversions | Overlaps |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of throughput) {
    const d = r.drain;
    const e = r.enqueue.redis;
    lines.push(
      `| ${label(r)} | ${n(d.jobsPerSec)}${d.finished ? '' : ' (timed out)'} | ${n(e.callsPerJob + d.redis.callsPerJob, 1)} | ${n(e.scriptRunsPerJob + d.redis.scriptRunsPerJob, 1)} | ${n(e.redisCpuMsPer1kJobs + d.redis.redisCpuMsPer1kJobs)} ms | ${n(d.clients)} | ${n(r.order.inversions)} | ${n(r.order.overlaps)} |`,
    );
  }
  lines.push('');
}

const latency = runs.filter(r => r.scenario === 'latency').sort((a, b) => a.params.rate - b.params.rate || byApproach(a, b));
if (latency.length) {
  lines.push('## Open-loop latency: jobs arrive at a fixed rate across 1,000 groups for 30 s, each job takes 5 ms', '');
  lines.push('Latency is from `add()` being called to the processor starting the job.', '');
  lines.push('| Approach | Offered rate | Achieved | p50 | p95 | p99 | Redis commands/job | Redis CPU per 1k jobs | Order inversions | Overlaps |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of latency) {
    const l = r.latency;
    lines.push(
      `| ${label(r)} | ${n(l.offeredRate)}/s | ${n(l.achievedRate)}/s${l.finished ? '' : ' (backlog left)'} | ${ms(l.startMs.p50)} | ${ms(l.startMs.p95)} | ${ms(l.startMs.p99)} | ${n(l.redis.callsPerJob, 1)} | ${n(l.redis.redisCpuMsPer1kJobs)} ms | ${n(r.order.inversions)} | ${n(r.order.overlaps)} |`,
    );
  }
  lines.push('');
}

const scale = runs.filter(r => r.scenario === 'throughput' && !(r.params.groups === 1000 && r.params.jobs === 20)).sort((a, b) => a.params.groups - b.params.groups || byApproach(a, b));
if (scale.length) {
  lines.push('## Many groups: enqueue and drain, worker concurrency 50', '');
  lines.push('| Approach | Groups x jobs | Enqueue jobs/s | Drain jobs/s | Redis memory per waiting job | Keys while waiting | Keys after drain | Redis connections |');
  lines.push('| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of scale) {
    lines.push(
      `| ${label(r)} | ${n(r.params.groups)} x ${r.params.jobs} | ${n(r.enqueue.jobsPerSec)} | ${n(r.drain.jobsPerSec)}${r.drain.finished ? '' : ' (timed out)'} | ${n(r.memory.bytesPerJob)} B | ${n(r.memory.keys)} | ${n(r.drain.keysAfter)} | ${n(r.drain.clients)} |`,
    );
  }
  lines.push('');
}

const skewSizes = [...new Set(runs.filter(r => r.scenario === 'skew').map(r => Number(r.params.hot)))].sort((a, b) => a - b);
for (const hot of skewSizes) {
  const skew = runs.filter(r => r.scenario === 'skew' && Number(r.params.hot) === hot).sort(byApproach);
  const s0 = skew[0].params;
  lines.push(`## Skew: one group with ${n(hot)} jobs added first, then ${n(s0.groups)} groups with one job each; 10 ms jobs, worker concurrency ${s0.concurrency}`, '');
  lines.push('The question is how long the small groups wait behind the big one.', '');
  lines.push('| Approach | All small groups done after | Small-group start latency p50 | p99 | Big-group jobs done meanwhile | Big-group jobs dropped at add | Wasted picks | Redis commands/job | Redis connections |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
  for (const r of skew) {
    const s = r.skew;
    lines.push(
      `| ${label(r)} | ${s.smallGroupsFinished ? ms(s.smallGroupsDoneMs) : `not within ${r.params.timeout} s (${n(s.smallDone)} of ${n(r.params.groups)} done)`} | ${ms(s.smallStartMs.p50)} | ${ms(s.smallStartMs.p99)} | ${n(s.hotDoneMeanwhile)} | ${s.dropped === undefined ? '0' : n(s.dropped)} | ${s.wastedPicks === undefined ? '' : n(s.wastedPicks)} | ${n(s.redis.callsPerJob, 1)} | ${n(s.clients)} |`,
    );
  }
  lines.push('');
  lines.push(
    `The flip side of strict per-group order: at group concurrency 1 the big group runs one job at a time, so its ${n(hot)} x ${s0.workMs} ms jobs need at least ${n((hot * s0.workMs) / 1000)} s to drain (${n((hot * s0.workMs) / 4000)} s at concurrency 4), while plain BullMQ runs them ${s0.concurrency} at a time. That cap is the point when a group's jobs must not overlap, but it is a real cost for a group with a big backlog. These drain times are arithmetic, not measured. Deduplicating by group keeps the small groups fast by dropping the big group's other jobs at add time.`,
    '',
  );
}

const memory = runs.filter(r => r.scenario === 'memory').sort(byApproach);
if (memory.length) {
  const m0 = memory[0].params;
  lines.push(`## Memory: ${n(m0.groups * m0.jobs)} jobs waiting across ${n(m0.groups)} groups, no workers`, '');
  lines.push('| Approach | Redis memory per waiting job | Total Redis memory | Keys | Keys per group | Enqueue jobs/s |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: |');
  for (const r of memory) {
    lines.push(`| ${label(r)} | ${n(r.memory.bytesPerJob)} B | ${n(r.memory.usedMemoryMB, 1)} MB | ${n(r.memory.keys)} | ${n(r.memory.keysPerGroup, 1)} | ${n(r.enqueue.jobsPerSec)} |`);
  }
  lines.push('');
}

const backlog = runs.filter(r => r.scenario === 'backlog').sort(byApproach);
if (backlog.length) {
  const b0 = backlog[0].params;
  lines.push(
    `## Backlog: ${n(b0.groups)} groups with one ${n(b0.workMs / 1000)} s job each, ${b0.workers ?? 4} worker processes' worth of workers at concurrency ${b0.concurrency}, measured for ${b0.duration ?? 75} s`,
    '',
  );
  lines.push(
    "Slow jobs keep most released jobs waiting in BullMQ's wait list for longer than the reconciler's 30 s grace period, as minutes-long syncs do. Every worker runs the reconciler. A separate client pings Redis every 10 ms to measure what everyone else sees.",
    '',
  );
  lines.push('| Approach | Redis CPU | PING p99 | PING max | Top Redis time |');
  lines.push('| --- | ---: | ---: | ---: | --- |');
  for (const r of backlog) {
    const b = r.backlog;
    const top = (b.topMs as [string, number][]).slice(0, 3).map(([k, t]) => `${k} ${n(t)} ms`).join(', ');
    lines.push(`| ${label(r)} | ${n(b.redisCpuPct, 1)}% | ${n(b.pingMs.p99, 1)} ms | ${n(b.pingMs.max, 1)} ms | ${top} |`);
  }
  lines.push('');
}

writeFileSync(join(__dirname, 'RESULTS.md'), lines.join('\n'));
console.log(lines.join('\n'));
