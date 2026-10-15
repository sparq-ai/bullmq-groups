import { EventEmitter } from 'events';
import { randomBytes } from 'crypto';
import { Queue, JobsOptions } from 'bullmq';
import { createGroupRedis, GroupRedis } from './scripts';
import type {
  AddResult,
  FailurePolicy,
  GroupCounts,
  GroupJobOptions,
  GroupQueueOptions,
  GroupStats,
  GroupSummary,
  StuckJob,
} from './types';

/** What we keep for a job until BullMQ has it. */
interface Payload {
  n: string; // job name
  d: unknown; // job data
  o: JobsOptions; // options passed through to BullMQ
  g: string; // group id
  t: number; // when it was added (ms epoch), becomes the BullMQ job's timestamp
}

export type FinalOutcome = 'completed' | 'failed' | 'removed';

const MAX_PRIORITY = 2_097_151;
const NOT_FOR_GROUP_JOBS = ['repeat', 'repeatJobKey', 'lifo', 'parent', 'deduplication', 'debounce', 'timestamp', 'prevMillis'];
const processTag = randomBytes(3).toString('hex');

/**
 * Producer and admin side of a grouped queue.
 *
 * Jobs wait in their group (our Redis keys) and are handed to a normal BullMQ queue of the same
 * name only when the group has a free slot, so BullMQ never holds more than `concurrency` jobs of
 * one group. BullMQ's wait list then serves groups round-robin, and its workers, locks, retries,
 * stalled-job recovery and events all work unchanged.
 */
export class GroupQueue<DataType = any> extends EventEmitter {
  readonly name: string;
  /** The plain BullMQ queue that released jobs go to. Safe to use for getters and dashboards. */
  readonly bull: Queue;
  readonly redis: GroupRedis;
  readonly keyBase: string;
  private readonly metaKey: string;
  private readonly defaultJobOptions: GroupJobOptions;
  private readonly ready: Promise<void>;
  private failurePolicy: FailurePolicy = 'continue';
  private idCounter = 0;
  private closing?: Promise<void>;

  constructor(name: string, opts: GroupQueueOptions) {
    super();
    const prefix = opts.prefix ?? 'bull';
    this.name = name;
    this.bull = new Queue(name, { connection: opts.connection, prefix });
    this.bull.on('error', err => this._error(err));
    this.redis = createGroupRedis(opts.connection);
    this.redis.on('error', err => this._error(err));
    this.keyBase = `${prefix}:${name}:gq:`;
    this.metaKey = `${this.keyBase}meta`;
    this.defaultJobOptions = opts.defaultJobOptions ?? {};
    this.ready = this.writeConfig(opts);
  }

  private async writeConfig(opts: GroupQueueOptions): Promise<void> {
    const fields: string[] = [];
    if (opts.groupConcurrency !== undefined) {
      assertConcurrency(opts.groupConcurrency);
      fields.push('concurrency', String(opts.groupConcurrency));
    }
    if (opts.failurePolicy !== undefined) {
      fields.push('failurePolicy', opts.failurePolicy);
    }
    if (opts.reconcileGraceMs !== undefined) {
      if (!Number.isInteger(opts.reconcileGraceMs) || opts.reconcileGraceMs < 1) throw new Error('reconcileGraceMs must be a positive integer');
      fields.push('grace', String(opts.reconcileGraceMs));
    }
    if (fields.length) {
      await this.redis.hset(this.metaKey, ...fields);
    }
    this.failurePolicy = ((await this.redis.hget(this.metaKey, 'failurePolicy')) as FailurePolicy | null) ?? 'continue';
  }

  /** Add a job to the tail of its group. Runs once every earlier job of the group has finished. */
  async add(groupId: string, name: string, data: DataType, opts?: GroupJobOptions): Promise<AddResult> {
    const [result] = await this.addBulk([{ groupId, name, data, opts }]);
    return result;
  }

  /** Add many jobs in one round trip. Jobs of the same group keep their order in the array. */
  async addBulk(
    jobs: { groupId: string; name: string; data: DataType; opts?: GroupJobOptions }[],
  ): Promise<AddResult[]> {
    await this.ready;
    const args: (string | number)[] = [];
    const ids: string[] = [];
    const now = Date.now();
    for (const job of jobs) {
      assertGroupId(job.groupId);
      const { jobId, delay = 0, priority = 0, ...rest } = { ...this.defaultJobOptions, ...job.opts };
      const id = jobId ?? this.nextJobId();
      assertJobId(id);
      if (!Number.isInteger(priority) || priority < 0 || priority > MAX_PRIORITY) {
        throw new Error(`priority must be an integer between 0 and ${MAX_PRIORITY}`);
      }
      if (!Number.isFinite(delay) || delay < 0) {
        throw new Error('delay must be a non-negative number of milliseconds');
      }
      for (const key of NOT_FOR_GROUP_JOBS) {
        if (key in rest) throw new Error(`option '${key}' is not supported for group jobs`);
      }
      // A blocked group is unblocked by retrying the failed job, so it must stay in BullMQ.
      const bullOpts: JobsOptions = this.failurePolicy === 'block' ? { ...rest, removeOnFail: false } : rest;
      const payload: Payload = { n: job.name, d: job.data, o: bullOpts, g: job.groupId, t: now };
      args.push(id, job.groupId, JSON.stringify(payload), Math.ceil(delay), priority);
      ids.push(id);
    }
    const [statuses, released] = (await this.redis.gq_enqueue(this.metaKey, this.keyBase, jobs.length, ...args)) as [
      AddResult['status'][],
      string[],
    ];
    await this.dispatch(released);
    return ids.map((id, i) => ({ id, status: statuses[i] }));
  }

  /** Stop starting jobs of this group, on every worker. Jobs already running finish normally. */
  async pauseGroup(groupId: string): Promise<void> {
    await this.control('pause', groupId);
  }

  async resumeGroup(groupId: string): Promise<void> {
    await this.control('resume', groupId);
  }

  /** Override how many jobs of one group may run at once; null falls back to the queue default. */
  async setGroupConcurrency(groupId: string, concurrency: number | null): Promise<void> {
    if (concurrency !== null) assertConcurrency(concurrency);
    await this.control('concurrency', groupId, concurrency === null ? '' : String(concurrency));
  }

  /** Groups held by a job that failed for good under failurePolicy=block. */
  async getBlockedGroups(): Promise<{ groupId: string; jobId: string }[]> {
    const flat = await this.redis.hgetall(`${this.keyBase}blocked`);
    return Object.entries(flat).map(([groupId, jobId]) => ({ groupId, jobId }));
  }

  /** Run the failed job that blocks this group again. The group stays held until it finishes. */
  async retryBlockedGroup(groupId: string): Promise<string | null> {
    const [status, jobId] = await this.control('retry-blocked', groupId);
    if (status !== 'ok') return null;
    const job = await this.bull.getJob(jobId);
    if (job) {
      await job.retry('failed');
    } else {
      // The failed job is gone from BullMQ; nothing to retry, so let the group move on.
      await this._ack(jobId, 'removed');
    }
    return jobId;
  }

  /** Give up on the job that blocks this group and continue with the group's next job. */
  async skipBlockedGroup(groupId: string): Promise<string | null> {
    const [status, jobId] = await this.control('skip-blocked', groupId);
    return status === 'ok' ? jobId : null;
  }

  async getGroupCounts(groupId: string): Promise<GroupCounts> {
    const k = this.keyBase;
    const res = await this.redis
      .multi()
      .llen(`${k}w:${groupId}`)
      .zcard(`${k}p:${groupId}`)
      .hget(`${k}active`, groupId)
      .hget(`${k}conc`, groupId)
      .hget(`${k}meta`, 'concurrency')
      .sismember(`${k}paused`, groupId)
      .hget(`${k}blocked`, groupId)
      .zscore(`${k}groups`, groupId)
      .exec();
    const v = (i: number) => res![i][1] as string | number | null;
    return {
      waiting: Number(v(0)) + Number(v(1)),
      prioritized: Number(v(1)),
      active: Number(v(2) ?? 0),
      concurrency: Number(v(3) ?? v(4) ?? 1),
      paused: v(5) === 1,
      blockedJobId: (v(6) as string | null) ?? null,
      headReadyAt: v(7) === null ? null : Number(v(7)),
    };
  }

  /** Groups with waiting jobs, most behind first. */
  async getGroups(start = 0, end = 99): Promise<GroupSummary[]> {
    const k = this.keyBase;
    const [flat, now] = await Promise.all([this.redis.zrange(`${k}groups`, start, end, 'WITHSCORES'), this.serverTime()]);
    if (!flat.length) return [];
    const pipe = this.redis.pipeline();
    for (let i = 0; i < flat.length; i += 2) {
      pipe.llen(`${k}w:${flat[i]}`).zcard(`${k}p:${flat[i]}`).hget(`${k}active`, flat[i]);
    }
    const res = (await pipe.exec())!;
    const groups: GroupSummary[] = [];
    for (let i = 0, j = 0; i < flat.length; i += 2, j += 3) {
      groups.push({
        id: flat[i],
        lagMs: Math.max(0, now - Number(flat[i + 1])),
        waiting: Number(res[j][1]) + Number(res[j + 1][1]),
        active: Number(res[j + 2][1] ?? 0),
      });
    }
    return groups;
  }

  /** Queue-wide numbers for dashboards and alerts. All O(1) or O(log n). */
  async getStats(): Promise<GroupStats> {
    const k = this.keyBase;
    const [res, now] = await Promise.all([
      this.redis
        .multi()
        .zcard(`${k}groups`)
        .hlen(`${k}active`)
        .hget(`${k}meta`, 'waiting')
        .hlen(`${k}inflight`)
        .zcard(`${k}delayed`)
        .scard(`${k}paused`)
        .hlen(`${k}blocked`)
        .zrange(`${k}groups`, 0, 0, 'WITHSCORES')
        .exec(),
      this.serverTime(),
    ]);
    const v = (i: number) => res![i][1] as any;
    const oldest = v(7) as string[];
    return {
      waitingGroups: Number(v(0)),
      activeGroups: Number(v(1)),
      waiting: Number(v(2) ?? 0),
      inflight: Number(v(3)),
      delayed: Number(v(4)),
      pausedGroups: Number(v(5)),
      blockedGroups: Number(v(6)),
      oldestWaitingMs: oldest.length ? Math.max(0, now - Number(oldest[1])) : 0,
    };
  }

  /** Jobs that have been in flight longer than `olderThanMs` (running, retrying or waiting in BullMQ). */
  async getStuckJobs(olderThanMs: number): Promise<StuckJob[]> {
    const now = await this.serverTime();
    const stuck: StuckJob[] = [];
    let cursor = '0';
    do {
      const [next, flat] = await this.redis.hscan(`${this.keyBase}inflight`, cursor, 'COUNT', 500);
      cursor = next;
      for (let i = 0; i < flat.length; i += 2) {
        const rec = JSON.parse(flat[i + 1]) as { g: string; t: number; b?: number };
        if (now - rec.t > olderThanMs) {
          stuck.push({ jobId: flat[i], groupId: rec.g, state: rec.b ? 'blocked' : 'inflight', ageMs: now - rec.t });
        }
      }
    } while (cursor !== '0');
    return stuck.sort((a, b) => b.ageMs - a.ageMs);
  }

  async close(): Promise<void> {
    this.closing ??= (async () => {
      await this.ready.catch(() => undefined);
      await Promise.allSettled([this.bull.close(), this.redis.quit()]);
    })();
    return this.closing;
  }

  // ---- internals used by GroupWorker and the reconciler ----

  /** Emit 'error' only when someone listens: an unhandled 'error' event would crash the process. */
  _error(err: unknown): void {
    if (this.listenerCount('error') > 0) this.emit('error', err);
    else console.error(`[bullmq-groups:${this.name}]`, err);
  }

  /** A released job reached a final state in BullMQ: free its slot and release the group's next job. */
  async _ack(jobId: string, outcome: FinalOutcome): Promise<void> {
    const [status, groupId, released] = (await this.redis.gq_ack(this.metaKey, this.keyBase, jobId, outcome)) as [
      string,
      string,
      string[],
    ];
    if (status === 'blocked') this.emit('blocked', groupId, jobId);
    await this.dispatch(released);
  }

  async _markAdded(jobId: string, nonce: string | number): Promise<void> {
    await this.redis.gq_markAdded(this.metaKey, this.keyBase, jobId, nonce);
  }

  /** Re-add a released job whose BullMQ add never happened (producer crashed in between). */
  async _redispatch(jobId: string, nonce: string | number): Promise<boolean> {
    const raw = await this.redis.hget(`${this.keyBase}data`, jobId);
    if (raw === null) return false;
    await this.dispatch([jobId, String(nonce), raw]);
    return true;
  }

  async _promoteDelayed(limit = 500): Promise<number | null> {
    const [nextDue, released] = (await this.redis.gq_promote(this.metaKey, this.keyBase, limit)) as [string, string[]];
    await this.dispatch(released);
    return nextDue === '-1' ? null : Number(nextDue);
  }

  async _lease(limit: number): Promise<{ now: number; records: { jobId: string; groupId: string; added: boolean; releasedAt: number; nonce: number; blocked: boolean }[] }> {
    const [now, flat] = (await this.redis.gq_lease(this.metaKey, this.keyBase, limit)) as [string, string[]];
    const records = [];
    for (let i = 0; i < flat.length; i += 2) {
      const rec = JSON.parse(flat[i + 1]) as { g: string; s: 'P' | 'A'; t: number; r: number; b?: number };
      records.push({ jobId: flat[i], groupId: rec.g, added: rec.s === 'A', releasedAt: rec.t, nonce: rec.r, blocked: !!rec.b });
    }
    return { now: Number(now), records };
  }

  async _kick(start: number, count: number): Promise<{ next: number; repaired: string[] }> {
    const [next, released, repaired] = (await this.redis.gq_kick(this.metaKey, this.keyBase, start, count)) as [
      string,
      string[],
      string[],
    ];
    await this.dispatch(released);
    return { next: Number(next), repaired };
  }

  /**
   * Hand released jobs to BullMQ, then confirm. If this process dies in between, the job stays
   * unconfirmed and the reconciler adds it after the grace period; the explicit job id makes a
   * double add harmless.
   */
  private async dispatch(released: string[]): Promise<void> {
    if (!released.length) return;
    const jobs: { name: string; data: unknown; opts: JobsOptions }[] = [];
    const confirm: string[] = [];
    for (let i = 0; i < released.length; i += 3) {
      const [jobId, nonce, raw] = [released[i], released[i + 1], released[i + 2]];
      if (!raw) {
        this._error(new Error(`group job ${jobId} was released without a payload`));
        continue;
      }
      const p = JSON.parse(raw) as Payload;
      // `group.id` is the same field BullMQ Pro uses, so processors can read job.opts.group.id.
      jobs.push({ name: p.n, data: p.d, opts: { ...p.o, jobId, timestamp: p.t, group: { id: p.g } } as JobsOptions });
      confirm.push(jobId, nonce);
    }
    if (jobs.length === 1) {
      await this.bull.add(jobs[0].name, jobs[0].data, jobs[0].opts);
    } else if (jobs.length > 1) {
      await this.bull.addBulk(jobs);
    }
    if (confirm.length) {
      await this.redis.gq_markAdded(this.metaKey, this.keyBase, ...confirm);
    }
  }

  private async control(op: string, groupId: string, arg = ''): Promise<[string, string]> {
    assertGroupId(groupId);
    await this.ready;
    const [status, info, released] = (await this.redis.gq_control(this.metaKey, this.keyBase, op, groupId, arg)) as [
      string,
      string,
      string[],
    ];
    await this.dispatch(released);
    return [status, info];
  }

  private async serverTime(): Promise<number> {
    const [s, us] = await this.redis.time();
    return Number(s) * 1000 + Math.floor(Number(us) / 1000);
  }

  private nextJobId(): string {
    this.idCounter = (this.idCounter + 1) % 1_679_616; // keeps the counter part at most 4 base-36 chars
    return `g${processTag}${Date.now().toString(36)}${this.idCounter.toString(36)}`;
  }
}

function assertGroupId(groupId: string): void {
  if (typeof groupId !== 'string' || groupId.length === 0 || groupId.length > 256) {
    throw new Error('groupId must be a non-empty string of at most 256 characters');
  }
}

function assertJobId(jobId: string): void {
  if (typeof jobId !== 'string' || !jobId || jobId.includes(':') || `${parseInt(jobId, 10)}` === jobId) {
    throw new Error(`invalid jobId '${jobId}': must be a non-empty string, not an integer, without ':'`);
  }
}

function assertConcurrency(n: number): void {
  if (!Number.isInteger(n) || n < 0) throw new Error('group concurrency must be a non-negative integer');
}
