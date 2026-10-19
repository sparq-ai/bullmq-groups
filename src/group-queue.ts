import { randomBytes } from 'crypto';
import { Job, Queue } from 'bullmq';
import type { Redis } from 'ioredis';
import type { JobsOptions, ObliterateOpts, QueueListener } from 'bullmq';
import { bullmqClient, createGroupRedis, GroupRedis, isRedisInstance, untilReady } from './scripts';
import { GroupJob } from './group-job';
import type { GroupAware } from './group-job';
import { GroupMaxSizeExceededError, GroupStatus } from './types';
import type {
  BlockedGroup,
  FailurePolicy,
  GroupCounts,
  GroupJobsOptions,
  GroupLag,
  GroupQueueOptions,
  GroupStats,
  JobGroupOptions,
  RejectedJob,
  StuckJob,
} from './types';

/** What we keep for a job until BullMQ has it. */
interface Payload {
  n: string; // job name
  d: unknown; // job data
  o: JobsOptions; // options passed through to BullMQ
  g: string; // group id
  t: number; // when it was added (ms epoch), becomes the BullMQ job's timestamp
  p?: number; // group.priority
  m?: number; // group.maxSize
  l?: number; // delay
  u?: string; // deduplication id
}

/** A released job, on its way to BullMQ. */
interface Released {
  jobId: string;
  nonce: string;
  groupId: string;
  name: string;
  data: unknown;
  opts: JobsOptions;
}

/** A grouped job checked and turned into the enqueue script's arguments. */
interface Prepared {
  args: (string | number)[];
  name: string;
  data: unknown;
  opts: GroupJobsOptions;
}

export type AddStatus = 'waiting' | 'delayed' | 'duplicate' | 'maxed';
export type FinalOutcome = 'completed' | 'failed' | 'removed' | 'rejected';
export type ClaimVerdict = 'run' | 'paused' | 'stale' | 'ghost' | 'adopted' | 'wait';

/** Jobs per enqueue script call: keeps one call from blocking Redis for long. */
const CHUNK = 1000;
const MAX_PRIORITY = 2_097_152;
const NOT_FOR_GROUP_JOBS = ['repeat', 'repeatJobKey', 'lifo', 'parent', 'debounce', 'timestamp', 'prevMillis'];
/** Ids that would collide with BullMQ's own keys under the queue prefix. */
const RESERVED_IDS = new Set([
  'id', 'meta', 'events', 'wait', 'waiting', 'paused', 'active', 'completed', 'failed', 'delayed', 'prioritized',
  'stalled', 'stalled-check', 'limiter', 'repeat', 'marker', 'pc', 'de', 'metrics', 'gq',
]);

/** Events a GroupQueue adds to BullMQ's Queue events. */
export interface GroupQueueListener {
  /** BullMQ refused a released job (for example sizeLimit exceeded); it was dropped from its group. */
  rejected: (job: RejectedJob) => void;
  /** A job failed for good under failurePolicy 'block', so its group stopped. */
  blocked: (groupId: string, jobId: string) => void;
}

/** What BullMQ's own getJob resolves to for these type parameters. */
type BullJob<D, R, N extends string> = NonNullable<Resolved<ReturnType<Queue<D, R, N, D, R, N>['getJob']>>>;
type Resolved<T> = T extends Promise<infer U> ? U : never;

/**
 * Options a released job sets explicitly, because BullMQ merges the queue's defaultJobOptions into
 * every add again: the group already applied the delay, and priority, lifo and deduplication are
 * not BullMQ's business for a job its group released. `delay` is undefined, not 0: BullMQ before
 * 5.41.0 stores any numeric delay, 0 included, as a delayed job. (Not `debounce`: BullMQ 6
 * refuses the key even when undefined.)
 */
const RELEASED = { delay: undefined, priority: 0, lifo: false, deduplication: undefined } as const;

const processTag = randomBytes(4).toString('hex');
let idCounter = 0;

/** Runs BullMQ's own option checks (the ones Queue.add runs) without adding anything. */
class OptionCheck extends Job {
  check(): void {
    this.validateOptions(this.asJSON());
  }
}

/**
 * A BullMQ queue with groups, shaped like BullMQ Pro's QueuePro.
 *
 * A job added with `{ group: { id } }` waits in its group (our Redis keys) and is handed to this
 * queue's normal BullMQ wait list only when the group has a free slot, so BullMQ never holds more
 * than the group's share of jobs. BullMQ's wait list then serves groups in turn, and its workers,
 * locks, retries, stalled-job recovery and events all work unchanged. A job added without a group
 * is a plain BullMQ job.
 *
 * Every Queue method works. getJob(), remove() and drain() also see jobs still waiting in a group;
 * the other getters (getJobCounts, getWaiting ...) count only jobs BullMQ has, as in BullMQ Pro,
 * where getGroupsJobsCount() counts the rest.
 *
 * Extra events: 'rejected' (RejectedJob), 'blocked' (groupId, jobId).
 */
export class GroupQueue<DataType = any, ResultType = any, NameType extends string = string>
  extends Queue<DataType, ResultType, NameType, DataType, ResultType, NameType>
  implements GroupAware
{
  readonly keyBase: string;
  private readonly metaKey: string;
  private readonly isWorker: boolean;
  /** Settings this instance writes to the meta hash (a GroupWorker's group options). */
  private readonly config?: { set: string[]; del: string[] };
  private _redis?: GroupRedis;
  private configured?: Promise<void>;
  private groupClosing?: Promise<void>;

  constructor(name: string, opts?: GroupQueueOptions, Connection?: ConstructorParameters<typeof Queue>[2]) {
    const { groupDefaults, worker, ...queueOpts } = opts ?? ({} as GroupQueueOptions);
    // Validate before touching Redis, so a bad option throws here, not later as an unhandled rejection.
    const config = groupDefaults ? configFields(groupDefaults) : undefined;
    super(name, queueOpts, Connection);
    this.config = config;
    this.isWorker = !!worker;
    this.keyBase = `${this.qualifiedName}:gq:`;
    this.metaKey = `${this.keyBase}meta`;
    if (this.config) this.ensureConfig().catch(err => this._error(err));
  }

  /** Every job this queue hands out is a GroupJob. */
  protected get Job(): typeof Job {
    return GroupJob as typeof Job;
  }

  on<U extends keyof QueueListener<GroupJob<DataType, ResultType, NameType>>>(
    event: U,
    listener: QueueListener<GroupJob<DataType, ResultType, NameType>>[U],
  ): this;
  on<U extends keyof GroupQueueListener>(event: U, listener: GroupQueueListener[U]): this;
  on(event: string, listener: (...args: any[]) => void): this;
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event as never, listener as never);
  }

  once<U extends keyof QueueListener<GroupJob<DataType, ResultType, NameType>>>(
    event: U,
    listener: QueueListener<GroupJob<DataType, ResultType, NameType>>[U],
  ): this;
  once<U extends keyof GroupQueueListener>(event: U, listener: GroupQueueListener[U]): this;
  once(event: string, listener: (...args: any[]) => void): this;
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event as never, listener as never);
  }

  off<U extends keyof QueueListener<GroupJob<DataType, ResultType, NameType>>>(
    event: U,
    listener: QueueListener<GroupJob<DataType, ResultType, NameType>>[U],
  ): this;
  off<U extends keyof GroupQueueListener>(event: U, listener: GroupQueueListener[U]): this;
  off(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this {
    return super.off(event as never, listener as never);
  }

  /**
   * The connection for the group scripts, opened on first use, so a queue that never sees a
   * grouped job costs nothing extra. Separate from BullMQ's connections so the package relies only
   * on BullMQ's public API and works the same on BullMQ 5 and 6.
   */
  get redis(): GroupRedis {
    if (!this._redis) {
      this._redis = createGroupRedis(this.opts.connection, { worker: this.isWorker });
      this._redis.on('error', err => this._error(err));
    }
    return this._redis;
  }

  /** Writes this instance's settings once. A failed write (Redis down at startup) is retried by the next call. */
  private ensureConfig(): Promise<void> {
    this.configured ??= this.writeConfig().catch(err => {
      this.configured = undefined;
      throw err;
    });
    return this.configured;
  }

  private async writeConfig(): Promise<void> {
    if ((this.redis.options as { enableOfflineQueue?: boolean }).enableOfflineQueue === false) await untilReady(this.redis);
    if (!this.config) return;
    const { set, del } = this.config;
    const touchesConcurrency = set.includes('concurrency') || del.includes('concurrency');
    const previous = touchesConcurrency ? await this.redis.hget(this.metaKey, 'concurrency') : null;
    const tx = this.redis.multi();
    if (set.length) tx.hset(this.metaKey, ...set);
    if (del.length) tx.hdel(this.metaKey, ...del);
    await tx.exec();
    const i = set.indexOf('concurrency');
    const next = i >= 0 ? set[i + 1] : null;
    if (touchesConcurrency && previous !== next) {
      // A new default limit applies to groups that are already waiting now, not when the reconciler reaches them.
      await this.repairAll();
    }
  }

  // ---- adding jobs ----

  /**
   * Add a job. With `opts.group` it joins the tail of its group and runs once the group has room
   * (BullMQ Pro's QueuePro.add); without, it is a plain BullMQ job.
   * Throws GroupMaxSizeExceededError when group.maxSize is set and reached.
   */
  async add(name: NameType, data: DataType, opts?: GroupJobsOptions): Promise<GroupJob<DataType, ResultType, NameType>> {
    if (!opts?.group) return (await super.add(name, data, opts)) as unknown as GroupJob<DataType, ResultType, NameType>;
    const [added] = await this._addGroupJobs([{ name, data, opts }], true);
    if (added.status === 'maxed') throw new GroupMaxSizeExceededError(added.job.gid!, added.job.opts.group!.maxSize!);
    this.emitGroup('waiting', added.job);
    return added.job;
  }

  /**
   * Add many jobs, grouped and plain mixed. Jobs of the same group keep their order in the array.
   * Every grouped job is checked first (including BullMQ's own option checks), so an invalid job
   * throws before anything is stored. Grouped jobs are stored 1,000 per script call; each call is
   * atomic. If storing a later chunk fails, the error carries `results` for the jobs already
   * stored, so a caller can retry only the rest. group.maxSize is not supported here, as in Pro.
   *
   * Resolves once the jobs are stored. Handing released jobs to BullMQ is retried by the
   * reconciler, so a failure at that step is reported through 'error' instead of thrown: a caller
   * that retried would add the job twice.
   */
  async addBulk(
    jobs: { name: NameType; data: DataType; opts?: GroupJobsOptions }[],
  ): Promise<GroupJob<DataType, ResultType, NameType>[]> {
    const grouped: number[] = [];
    const plain: number[] = [];
    jobs.forEach((job, i) => (job.opts?.group ? grouped : plain).push(i));
    if (!grouped.length) return (await super.addBulk(jobs)) as unknown as GroupJob<DataType, ResultType, NameType>[];
    const prepared = grouped.map(i => this.prepare(jobs[i], false));
    const plainJobs = plain.length ? await super.addBulk(plain.map(i => jobs[i])) : [];
    const added = await this.store(prepared);
    const result: GroupJob<DataType, ResultType, NameType>[] = new Array(jobs.length);
    plain.forEach((at, k) => (result[at] = plainJobs[k] as unknown as GroupJob<DataType, ResultType, NameType>));
    grouped.forEach((at, k) => (result[at] = added[k].job));
    return result;
  }

  /** @internal Grouped adds with each job's outcome: 'waiting', 'delayed', 'duplicate' or 'maxed'. */
  async _addGroupJobs(
    jobs: { name: NameType; data: DataType; opts?: GroupJobsOptions }[],
    allowMaxSize = false,
  ): Promise<{ job: GroupJob<DataType, ResultType, NameType>; status: AddStatus }[]> {
    return this.store(jobs.map(job => this.prepare(job, allowMaxSize)));
  }

  private async store(prepared: Prepared[]): Promise<{ job: GroupJob<DataType, ResultType, NameType>; status: AddStatus }[]> {
    await this.ensureConfig();
    const results: { job: GroupJob<DataType, ResultType, NameType>; status: AddStatus }[] = [];
    for (let i = 0; i < prepared.length; i += CHUNK) {
      const part = prepared.slice(i, i + CHUNK);
      let reply: [AddStatus[], string[], string[]];
      try {
        reply = (await this.redis.gq_enqueue(this.metaKey, this.keyBase, part.length, part.map(p => p.args).flat())) as typeof reply;
      } catch (err) {
        if (!results.length) throw err;
        throw Object.assign(
          new Error(`addBulk stored the first ${results.length} of ${prepared.length} grouped jobs, then failed: ${(err as Error).message}`),
          { results, cause: err },
        );
      }
      const [statuses, ids, released] = reply;
      await this.dispatchOrReport(released);
      part.forEach((p, k) => {
        const job = new this.Job(this, p.name, p.data, p.opts, ids[k]) as GroupJob<DataType, ResultType, NameType>;
        results.push({ job, status: statuses[k] });
      });
    }
    return results;
  }

  /** Validates one grouped job and turns it into the enqueue script's arguments. */
  private prepare(job: { name: NameType; data: DataType; opts?: GroupJobsOptions }, allowMaxSize: boolean): Prepared {
    const explicit = (job.opts ?? {}) as GroupJobsOptions & Record<string, unknown>;
    const group = groupOption(explicit.group);
    if (group.maxSize !== undefined && !allowMaxSize) {
      throw new Error('group.maxSize is not supported by addBulk (as in BullMQ Pro); add the job with add()');
    }
    if (explicit.priority !== undefined) throw new Error('priority is not supported for group jobs; use group.priority');
    for (const key of NOT_FOR_GROUP_JOBS) {
      if (explicit[key] !== undefined) throw new Error(`option '${key}' is not supported for group jobs`);
    }
    // Queue defaults apply, minus the ones a group job cannot take.
    const defaults = this.defaultJobOptions as Record<string, unknown>;
    for (const key of [...NOT_FOR_GROUP_JOBS, 'priority', 'deduplication', 'group', 'jobId']) delete defaults[key];
    const { group: _group, jobId, delay, priority: _priority, deduplication, ...rest } = { ...defaults, ...explicit } as GroupJobsOptions;
    const id = jobId ?? nextJobId();
    assertJobId(id);
    const wait = delay ?? 0;
    if (!Number.isFinite(wait) || wait < 0) throw new Error('delay must be a non-negative number of milliseconds');
    if (deduplication !== undefined) {
      const { id: dedupId, ...modes } = deduplication as { id?: unknown };
      if (typeof dedupId !== 'string' || !dedupId) throw new Error('deduplication.id must be a non-empty string');
      if (Object.keys(modes).length) throw new Error(`deduplication supports only { id } for group jobs (got ${Object.keys(modes).join(', ')})`);
    }
    // BullMQ's own checks, with the options the job will carry, so a job BullMQ would refuse is refused now.
    new OptionCheck(this, job.name, job.data, { ...rest, jobId: id }, id).check();
    const payload: Payload = { n: job.name, d: job.data, o: rest, g: group.id, t: Date.now() };
    if (group.priority) payload.p = group.priority;
    if (group.maxSize) payload.m = group.maxSize;
    if (wait) payload.l = wait;
    if (deduplication) payload.u = deduplication.id;
    return {
      args: [id, group.id, JSON.stringify(payload), Math.ceil(wait), group.priority ?? 0, deduplication?.id ?? '', group.maxSize ?? 0],
      name: job.name,
      data: job.data,
      opts: this.jobOpts(id, payload),
    };
  }

  /** The options a job still waiting in its group shows, as it was added. */
  private jobOpts(jobId: string, p: Payload): GroupJobsOptions {
    const group: JobGroupOptions = { id: p.g };
    if (p.p) group.priority = p.p;
    if (p.m) group.maxSize = p.m;
    const opts: GroupJobsOptions = { ...p.o, jobId, timestamp: p.t, group };
    if (p.l) opts.delay = p.l;
    if (p.u) opts.deduplication = { id: p.u };
    return opts;
  }

  private jobFromPayload(jobId: string, p: Payload): GroupJob<DataType, ResultType, NameType> {
    return new this.Job(this, p.n as NameType, p.d as DataType, this.jobOpts(jobId, p), jobId) as GroupJob<DataType, ResultType, NameType>;
  }

  // ---- jobs waiting in a group are jobs too ----

  /** A job in BullMQ, or one still waiting in its group. */
  async getJob(jobId: string): Promise<(GroupJob<DataType, ResultType, NameType> & BullJob<DataType, ResultType, NameType>) | undefined> {
    const job = await super.getJob(jobId);
    if (job) return job as GroupJob<DataType, ResultType, NameType> & typeof job;
    const raw = await (await this.bullClient()).hget(`${this.keyBase}data`, jobId);
    return raw ? (this.jobFromPayload(jobId, JSON.parse(raw) as Payload) as GroupJob<DataType, ResultType, NameType> & BullJob<DataType, ResultType, NameType>) : undefined;
  }

  /**
   * Remove a job, including one still waiting in its group. Returns 1 when removed, as BullMQ does.
   * Removing a released job that BullMQ holds frees its group slot at once.
   */
  async remove(jobId: string, opts?: { removeChildren?: boolean }): Promise<number> {
    if (await this._removePending(jobId)) return 1;
    const nonce = releaseNonce(await this._bullJob(jobId));
    const removed = await super.remove(jobId, opts);
    if (removed === 1 && nonce !== undefined) await this._releaseRemoved(jobId, nonce);
    return removed;
  }

  /** BullMQ's drain, plus every job waiting in a group (and delayed group jobs when `delayed`). */
  async drain(delayed = false): Promise<void> {
    await super.drain(delayed);
    if (!(await this.hasGroupKeys())) return;
    await this.deleteGroups();
    if (delayed) {
      while (Number(await this.redis.gq_remove(this.metaKey, this.keyBase, 'delayed', CHUNK)) === CHUNK);
    }
    // BullMQ's drain took released jobs with it, and their groups still count them. Have the workers'
    // reconcilers check every release on their next pass, not at its own (backed-off) check time.
    const checks = `${this.keyBase}checks`;
    await this.redis.zunionstore(checks, 1, checks, 'WEIGHTS', 0);
  }

  /** @internal A released job left BullMQ without finishing (removed by hand): free its group slot now. */
  async _releaseRemoved(jobId: string, nonce: number): Promise<void> {
    try {
      await this.ensureConfig();
      await this._ack(jobId, 'removed', nonce);
    } catch (err) {
      // The job is already gone from BullMQ, so the removal stands. The reconciler frees the slot instead.
      this._error(err);
    }
  }

  /** @internal */
  async _pendingState(jobId: string): Promise<'waiting' | 'prioritized' | 'delayed' | null> {
    // BullMQ's own connection: a plain queue opens no extra one for this.
    const k = this.keyBase;
    const res = await (await this.bullClient()).multi().hget(`${k}data`, jobId).hexists(`${k}dmeta`, jobId).hexists(`${k}inflight`, jobId).exec();
    const raw = res![0][1] as string | null;
    if (!raw) return null;
    if (res![1][1] === 1) return 'delayed';
    if (res![2][1] === 1) return 'waiting'; // released, on its way into BullMQ
    return (JSON.parse(raw) as Payload).p ? 'prioritized' : 'waiting';
  }

  /** @internal */
  async _removePending(jobId: string): Promise<boolean> {
    if (!(await (await this.bullClient()).hexists(`${this.keyBase}data`, jobId))) return false;
    await this.ensureConfig();
    const removed = Number(await this.redis.gq_remove(this.metaKey, this.keyBase, 'job', jobId)) === 1;
    if (removed) this.emitGroup('removed', jobId);
    return removed;
  }

  // ---- BullMQ Pro's group methods ----

  /**
   * Stop starting jobs of this group, on every worker. Running jobs finish; released jobs that have
   * not started wait for resume. Works for a group that has no jobs yet. False if it was already paused.
   */
  async pauseGroup(groupId: string | number): Promise<boolean> {
    const [, changed] = await this.control('pause', gidOf(groupId));
    return changed === '1';
  }

  /** False if the group was not paused. */
  async resumeGroup(groupId: string | number): Promise<boolean> {
    const [, changed] = await this.control('resume', gidOf(groupId));
    return changed === '1';
  }

  /**
   * Give one group its own limit, over the workers' group.concurrency. Stored in Redis until
   * removeGroupConcurrency(). Resolves to 1 when the group had no limit of its own before, else 0.
   */
  async setGroupConcurrency(groupId: string | number, concurrency: number, opts?: { ttlSeconds?: number }): Promise<number> {
    if (opts?.ttlSeconds !== undefined) throw new Error('setGroupConcurrency: ttlSeconds is not supported');
    assertConcurrency(concurrency);
    const [, added] = await this.control('concurrency', gidOf(groupId), String(concurrency));
    return Number(added);
  }

  /** The group's limit: its own, else the workers' group.concurrency, else Infinity (no limit). */
  async getGroupConcurrency(groupId: string | number): Promise<number> {
    const res = await this.redis.multi().hget(`${this.keyBase}conc`, gidOf(groupId)).hget(this.metaKey, 'concurrency').exec();
    const value = (res![0][1] as string | null) ?? (res![1][1] as string | null);
    return value === null ? Infinity : Number(value);
  }

  /** Drop the group's own limit; it falls back to the workers' group.concurrency. Resolves to 1 if it had one. */
  async removeGroupConcurrency(groupId: string | number): Promise<number> {
    const [, removed] = await this.control('concurrency', gidOf(groupId), '');
    return Number(removed);
  }

  /** @deprecated BullMQ Pro's former name of removeGroupConcurrency. */
  deleteGroupConcurrency(groupId: string | number): Promise<number> {
    return this.removeGroupConcurrency(groupId);
  }

  /** Jobs waiting in the group, in the order they will run (unprioritized first). Inclusive range, as Pro. */
  async getGroupJobs(groupId: string | number, start = 0, end = -1): Promise<GroupJob<DataType, ResultType, NameType>[]> {
    const gid = gidOf(groupId);
    const k = this.keyBase;
    const lens = await this.redis.multi().llen(`${k}w:${gid}`).zcard(`${k}p:${gid}`).exec();
    const [wLen, pLen] = [Number(lens![0][1]), Number(lens![1][1])];
    const [from, to] = clampRange(start, end, wLen + pLen);
    if (to < from) return [];
    const ids: string[] = [];
    if (from < wLen) {
      const entries = await this.redis.lrange(`${k}w:${gid}`, from, Math.min(to, wLen - 1));
      ids.push(...entries.map(idOfEntry));
    }
    if (to >= wLen) {
      const members = await this.redis.zrange(`${k}p:${gid}`, Math.max(0, from - wLen), to - wLen);
      ids.push(...members.map(m => idOfEntry(m.slice(17))));
    }
    if (!ids.length) return [];
    const raws = await this.redis.hmget(`${k}data`, ...ids);
    return ids.flatMap((id, i) => (raws[i] ? [this.jobFromPayload(id, JSON.parse(raws[i]!) as Payload)] : []));
  }

  /** Jobs waiting in the group. */
  async getGroupJobsCount(groupId: string | number): Promise<number> {
    const gid = gidOf(groupId);
    const res = await this.redis.multi().llen(`${this.keyBase}w:${gid}`).zcard(`${this.keyBase}p:${gid}`).exec();
    return Number(res![0][1]) + Number(res![1][1]);
  }

  /** Jobs waiting in any group, prioritized or not. O(1); `limit` is accepted for Pro compatibility. */
  async getGroupsJobsCount(_limit?: number): Promise<number> {
    return Number((await this.redis.hget(this.metaKey, 'waiting')) ?? 0);
  }

  /** Jobs of the group handed to workers and not finished: running, retrying, or about to start. */
  async getGroupActiveCount(groupId: string | number): Promise<number> {
    return Number((await this.redis.hget(`${this.keyBase}active`, gidOf(groupId))) ?? 0);
  }

  /** Groups with jobs waiting, longest-waiting first, with their status. Inclusive range, as Pro. */
  async getGroups(start = 0, end = -1): Promise<{ id: string; status: GroupStatus }[]> {
    const ids = await this.redis.zrange(`${this.keyBase}groups`, start, end);
    const statuses = await this.statusesOf(ids);
    return statuses.filter(s => s.status).map(({ id, status }) => ({ id, status: status as GroupStatus }));
  }

  /** Number of groups with jobs waiting. */
  async getGroupsCount(): Promise<number> {
    return this.redis.zcard(`${this.keyBase}groups`);
  }

  /** The group's status, or null when it has no jobs waiting and is neither paused nor blocked. */
  async getGroupStatus(groupId: string | number): Promise<GroupStatus | null> {
    const [s] = await this.statusesOf([gidOf(groupId)]);
    return (s.status as GroupStatus) || null;
  }

  /** Groups with jobs waiting that are in `status`, with how many jobs wait in each. Reads every group: O(groups). */
  async getGroupsByStatus(status: GroupStatus, start = 0, end = -1): Promise<{ id: string; count: number }[]> {
    const matched = (await this.allStatuses()).filter(s => s.status === status).map(({ id, count }) => ({ id, count }));
    const [from, to] = clampRange(start, end, matched.length);
    return matched.slice(from, to + 1);
  }

  /** How many groups with jobs waiting are in each status. Reads every group: O(groups). */
  async getGroupsCountByStatus(): Promise<{ waiting: number; paused: number; limited: number; maxed: number; blocked: number }> {
    const counts = { waiting: 0, paused: 0, limited: 0, maxed: 0, blocked: 0 };
    for (const s of await this.allStatuses()) counts[s.status as keyof typeof counts]++;
    return counts;
  }

  /** Waiting jobs of the group per priority; priority 0 counts the jobs without one. */
  async getCountsPerPriorityForGroup(groupId: string, priorities: number[]): Promise<{ [index: string]: number }> {
    const gid = gidOf(groupId);
    const tx = this.redis.multi();
    for (const p of priorities) {
      if (p === 0) tx.llen(`${this.keyBase}w:${gid}`);
      else tx.zcount(`${this.keyBase}p:${gid}`, p, p);
    }
    const res = await tx.exec();
    return Object.fromEntries(priorities.map((p, i) => [String(p), Number(res![i][1])]));
  }

  /** Remove every job waiting in the group. Running and delayed jobs are not touched. */
  async deleteGroup(groupId: string | number): Promise<void> {
    const gid = gidOf(groupId);
    while (Number((await this.control('delete', gid, String(CHUNK)))[1]) === CHUNK);
  }

  /** Remove every job waiting in any group. */
  async deleteGroups(): Promise<void> {
    for (;;) {
      const ids = await this.redis.zrange(`${this.keyBase}groups`, 0, CHUNK - 1);
      if (!ids.length) return;
      for (const id of ids) await this.deleteGroup(id);
    }
  }

  /**
   * Recount the group's in-flight jobs and release what fits: repairs a group whose count drifted
   * (a write lost in a Redis failover) so that it looks full with nothing running.
   * Reads every in-flight record of the queue.
   */
  async repairMaxedGroup(groupId: string | number): Promise<{ previous: number; active: number }> {
    await this.ensureConfig();
    const [previous, active, released] = (await this.redis.gq_repair(this.metaKey, this.keyBase, gidOf(groupId))) as [number, number, string[]];
    await this.dispatchOrReport(released);
    return { previous: Number(previous), active: Number(active) };
  }

  // ---- failurePolicy 'block' (not in BullMQ Pro) ----

  /** Groups held by jobs that failed for good under failurePolicy 'block'. */
  async getBlockedGroups(): Promise<BlockedGroup[]> {
    const groups = Object.keys(await this.redis.hgetall(`${this.keyBase}blocked`));
    if (!groups.length) return [];
    const pipe = this.redis.pipeline();
    for (const g of groups) pipe.lrange(`${this.keyBase}b:${g}`, 0, -1);
    const res = (await pipe.exec())!;
    return groups
      .map((groupId, i) => ({ groupId, jobIds: res[i][1] as string[] }))
      .filter(g => g.jobIds.length)
      .map(g => ({ groupId: g.groupId, jobId: g.jobIds[0], jobIds: g.jobIds }));
  }

  /**
   * Run the oldest job blocking this group again, with a fresh set of attempts. It keeps the
   * group's slot, so nothing behind it starts until it finishes. Returns its id, or null when
   * nothing was blocked, or when the job is gone from BullMQ (the group then moves on without it).
   */
  async retryBlockedGroup(groupId: string | number): Promise<string | null> {
    const gid = gidOf(groupId);
    const [status, jobId] = await this.control('blocked-head', gid);
    if (status !== 'ok') return null;
    const job = await this._bullJob(jobId);
    if (!job) {
      this._error(new Error(`blocked job ${jobId} of group ${gid} is gone from BullMQ, so it cannot be retried; skipping it`));
      await this.control('skip-blocked', gid, jobId);
      return null;
    }
    // Retry first, unblock second: if this process dies in between, the job still runs and its ack unblocks the group.
    if (await job.isFailed()) await job.retry('failed', { resetAttemptsMade: true });
    const [unblocked] = await this.control('unblock', gid, jobId);
    if (unblocked !== 'ok') {
      // Someone skipped it meanwhile, so the group has moved on: take the retried copy back out before it runs.
      await job.remove().catch(() => undefined);
      return null;
    }
    return jobId;
  }

  /** Give up on the oldest job blocking this group. The group moves on once no job blocks it. */
  async skipBlockedGroup(groupId: string | number): Promise<string | null> {
    const gid = gidOf(groupId);
    // Name the job first, so a skip resent after a reconnect cannot skip a second one.
    const [head, jobId] = await this.control('blocked-head', gid);
    if (head !== 'ok') return null;
    const [status] = await this.control('skip-blocked', gid, jobId);
    return status === 'ok' ? jobId : null;
  }

  // ---- observability (not in BullMQ Pro) ----

  async getGroupCounts(groupId: string | number): Promise<GroupCounts> {
    const gid = gidOf(groupId);
    const k = this.keyBase;
    const res = await this.redis
      .multi()
      .llen(`${k}w:${gid}`)
      .zcard(`${k}p:${gid}`)
      .hget(`${k}active`, gid)
      .hget(`${k}conc`, gid)
      .hget(`${k}meta`, 'concurrency')
      .sismember(`${k}paused`, gid)
      .lindex(`${k}b:${gid}`, 0)
      .zscore(`${k}groups`, gid)
      .exec();
    const v = (i: number) => res![i][1] as string | number | null;
    const limit = v(3) ?? v(4);
    return {
      waiting: Number(v(0)) + Number(v(1)),
      prioritized: Number(v(1)),
      active: Number(v(2) ?? 0),
      concurrency: limit === null ? Infinity : Number(limit),
      paused: v(5) === 1,
      blockedJobId: (v(6) as string | null) ?? null,
      headReadyAt: v(7) === null ? null : Number(v(7)),
    };
  }

  /** Groups with waiting jobs, most behind first, with how long their next job has been ready. */
  async getLaggingGroups(start = 0, end = 99): Promise<GroupLag[]> {
    const k = this.keyBase;
    const [flat, now] = await Promise.all([this.redis.zrange(`${k}groups`, start, end, 'WITHSCORES'), this.serverTime()]);
    if (!flat.length) return [];
    const pipe = this.redis.pipeline();
    for (let i = 0; i < flat.length; i += 2) {
      pipe.llen(`${k}w:${flat[i]}`).zcard(`${k}p:${flat[i]}`).hget(`${k}active`, flat[i]);
    }
    const res = (await pipe.exec())!;
    const groups: GroupLag[] = [];
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

  // ---- lifecycle ----

  /**
   * BullMQ's obliterate, then every group key. Stop workers first. Scans one node, so on Redis
   * Cluster all keys must share a hash-tagged prefix.
   */
  async obliterate(opts?: ObliterateOpts): Promise<void> {
    await this.configured?.catch(() => undefined);
    await super.obliterate(opts);
    const client = await this.bullClient();
    const pattern = `${this.keyBase.replace(/[*?[\]\\]/g, '\\$&')}*`;
    let cursor = '0';
    do {
      const [next, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 1000);
      cursor = next;
      if (keys.length) await client.del(...keys);
    } while (cursor !== '0');
    this.configured = undefined;
  }

  /** Walk every waiting group once and release what has room, e.g. after a default-concurrency change. */
  async repairAll(): Promise<string[]> {
    const repaired: string[] = [];
    let cursor = '0';
    do {
      const r = await this._kick(cursor, CHUNK);
      repaired.push(...r.repaired);
      cursor = r.next;
    } while (cursor !== '0');
    return repaired;
  }

  async close(): Promise<void> {
    this.groupClosing ??= (async () => {
      await this.configured?.catch(() => undefined);
      await Promise.allSettled([super.close(), this._redis?.quit()]);
    })();
    return this.groupClosing;
  }

  /**
   * @internal A GroupWorker's close(true): drop the connections at once. In a Redis outage, worker
   * commands wait forever (maxRetriesPerRequest: null), and a QUIT would queue behind them.
   */
  async _forceClose(): Promise<void> {
    this.groupClosing ??= (async () => {
      this._redis?.disconnect();
      // BullMQ's own connection may hold queued commands too. Never touch a connection the app passed in.
      if (!isRedisInstance(this.opts.connection)) {
        const client = await Promise.race([bullmqClient(this), new Promise<undefined>(resolve => setTimeout(() => resolve(undefined), 100))]);
        client?.disconnect();
      }
      await Promise.allSettled([super.close()]);
    })();
    return this.groupClosing;
  }

  // ---- internals used by GroupWorker and the reconciler ----

  /** Emit 'error' only when someone listens: an unhandled 'error' event would crash the process. */
  _error(err: unknown): void {
    if (this.listenerCount('error') > 0) this.emit('error', err as Error);
    else console.error(`[bullmq-groups:${this.name}]`, err);
  }

  /** The job as BullMQ has it, ignoring jobs that wait in a group. */
  _bullJob(jobId: string): Promise<Job | undefined> {
    return super.getJob(jobId);
  }

  /** Add released jobs to BullMQ (BullMQ's own addBulk, not the grouped one). */
  async _addReleased(jobs: { name: NameType; data: DataType; opts: JobsOptions }[]): Promise<void> {
    await super.addBulk(jobs as Parameters<Queue<DataType, ResultType, NameType, DataType, ResultType, NameType>['addBulk']>[0]);
  }

  /**
   * A released job reached a final state in BullMQ: free its slot and release the group's next job.
   * `nonce` limits the ack to that release; `requireState` to a release still in that state.
   * Returns 'ok', 'blocked' or 'none' (nothing to do).
   */
  async _ack(jobId: string, outcome: FinalOutcome, nonce?: number, requireState?: 'P' | 'A'): Promise<string> {
    const [status, groupId, released] = (await this.redis.gq_ack(
      this.metaKey,
      this.keyBase,
      jobId,
      outcome,
      nonce ?? '',
      requireState ?? '',
    )) as [string, string, string[]];
    if (status === 'blocked') this.emitGroup('blocked', groupId, jobId);
    await this.dispatch(released);
    return status;
  }

  async _markAdded(jobId: string, nonce: number): Promise<void> {
    await this.redis.gq_markAdded(this.metaKey, this.keyBase, jobId, nonce);
  }

  /** Re-add a released job whose BullMQ add never happened (producer crashed in between, add lost). */
  async _redispatch(jobId: string, nonce: number): Promise<boolean> {
    const res = await this.redis.multi().hget(`${this.keyBase}data`, jobId).hget(this.metaKey, 'failurePolicy').exec();
    const raw = res![0][1] as string | null;
    if (raw === null) return false;
    await this.dispatch([(res![1][1] as string | null) ?? 'continue', jobId, String(nonce), raw]);
    return true;
  }

  /** The start check a GroupWorker runs before a job's processor. Also returns jobs released by it. */
  async _claim(jobId: string, nonce: number, groupId: string, ranBefore: boolean): Promise<[ClaimVerdict, FailurePolicy, string[]]> {
    return (await this.redis.gq_claim(this.metaKey, this.keyBase, jobId, nonce, groupId, ranBefore ? '1' : '0')) as [
      ClaimVerdict,
      FailurePolicy,
      string[],
    ];
  }

  /** Promote due delayed jobs. Returns ms until the next delayed job is due (Redis clock), or null when none. */
  async _promoteDelayed(limit = 500): Promise<number | null> {
    const [dueIn, released] = (await this.redis.gq_promote(this.metaKey, this.keyBase, limit)) as [string, string[]];
    await this.dispatch(released);
    return dueIn === '-1' ? null : Number(dueIn);
  }

  async _lease(limit: number): Promise<{
    now: number;
    records: { jobId: string; groupId: string; added: boolean; releasedAt: number; nonce: number; blocked: boolean }[];
  }> {
    const [now, flat] = (await this.redis.gq_lease(this.metaKey, this.keyBase, limit)) as [string, string[]];
    const records = [];
    for (let i = 0; i < flat.length; i += 2) {
      const rec = JSON.parse(flat[i + 1]) as { g: string; s: 'P' | 'A'; t: number; r: number; b?: number };
      records.push({ jobId: flat[i], groupId: rec.g, added: rec.s === 'A', releasedAt: rec.t, nonce: rec.r, blocked: !!rec.b });
    }
    return { now: Number(now), records };
  }

  /**
   * One kick pass over the next page of the group index. ZSCAN, not ranks: releases move groups
   * around the index, and a full scan still visits every group that waits throughout.
   */
  async _kick(cursor: string, count: number): Promise<{ next: string; repaired: string[] }> {
    const [next, flat] = await this.redis.zscan(`${this.keyBase}groups`, cursor, 'COUNT', count);
    const groups = flat.filter((_, i) => i % 2 === 0);
    const [released, repaired] = (await this.redis.gq_kick(this.metaKey, this.keyBase, groups)) as [string[], string[]];
    await this.dispatch(released);
    return { next, repaired };
  }

  /**
   * Hand released jobs to BullMQ, then confirm. `released` is [failurePolicy, jobId, nonce,
   * payload, ...]. If this process dies in between, the job stays unconfirmed and the reconciler
   * adds it again a few seconds later: the job id makes a double add harmless, and workers skip a
   * copy that is not the current release.
   */
  async _dispatch(released: string[]): Promise<void> {
    await this.dispatch(released);
  }

  /** dispatch() for callers that must not fail once the jobs are stored: the reconciler retries the BullMQ step. */
  private async dispatchOrReport(released: string[]): Promise<void> {
    try {
      await this.dispatch(released);
    } catch (err) {
      this._error(err);
    }
  }

  private async dispatch(released: string[]): Promise<void> {
    if (released.length < 4) return;
    const policy = released[0];
    const jobs: Released[] = [];
    for (let i = 1; i < released.length; i += 3) {
      const [jobId, nonce, raw] = [released[i], released[i + 1], released[i + 2]];
      if (!raw) {
        this._error(new Error(`group job ${jobId} was released without a payload`));
        continue;
      }
      const p = JSON.parse(raw) as Payload;
      // job.opts.group is the group option as added (id, priority, maxSize), plus `r`: the release nonce.
      const opts = { ...p.o, ...RELEASED, jobId, timestamp: p.t, group: { ...this.jobOpts(jobId, p).group, r: Number(nonce) } } as JobsOptions;
      // Under 'block' a failed job must stay in BullMQ so retryBlockedGroup can run it again.
      if (policy === 'block') opts.removeOnFail = false;
      jobs.push({ jobId, nonce, groupId: p.g, name: p.n, data: p.d, opts });
    }
    await this.dispatchJobs(jobs, true);
  }

  private async dispatchJobs(jobs: Released[], retryForeign: boolean): Promise<void> {
    if (!jobs.length) return;
    let added = jobs;
    try {
      await this.addToBull(jobs);
    } catch (err) {
      // One job that fails BullMQ's option checks fails the whole batch: drop it from its group and
      // add the rest. Any Redis error (connection, OOM, failover) is left to the reconciler, which
      // adds the jobs again; a Redis error never drops a job.
      const invalid = jobs.map(j => this.optionError(j));
      if (!invalid.some(Boolean)) throw err;
      added = jobs.filter((_, i) => !invalid[i]);
      for (const [i, job] of jobs.entries()) if (invalid[i]) await this.reject(job, invalid[i]!);
      if (added.length) await this.addToBull(added);
    }
    if (!added.length) return;
    const foreign = await this.redis.gq_markAdded(this.metaKey, this.keyBase, added.flatMap(j => [j.jobId, j.nonce]));
    if (retryForeign && Array.isArray(foreign) && foreign.length) {
      await this.makeRoom(added.filter(j => (foreign as string[]).includes(j.jobId)));
    }
  }

  /**
   * BullMQ did not take these jobs: another job holds the id (an older copy of the same job that
   * already finished), or it has none (an add lost in a Redis failover). Remove a finished older
   * copy and add ours once more; anything else is left to the reconciler.
   */
  private async makeRoom(jobs: Released[]): Promise<void> {
    const retry: Released[] = [];
    for (const job of jobs) {
      const existing = await this._bullJob(job.jobId);
      if (!existing) {
        retry.push(job);
      } else if (existing.finishedOn && (existing.opts as { group?: unknown }).group) {
        await existing.remove();
        retry.push(job);
      }
    }
    await this.dispatchJobs(retry, false);
  }

  private async addToBull(jobs: Released[]): Promise<void> {
    await this._addReleased(jobs.map(j => ({ name: j.name as NameType, data: j.data as DataType, opts: j.opts })));
  }

  private optionError(job: Released): Error | null {
    try {
      new OptionCheck(this, job.name, job.data, job.opts, job.jobId).check();
      return null;
    } catch (err) {
      return err as Error;
    }
  }

  /** BullMQ will never take this job (for example sizeLimit exceeded): drop it so its group can go on. */
  private async reject(job: Released, error: Error): Promise<void> {
    const status = await this._ack(job.jobId, 'rejected', Number(job.nonce));
    if (status !== 'ok') return;
    const rejected: RejectedJob = { jobId: job.jobId, groupId: job.groupId, error };
    this.emitGroup('rejected', rejected);
    this._error(new Error(`BullMQ refused group job ${job.jobId} of group ${job.groupId}, so it was dropped: ${error.message}`));
  }

  private async control(op: string, groupId: string, arg = ''): Promise<[string, string]> {
    await this.ensureConfig();
    const [status, info, released] = (await this.redis.gq_control(this.metaKey, this.keyBase, op, groupId, arg)) as [
      string,
      string,
      string[],
      string,
    ];
    await this.dispatchOrReport(released);
    return [status, info];
  }

  private async statusesOf(ids: string[]): Promise<{ id: string; status: string; count: number }[]> {
    const out: { id: string; status: string; count: number }[] = [];
    for (let i = 0; i < ids.length; i += CHUNK) {
      const part = ids.slice(i, i + CHUNK);
      const flat = (await this.redis.gq_status(this.metaKey, this.keyBase, part)) as (string | number)[];
      part.forEach((id, k) => out.push({ id, status: String(flat[2 * k]), count: Number(flat[2 * k + 1]) }));
    }
    return out;
  }

  private async allStatuses(): Promise<{ id: string; status: string; count: number }[]> {
    const out: { id: string; status: string; count: number }[] = [];
    let cursor = '0';
    do {
      const [next, flat] = await this.redis.zscan(`${this.keyBase}groups`, cursor, 'COUNT', CHUNK);
      cursor = next;
      out.push(...(await this.statusesOf(flat.filter((_, i) => i % 2 === 0))).filter(s => s.status));
    } while (cursor !== '0');
    // ZSCAN may return a member twice.
    return [...new Map(out.map(s => [s.id, s])).values()];
  }

  /** Whether this queue ever had a grouped job, checked on BullMQ's connection so a plain queue opens no extra one. */
  private async hasGroupKeys(): Promise<boolean> {
    return (await (await this.bullClient()).exists(this.metaKey)) === 1;
  }

  /** BullMQ's own connection, so a plain queue opens no extra one; the group connection if BullMQ hides it. */
  private async bullClient(): Promise<Redis> {
    return (await bullmqClient(this)) ?? (this.redis as Redis);
  }

  private emitGroup(event: string, ...args: unknown[]): void {
    (this.emit as (event: string, ...args: unknown[]) => boolean)(event, ...args);
  }

  private async serverTime(): Promise<number> {
    const [s, us] = await this.redis.time();
    return Number(s) * 1000 + Math.floor(Number(us) / 1000);
  }
}

/**
 * Generated ids: g<process tag><time, base 36><counter, 4 base-36 digits>. The counter is shared by
 * every GroupQueue in the process, so two instances adding in the same millisecond never collide.
 */
function nextJobId(): string {
  idCounter = (idCounter + 1) % 1_679_616;
  return `g${processTag}${Date.now().toString(36)}${idCounter.toString(36).padStart(4, '0')}`;
}

/** Validates the meta settings a GroupWorker writes. null concurrency removes the default limit. */
export function configFields(d: NonNullable<GroupQueueOptions['groupDefaults']>): { set: string[]; del: string[] } {
  const set: string[] = [];
  const del: string[] = [];
  if (d.concurrency === null) {
    del.push('concurrency');
  } else if (d.concurrency !== undefined) {
    assertConcurrency(d.concurrency);
    set.push('concurrency', String(d.concurrency));
  }
  if (d.failurePolicy !== undefined) {
    if (d.failurePolicy !== 'continue' && d.failurePolicy !== 'block') {
      throw new Error(`failurePolicy must be 'continue' or 'block', got '${d.failurePolicy}'`);
    }
    set.push('failurePolicy', d.failurePolicy);
  }
  if (d.reconcileGraceMs !== undefined) {
    if (!Number.isInteger(d.reconcileGraceMs) || d.reconcileGraceMs < 1) throw new Error('reconcileGraceMs must be a positive integer');
    set.push('grace', String(d.reconcileGraceMs));
  }
  return { set, del };
}

/** The release nonce a BullMQ job carries when its group released it, or undefined (a plain job). */
export function releaseNonce(job: Job | undefined): number | undefined {
  return (job?.opts as { group?: { r?: number } } | undefined)?.group?.r;
}

function groupOption(group: unknown): JobGroupOptions {
  if (typeof group !== 'object' || group === null) throw new Error('group must be an object: { id, priority?, maxSize? }');
  const { id, priority, maxSize, ...rest } = group as JobGroupOptions & Record<string, unknown>;
  const unknown = Object.keys(rest);
  if (unknown.length) throw new Error(`group option(s) not supported: ${unknown.join(', ')}`);
  const out: JobGroupOptions = { id: gidOf(id) };
  if (priority !== undefined) {
    if (!Number.isInteger(priority) || priority < 0 || priority > MAX_PRIORITY) {
      throw new Error(`group.priority must be an integer from 0 to ${MAX_PRIORITY}`);
    }
    if (priority > 0) out.priority = priority;
  }
  if (maxSize !== undefined) {
    if (!Number.isInteger(maxSize) || maxSize < 1) throw new Error('group.maxSize must be a positive integer');
    out.maxSize = maxSize;
  }
  return out;
}

/** Group ids are strings in Redis; numbers are accepted, as in BullMQ Pro. */
function gidOf(groupId: unknown): string {
  const gid = typeof groupId === 'number' && Number.isFinite(groupId) ? String(groupId) : groupId;
  if (typeof gid !== 'string' || gid.length === 0 || gid.length > 256) {
    throw new Error('group id must be a non-empty string (or a number) of at most 256 characters');
  }
  return gid;
}

function assertJobId(jobId: string): void {
  if (typeof jobId !== 'string' || !jobId || jobId.includes(':') || `${parseInt(jobId, 10)}` === jobId || RESERVED_IDS.has(jobId)) {
    throw new Error(`invalid jobId '${jobId}': must be a non-empty string, not an integer, without ':', and not a BullMQ key name`);
  }
}

export function assertConcurrency(n: number): void {
  if (!Number.isInteger(n) || n < 1) throw new Error('group concurrency must be a positive integer');
}

/** "<readyAt>:<jobId>" -> jobId */
function idOfEntry(entry: string): string {
  return entry.slice(entry.indexOf(':') + 1);
}

/** Redis-style inclusive range over `length` items, clamped. */
function clampRange(start: number, end: number, length: number): [number, number] {
  const from = Math.max(0, start < 0 ? length + start : start);
  const to = Math.min(length - 1, end < 0 ? length + end : end);
  return [from, to];
}
