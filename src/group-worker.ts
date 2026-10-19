import { DelayedError, Job, Processor, Worker, WorkerListener } from 'bullmq';
import type { ConnectionOptions } from 'bullmq';
import { assertConcurrency, configFields, GroupQueue } from './group-queue';
import { bullmqClient } from './scripts';
import { GroupJob } from './group-job';
import type { GroupAware } from './group-job';
import type { FailurePolicy, GroupWorkerOptions, RejectedJob, SkippedJob, StuckJob, WorkerGroupOptions } from './types';

const DEFAULTS = {
  reconcileIntervalMs: 5_000,
  stuckAfterMs: 15 * 60_000,
  delayedPollMs: 1_000,
  recheckMs: 5_000,
};
const LEASE_BATCH = 500;
const KICK_BATCH = 2_000;
let warnedStalledCount = false;

/** Events a GroupWorker adds to BullMQ's Worker events. */
export interface GroupWorkerListener<DataType = any, ResultType = any, NameType extends string = string>
  extends WorkerListener<DataType, ResultType, NameType> {
  /** A job has been running for longer than stuckAfterMs. Reported by the reconciler, at most once per check. */
  stuck: (job: StuckJob) => void;
  /** A job failed for good under failurePolicy 'block', so its group stopped. */
  blocked: (groupId: string, jobId: string) => void;
  /** The reconciler released jobs of groups that had room but no release (lost writes, a new default concurrency). */
  repaired: (groupIds: string[]) => void;
  /** A BullMQ job the worker did not run because it is not the release its group is waiting for. */
  skipped: (job: SkippedJob) => void;
  /** BullMQ refused to add a released job; it was dropped from its group. */
  rejected: (job: RejectedJob) => void;
}

export interface ReconcileSummary {
  checked: number;
  acked: number;
  redispatched: number;
  confirmed: number;
  stuck: StuckJob[];
  repaired: string[];
}

/** A processor function, as BullMQ's, with the job typed as a GroupJob (job.gid, job.opts.group), as WorkerPro's ProcessorPro. */
export type GroupProcessor<DataType = any, ResultType = any, NameType extends string = string> = (
  job: GroupJob<DataType, ResultType, NameType>,
  token?: string,
  signal?: AbortSignal,
) => Promise<ResultType>;

type GroupOpts = { group?: { id?: string; r?: number } };

function groupOf(job: Job): { id?: string; r?: number } | undefined {
  return (job.opts as GroupOpts).group;
}

/** Did this BullMQ job run before (a retry of a job that already finished), as opposed to a fresh copy? */
function ranBefore(job: Job): boolean {
  return job.attemptsStarted > 1 || job.attemptsMade > 0 || (job.stacktrace?.length ?? 0) > 0;
}

/** Validates the `group` worker option (BullMQ Pro's shape) into the settings stored in Redis. */
function groupSettings(group: WorkerGroupOptions | undefined, reconcileGraceMs: number | undefined) {
  if (group !== undefined && (typeof group !== 'object' || group === null)) throw new Error('group must be an object');
  const { concurrency, failurePolicy, ...rest } = (group ?? {}) as WorkerGroupOptions & Record<string, unknown>;
  if ('limit' in rest) throw new Error('group.limit (group rate limiting) is not supported by bullmq-groups');
  const unknown = Object.keys(rest);
  if (unknown.length) throw new Error(`group worker option(s) not supported: ${unknown.join(', ')}`);
  if (concurrency !== undefined) assertConcurrency(concurrency);
  const settings = {
    concurrency: concurrency ?? null,
    failurePolicy: (failurePolicy ?? 'continue') as FailurePolicy,
    reconcileGraceMs,
  };
  configFields(settings); // throws on a bad failurePolicy or grace
  return settings;
}

/**
 * A BullMQ Worker that keeps groups moving, shaped like BullMQ Pro's WorkerPro.
 *
 * It is a real `Worker`: the processor (a function or a sandboxed file) is BullMQ's, and every
 * BullMQ worker option, method and event works. Jobs added without a group run as on any worker.
 * On top of that:
 *  - When a job completes, or fails after its last attempt, the worker frees the job's group slot
 *    and releases the group's next job. A job that fails and will be retried keeps its slot.
 *  - Before the processor runs, a start check makes sure the BullMQ job is the release its group
 *    is waiting for. A stale copy of the same job id (a late or resent add) is skipped, a job of a
 *    paused group waits for resume, and a finished job retried from a dashboard runs only in a
 *    free slot of its group.
 *  - A small reconciler covers the gaps a crash can leave between BullMQ's scripts and ours
 *    (see README, "Crash windows").
 *
 * The `group` options are written to Redis, so every worker of a queue that declares them should
 * use the same ones. A worker created without `group` options never writes them: it serves group
 * jobs by what the queue's other workers wrote, and starts its group bookkeeping only once its
 * queue has group jobs, so a plain queue pays nothing for it.
 *
 * Extra events: 'stuck', 'blocked', 'repaired', 'skipped', 'rejected' (see GroupWorkerListener).
 */
export class GroupWorker<DataType = any, ResultType = any, NameType extends string = string>
  extends Worker<DataType, ResultType, NameType>
  implements GroupAware
{
  private readonly settings: ReturnType<typeof groupSettings>;
  private readonly declaresGroups: boolean;
  private readonly maint: typeof DEFAULTS;
  private readonly groupConnection: ConnectionOptions;
  private readonly bookkeeping = new Set<Promise<unknown>>();
  /** Ids of skipped copies, whose 'completed' event is not emitted: their processor never ran. */
  private readonly skippedCopies = new Set<string>();
  private groupQueue?: GroupQueue<DataType>;
  private reconcileTimer?: NodeJS.Timeout;
  private promoteTimer?: NodeJS.Timeout;
  private probeTimer?: NodeJS.Timeout;
  private kickCursor = '0';
  private reconciling = false;
  private maintenance = false;
  private loops = false;
  private groupClosing?: Promise<void>;

  constructor(
    name: string,
    processor: string | URL | null | GroupProcessor<DataType, ResultType, NameType>,
    opts: GroupWorkerOptions,
  ) {
    const { group, reconcileIntervalMs, reconcileGraceMs, stuckAfterMs, delayedPollMs, recheckMs, ...workerOpts } = opts;
    // Validate before touching Redis, so a bad option throws here.
    const settings = groupSettings(group, reconcileGraceMs);
    // Started below, once the group bookkeeping exists.
    super(name, processor as string | URL | null | Processor<DataType, ResultType, NameType>, { ...workerOpts, autorun: false });
    this.settings = settings;
    this.declaresGroups = group !== undefined;
    this.groupConnection = opts.connection;
    this.maint = {
      reconcileIntervalMs: reconcileIntervalMs ?? DEFAULTS.reconcileIntervalMs,
      stuckAfterMs: stuckAfterMs ?? DEFAULTS.stuckAfterMs,
      delayedPollMs: delayedPollMs ?? DEFAULTS.delayedPollMs,
      recheckMs: recheckMs ?? DEFAULTS.recheckMs,
    };
    this.on('completed', job => {
      const g = groupOf(job);
      if (g?.r !== undefined) this.track(this.queue._ack(job.id!, 'completed', g.r));
    });
    this.on('failed', job => {
      // BullMQ emits 'failed' for every failed attempt; finishedOn is only set on the last one.
      if (job?.id && job.finishedOn && groupOf(job)?.r !== undefined) this.track(this.onFinalFailure(job));
    });
    if (workerOpts.autorun !== false) this.run().catch(err => this.emit('error', err));
  }

  /** Every job this worker hands its processor is a GroupJob (job.gid is its group id). */
  protected get Job(): typeof Job {
    return GroupJob as typeof Job;
  }

  /** The group side of this worker's queue. Created on first use. */
  get queue(): GroupQueue<DataType> {
    return this.groupQueue ?? this.activate();
  }

  on<U extends keyof GroupWorkerListener<DataType, ResultType, NameType>>(
    event: U,
    listener: GroupWorkerListener<DataType, ResultType, NameType>[U],
  ): this {
    return super.on(event as keyof WorkerListener<DataType, ResultType, NameType>, listener as never);
  }

  once<U extends keyof GroupWorkerListener<DataType, ResultType, NameType>>(
    event: U,
    listener: GroupWorkerListener<DataType, ResultType, NameType>[U],
  ): this {
    return super.once(event as keyof WorkerListener<DataType, ResultType, NameType>, listener as never);
  }

  off<U extends keyof GroupWorkerListener<DataType, ResultType, NameType>>(
    event: U,
    listener: GroupWorkerListener<DataType, ResultType, NameType>[U],
  ): this {
    return super.off(event as keyof WorkerListener<DataType, ResultType, NameType>, listener as never);
  }

  /**
   * Start processing when created with autorun: false. A worker that declares group options writes
   * them to Redis now, so producers release by them; one created with autorun: false and never run
   * (a processor switched off by a flag) opens nothing.
   */
  async run(): Promise<void> {
    if (this.declaresGroups && !this.groupClosing) this.activate();
    this.startMaintenance();
    return super.run();
  }

  emit<U extends keyof WorkerListener<DataType, ResultType, NameType>>(
    event: U,
    ...args: Parameters<WorkerListener<DataType, ResultType, NameType>[U]>
  ): boolean {
    if (event === 'completed' && this.skippedCopies.size) {
      const job = args[0] as Job | undefined;
      if (job?.id && this.skippedCopies.delete(job.id)) return false;
    }
    return super.emit(event, ...args);
  }

  /**
   * Graceful by default: stops the reconciler, lets running jobs finish, then waits for their
   * group bookkeeping, so the next job of each group is already in BullMQ for another worker.
   * With `force`, it waits for neither and drops the group connection at once (in a Redis outage
   * the bookkeeping would never finish). Another worker's reconciler completes what it left.
   */
  async close(force = false): Promise<void> {
    this.groupClosing ??= (async () => {
      clearTimeout(this.reconcileTimer);
      clearTimeout(this.promoteTimer);
      clearTimeout(this.probeTimer);
      await super.close(force);
      if (force) {
        await this.groupQueue?._forceClose();
        return;
      }
      while (this.bookkeeping.size) await Promise.allSettled([...this.bookkeeping]);
      await this.groupQueue?.close();
    })();
    return this.groupClosing;
  }

  /** @internal */
  async _pendingState(jobId: string): Promise<'waiting' | 'prioritized' | 'delayed' | null> {
    return (await this.hasGroups()) ? this.queue._pendingState(jobId) : null;
  }

  /** @internal */
  async _removePending(jobId: string): Promise<boolean> {
    return (await this.hasGroups()) ? this.queue._removePending(jobId) : false;
  }

  /** @internal */
  async _releaseRemoved(jobId: string, nonce: number): Promise<void> {
    await this.queue._releaseRemoved(jobId, nonce);
  }

  /** The start check (see the class comment). Runs for function and sandboxed processors alike. */
  protected async callProcessJob(job: Job<DataType, ResultType, NameType>, token: string, signal?: AbortSignal): Promise<ResultType> {
    const group = groupOf(job);
    if (!group?.id || group.r === undefined) {
      // Not released by this package (added without a group): it runs outside any group.
      return super.callProcessJob(job, token, signal);
    }
    const queue = this.queue;
    let verdict: string;
    let policy: string;
    let released: string[];
    try {
      [verdict, policy, released] = await queue._claim(job.id!, group.r, group.id, ranBefore(job));
    } catch (err) {
      // Closing, so the connection is gone: don't start the job. BullMQ's stalled check hands it on.
      if (this.groupClosing) throw err;
      // Redis trouble right at the start: run the job rather than fail it. The check only guards against rare copies.
      this.emit('error', err as Error);
      return super.callProcessJob(job, token, signal);
    }
    // A group with no limit hands its turn on as its job starts. The reconciler re-adds this if it is lost.
    if (released.length) this.track(queue._dispatch(released));
    if (verdict === 'run' || verdict === 'adopted') {
      // The policy in force now decides whether a final failure stays in BullMQ for retryBlockedGroup.
      if (policy === 'block') job.opts.removeOnFail = false;
      return super.callProcessJob(job, token, signal);
    }
    if (verdict === 'paused' || verdict === 'wait') {
      await job.moveToDelayed(Date.now() + this.maint.recheckMs, token);
      throw new DelayedError();
    }
    // A stale or ghost copy: its processor does not run. BullMQ completes it, so remove it on
    // completion and emit no 'completed' for it (BullMQ's QueueEvents stream still records one).
    job.opts.removeOnComplete = true;
    this.skippedCopies.add(job.id!);
    const skipped: SkippedJob = { jobId: job.id!, groupId: group.id, reason: verdict as SkippedJob['reason'] };
    this.emitGroup('skipped', skipped);
    return undefined as ResultType;
  }

  /** One reconciler pass. Exposed for tests and for running it from a cron instead of every worker. */
  async reconcile(): Promise<ReconcileSummary> {
    const summary: ReconcileSummary = { checked: 0, acked: 0, redispatched: 0, confirmed: 0, stuck: [], repaired: [] };
    const queue = this.queue;
    const { now, records } = await queue._lease(LEASE_BATCH);
    summary.checked = records.length;
    // One HGETALL per job. getJobState would scan BullMQ's wait list for every job still waiting in it.
    const jobs = await Promise.all(records.map(r => queue._bullJob(r.jobId)));
    await Promise.all(
      records.map(async (rec, i) => {
        const job = jobs[i];
        if (!job || groupOf(job)?.r !== rec.nonce) {
          if (rec.added) {
            // BullMQ had this release and no longer does: it finished and was removed, or someone removed it by hand.
            if ((await queue._ack(rec.jobId, 'removed', rec.nonce, 'A')) === 'ok') summary.acked++;
          } else if (job) {
            // An older copy of this job id holds it, so ours was never added. Once that copy is done, make room for ours.
            if (job.finishedOn) {
              await job.remove();
              if (await queue._redispatch(rec.jobId, rec.nonce)) summary.redispatched++;
            }
          } else if (await queue._redispatch(rec.jobId, rec.nonce)) {
            // Released, but the add never reached BullMQ (producer crashed in between, lost reply, failover).
            summary.redispatched++;
          } else if ((await queue._ack(rec.jobId, 'removed', rec.nonce, 'P')) === 'ok') {
            // No payload left and still unconfirmed: nothing can be added. (If the add just landed, the record is confirmed and this is a no-op.)
            summary.acked++;
          }
          return;
        }
        if (job.finishedOn) {
          const state = (await job.isCompleted()) ? 'completed' : (await job.isFailed()) ? 'failed' : null;
          if (state) {
            // The worker that finished it died before telling us.
            if ((await queue._ack(rec.jobId, state, rec.nonce)) !== 'none') summary.acked++;
            return;
          }
        }
        if (!rec.added) {
          // BullMQ has it; only the confirmation was lost.
          await queue._markAdded(rec.jobId, rec.nonce);
          summary.confirmed++;
        }
        if (job.processedOn && now - job.processedOn > this.maint.stuckAfterMs && (await job.isActive())) {
          const stuck: StuckJob = { jobId: rec.jobId, groupId: rec.groupId, state: 'active', ageMs: now - job.processedOn };
          summary.stuck.push(stuck);
          this.emitGroup('stuck', stuck);
        }
      }),
    );
    const { next, repaired } = await queue._kick(this.kickCursor, KICK_BATCH);
    this.kickCursor = next;
    summary.repaired = repaired;
    if (repaired.length) this.emitGroup('repaired', repaired);
    return summary;
  }

  /** Create the group side: writes this worker's group settings and, if running, starts the maintenance loops. */
  private activate(): GroupQueue<DataType> {
    if (this.groupQueue) return this.groupQueue;
    const queue = new GroupQueue<DataType>(this.name, {
      connection: this.groupConnection,
      prefix: this.opts.prefix,
      worker: true,
      // Only a worker that declares group options writes them. One without (a second app, or a NestJS
      // processor without `group`) serves group jobs by what the queue's other workers wrote.
      groupDefaults: this.declaresGroups ? this.settings : undefined,
    });
    this.groupQueue = queue;
    if (this.opts.maxStalledCount === 0 && !warnedStalledCount) {
      // Only for a worker that actually serves groups: with the NestJS module every processor is a GroupWorker.
      warnedStalledCount = true;
      process.emitWarning(
        'GroupWorker with maxStalledCount 0: a job whose worker crashed is failed, not re-run, so its group moves on without it (or blocks under failurePolicy "block").',
        'BullMQGroupsWarning',
      );
    }
    queue.on('error', err => this.emit('error', err));
    queue.on('blocked' as never, ((groupId: string, jobId: string) => this.emitGroup('blocked', groupId, jobId)) as never);
    queue.on('rejected' as never, ((job: RejectedJob) => this.emitGroup('rejected', job)) as never);
    if (this.maintenance && !this.groupClosing) this.startLoops();
    return queue;
  }

  /** Whether this worker's queue has group state, without creating the group side for a plain queue. */
  private async hasGroups(): Promise<boolean> {
    if (this.groupQueue) return true;
    const client = await bullmqClient(this);
    if (!client) return true; // cannot tell without a connection of our own: assume groups
    return (await client.exists(`${this.qualifiedName}:gq:meta`)) === 1;
  }

  private async onFinalFailure(job: Job): Promise<void> {
    // Confirm with BullMQ before freeing the slot: if the lock was lost the job may be running elsewhere.
    const nonce = groupOf(job)!.r!;
    const current = await this.queue._bullJob(job.id!);
    if (!current || (groupOf(current)?.r === nonce && current.finishedOn && (await current.isFailed()))) {
      await this.queue._ack(job.id!, 'failed', nonce);
    }
  }

  private startMaintenance(): void {
    if (this.maintenance || this.groupClosing) return;
    this.maintenance = true;
    if (this.groupQueue) this.startLoops();
    else this.probe();
  }

  /**
   * A worker without group options looks for group state now and then (one EXISTS on BullMQ's own
   * connection) and starts the full maintenance once the queue has any, for example delayed group
   * jobs no worker has seen yet.
   */
  private probe(): void {
    const tick = async () => {
      if (this.groupClosing || this.groupQueue) return;
      try {
        if (await this.hasGroups()) {
          this.activate();
          return;
        }
      } catch (err) {
        if (!this.groupClosing) this.emit('error', err as Error);
      }
      if (!this.groupClosing && !this.groupQueue) this.probeTimer = setTimeout(tick, jitter(this.maint.reconcileIntervalMs));
    };
    this.probeTimer = setTimeout(tick, 0);
  }

  private startLoops(): void {
    if (this.loops || this.groupClosing) return;
    this.loops = true;
    clearTimeout(this.probeTimer);
    const reconcileLoop = async () => {
      if (this.groupClosing) return;
      if (!this.reconciling) {
        this.reconciling = true;
        await this.track(this.reconcile()).finally(() => (this.reconciling = false));
      }
      if (!this.groupClosing) this.reconcileTimer = setTimeout(reconcileLoop, jitter(this.maint.reconcileIntervalMs));
    };
    const promoteLoop = async () => {
      if (this.groupClosing) return;
      // The wait comes from Redis's clock, so a skewed worker clock cannot make this busy-poll or sleep through a due job.
      const dueIn = await this.track(this.queue._promoteDelayed());
      const poll = this.maint.delayedPollMs;
      const wait = typeof dueIn === 'number' ? Math.max(5, Math.min(poll, dueIn)) : poll;
      if (!this.groupClosing) this.promoteTimer = setTimeout(promoteLoop, wait);
    };
    this.reconcileTimer = setTimeout(reconcileLoop, jitter(this.maint.reconcileIntervalMs));
    this.promoteTimer = setTimeout(promoteLoop, 0);
  }

  private emitGroup(event: string, ...args: unknown[]): void {
    (this.emit as (event: string, ...args: unknown[]) => boolean)(event, ...args);
  }

  /** Keep bookkeeping promises so close() can wait for them, and route failures to 'error'. */
  private track<T>(p: Promise<T>): Promise<T | undefined> {
    const tracked = p.catch(err => {
      if (!this.groupClosing || !String(err?.message).includes('Connection is closed')) this.emit('error', err);
      return undefined;
    });
    this.bookkeeping.add(tracked);
    tracked.finally(() => this.bookkeeping.delete(tracked));
    return tracked;
  }
}

function jitter(ms: number): number {
  return Math.round(ms * (0.8 + Math.random() * 0.4));
}
