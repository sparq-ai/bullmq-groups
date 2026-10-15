import { EventEmitter } from 'events';
import { Worker, Processor, Job } from 'bullmq';
import { GroupQueue } from './group-queue';
import type { GroupWorkerOptions, StuckJob } from './types';

const DEFAULTS = {
  reconcileIntervalMs: 5_000,
  stuckAfterMs: 15 * 60_000,
  delayedPollMs: 1_000,
};
const LEASE_BATCH = 500;
const KICK_BATCH = 200;

/** BullMQ states in which a released job is still on its way (not finished). */
const IN_PROGRESS = new Set(['waiting', 'active', 'delayed', 'prioritized', 'waiting-children', 'paused']);

/**
 * A plain BullMQ Worker plus the bookkeeping that keeps groups moving.
 *
 * The processor is passed to BullMQ untouched (a function or a sandboxed file path both work).
 * When a job completes, or fails after its last attempt, the worker frees the job's group slot and
 * releases the group's next job. A job that fails and will be retried keeps its slot, so nothing
 * behind it in the group starts until it succeeds or gives up.
 *
 * Every worker also runs a small reconciler that covers the gaps a crash can leave between BullMQ's
 * own scripts and ours (see README, "Crash windows").
 *
 * Events: 'stuck' (StuckJob), 'blocked' (groupId, jobId), 'repaired' (groupIds), 'error' (Error).
 */
export class GroupWorker<DataType = any, ResultType = any, NameType extends string = string> extends EventEmitter {
  readonly worker: Worker<DataType, ResultType, NameType>;
  readonly queue: GroupQueue<DataType>;
  private readonly opts: typeof DEFAULTS;
  private readonly pending = new Set<Promise<unknown>>();
  private reconcileTimer?: NodeJS.Timeout;
  private promoteTimer?: NodeJS.Timeout;
  private kickCursor = 0;
  private reconciling = false;
  private closing?: Promise<void>;

  constructor(name: string, processor: string | URL | Processor<DataType, ResultType, NameType>, opts: GroupWorkerOptions) {
    super();
    const { connection, prefix, reconcileIntervalMs, reconcileGraceMs, stuckAfterMs, delayedPollMs, ...workerOpts } = opts;
    this.opts = {
      reconcileIntervalMs: reconcileIntervalMs ?? DEFAULTS.reconcileIntervalMs,
      stuckAfterMs: stuckAfterMs ?? DEFAULTS.stuckAfterMs,
      delayedPollMs: delayedPollMs ?? DEFAULTS.delayedPollMs,
    };
    this.queue = new GroupQueue<DataType>(name, { connection, prefix, reconcileGraceMs });
    this.queue.on('error', err => this.error(err));
    this.queue.on('blocked', (groupId, jobId) => this.emit('blocked', groupId, jobId));
    this.worker = new Worker<DataType, ResultType, NameType>(name, processor, { ...workerOpts, connection, prefix });
    this.worker.on('error', err => this.error(err));
    this.worker.on('completed', job => this.track(this.queue._ack(job.id!, 'completed')));
    this.worker.on('failed', (job, err) => {
      // BullMQ emits 'failed' for every failed attempt; finishedOn is only set on the last one.
      if (job?.id && job.finishedOn) this.track(this.onFinalFailure(job));
    });
    if (workerOpts.autorun !== false) this.startMaintenance();
  }

  /** Start processing when created with autorun: false. */
  run(): Promise<void> {
    this.startMaintenance();
    return this.worker.run();
  }

  /**
   * Graceful by default: stops the reconciler, lets running jobs finish, then waits for their
   * group bookkeeping, so the next job of each group is already in BullMQ for another worker.
   */
  async close(force = false): Promise<void> {
    this.closing ??= (async () => {
      clearTimeout(this.reconcileTimer);
      clearTimeout(this.promoteTimer);
      await this.worker.close(force);
      while (this.pending.size) await Promise.allSettled([...this.pending]);
      await this.queue.close();
    })();
    return this.closing;
  }

  /** One reconciler pass. Exposed for tests and for running it from a cron instead of every worker. */
  async reconcile(): Promise<{ checked: number; acked: number; redispatched: number; confirmed: number; stuck: StuckJob[]; repaired: string[] }> {
    const summary = { checked: 0, acked: 0, redispatched: 0, confirmed: 0, stuck: [] as StuckJob[], repaired: [] as string[] };
    const { now, records } = await this.queue._lease(LEASE_BATCH);
    summary.checked = records.length;
    const states = await Promise.all(records.map(r => this.queue.bull.getJobState(r.jobId)));
    await Promise.all(
      records.map(async (rec, i) => {
        const state = states[i];
        if (state === 'completed' || state === 'failed') {
          // The worker that finished it died before telling us.
          await this.queue._ack(rec.jobId, state);
          summary.acked++;
        } else if (state === 'unknown') {
          if (rec.added) {
            // Finished and removed (removeOnComplete/removeOnFail), or removed by hand.
            await this.queue._ack(rec.jobId, 'removed');
            summary.acked++;
          } else if (await this.queue._redispatch(rec.jobId, rec.nonce)) {
            // Released but the producer died before BullMQ got it.
            summary.redispatched++;
          } else {
            await this.queue._ack(rec.jobId, 'removed');
            summary.acked++;
          }
        } else if (IN_PROGRESS.has(state)) {
          if (!rec.added) {
            // BullMQ has it; only the confirmation was lost.
            await this.queue._markAdded(rec.jobId, rec.nonce);
            summary.confirmed++;
          }
          if (now - rec.releasedAt > this.opts.stuckAfterMs) {
            const stuck = { jobId: rec.jobId, groupId: rec.groupId, state, ageMs: now - rec.releasedAt };
            summary.stuck.push(stuck);
            this.emit('stuck', stuck);
          }
        }
      }),
    );
    const { next, repaired } = await this.queue._kick(this.kickCursor, KICK_BATCH);
    this.kickCursor = next;
    summary.repaired = repaired;
    if (repaired.length) this.emit('repaired', repaired);
    return summary;
  }

  private async onFinalFailure(job: Job): Promise<void> {
    // Confirm with BullMQ before freeing the slot: if the lock was lost the job may be running elsewhere.
    const state = await this.queue.bull.getJobState(job.id!);
    if (state === 'failed' || state === 'unknown') {
      await this.queue._ack(job.id!, 'failed');
    }
  }

  private startMaintenance(): void {
    if (this.reconcileTimer || this.closing) return;
    const reconcileLoop = async () => {
      if (this.closing) return;
      if (!this.reconciling) {
        this.reconciling = true;
        await this.track(this.reconcile()).finally(() => (this.reconciling = false));
      }
      if (!this.closing) this.reconcileTimer = setTimeout(reconcileLoop, jitter(this.opts.reconcileIntervalMs));
    };
    const promoteLoop = async () => {
      if (this.closing) return;
      let wait = this.opts.delayedPollMs;
      const nextDue = await this.track(this.queue._promoteDelayed());
      if (typeof nextDue === 'number') wait = Math.max(5, Math.min(wait, nextDue - Date.now()));
      if (!this.closing) this.promoteTimer = setTimeout(promoteLoop, wait);
    };
    this.reconcileTimer = setTimeout(reconcileLoop, jitter(this.opts.reconcileIntervalMs));
    this.promoteTimer = setTimeout(promoteLoop, 0);
  }

  /** Emit 'error' only when someone listens: an unhandled 'error' event would crash the process. */
  private error(err: unknown): void {
    if (this.listenerCount('error') > 0) this.emit('error', err);
    else console.error(`[bullmq-groups:${this.queue.name}]`, err);
  }

  /** Keep bookkeeping promises so close() can wait for them, and route failures to 'error'. */
  private track<T>(p: Promise<T>): Promise<T | undefined> {
    const tracked = p.catch(err => {
      if (!this.closing || !String(err?.message).includes('Connection is closed')) this.error(err);
      return undefined;
    });
    this.pending.add(tracked);
    tracked.finally(() => this.pending.delete(tracked));
    return tracked;
  }
}

function jitter(ms: number): number {
  return Math.round(ms * (0.8 + Math.random() * 0.4));
}
