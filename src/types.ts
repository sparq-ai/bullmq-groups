import type { JobsOptions, QueueOptions, WorkerOptions } from 'bullmq';

/**
 * What a group does when one of its jobs fails after its last attempt. Not in BullMQ Pro, whose
 * groups always behave as 'continue'.
 * - continue: the job lands in BullMQ's failed set and the group moves on to its next job.
 * - block: the group stops until an operator calls retryBlockedGroup() or skipBlockedGroup().
 * While a job is still retrying it keeps its slot. At group concurrency 1 that holds the whole
 * group, so retries never break FIFO; at concurrency n the other n-1 slots keep going.
 */
export type FailurePolicy = 'continue' | 'block';

/** The `group` job option. Same fields as BullMQ Pro's. */
export interface JobGroupOptions {
  /** The group the job belongs to. Jobs of a group start in the order they were added. */
  id: string;
  /**
   * Order inside the group, as in BullMQ Pro: jobs without a priority first, then 1 (highest)
   * up to 2,097,152 (lowest). A prioritized job waits behind every unprioritized one of its group.
   */
  priority?: number;
  /**
   * Refuse the add with GroupMaxSizeExceededError when the group already has this many jobs
   * waiting. As in BullMQ Pro, not supported by addBulk.
   */
  maxSize?: number;
}

/**
 * Job options of a GroupQueue (BullMQ Pro's JobsProOptions). Without `group` a job is a plain
 * BullMQ job and every BullMQ option applies. With `group`, jobId, delay, attempts, backoff,
 * removeOnComplete/Fail and the other BullMQ options work as usual, except:
 *  - jobId: adding an id that is waiting or running in its group, or that BullMQ still holds,
 *    is a no-op that returns that job.
 *  - delay: the job joins the tail of its group when due.
 *  - priority: use group.priority instead.
 *  - deduplication: simple mode only ({ id }); ttl, extend and replace are not supported.
 *  - repeat, lifo, parent (flows) and debounce are not supported.
 */
export interface GroupJobsOptions extends JobsOptions {
  group?: JobGroupOptions;
}

export interface GroupQueueOptions extends QueueOptions {
  /**
   * @internal Written to Redis by a GroupWorker (from its `group` options) so producers and
   * workers agree. null concurrency: no per-group limit.
   */
  groupDefaults?: { concurrency?: number | null; failurePolicy?: FailurePolicy; reconcileGraceMs?: number };
  /** @internal Set by GroupWorker: its connection must never give up on a command. */
  worker?: boolean;
}

/** The `group` worker option. */
export interface WorkerGroupOptions {
  /**
   * How many jobs of one group may run at once, across every worker of the queue (BullMQ Pro's
   * group.concurrency). 1 runs each group strictly in order. Unset: no per-group limit, but jobs
   * of a group still start in order and take turns with other groups, as in BullMQ Pro.
   * Stored in Redis; give every worker of a queue the same value.
   */
  concurrency?: number;
  /** Not in BullMQ Pro. Default 'continue'. Stored in Redis; give every worker the same value. */
  failurePolicy?: FailurePolicy;
}

export interface GroupWorkerOptions extends WorkerOptions {
  group?: WorkerGroupOptions;
  // The options below are not in BullMQ Pro: they tune this package's background maintenance.
  /** How often this worker runs the reconciler. Default 5 s. */
  reconcileIntervalMs?: number;
  /**
   * How long a confirmed in-flight job goes unchecked before the reconciler looks at it (then with
   * back-off). Unconfirmed releases are checked after min(this, 5 s). Stored in Redis. Default 30 s.
   */
  reconcileGraceMs?: number;
  /** A job running longer than this is reported through the 'stuck' event. Default 15 min. */
  stuckAfterMs?: number;
  /** Upper bound on how late a delayed job joins its group. Default 1 s. */
  delayedPollMs?: number;
  /** A released job of a paused group, or a retried job waiting for a free slot, is looked at again after this. Default 5 s. */
  recheckMs?: number;
}

/** A group's status (BullMQ Pro's GroupStatus, plus Blocked). */
export enum GroupStatus {
  Waiting = 'waiting',
  Paused = 'paused',
  /** Rate limited. Never reported: group rate limits are not supported. */
  Limited = 'limited',
  /** As many of its jobs are running as its concurrency allows. */
  Maxed = 'maxed',
  /** Not in BullMQ Pro: a job failed for good under failurePolicy 'block'. */
  Blocked = 'blocked',
}

/** Thrown by GroupQueue.add when the job's group already has group.maxSize jobs waiting. */
export class GroupMaxSizeExceededError extends Error {
  constructor(
    readonly groupId: string,
    readonly maxSize: number,
  ) {
    super(`group ${groupId} already has ${maxSize} jobs waiting (maxSize)`);
    this.name = 'GroupMaxSizeExceededError';
  }
}

export interface GroupCounts {
  waiting: number;
  prioritized: number;
  /** Jobs handed to workers and not finished: running, retrying or about to start. */
  active: number;
  /** Infinity when the group has no limit. */
  concurrency: number;
  paused: boolean;
  /** The oldest job blocking the group under failurePolicy 'block', or null. */
  blockedJobId: string | null;
  /** When the job this group runs next became ready (ms epoch), or null when nothing is waiting. */
  headReadyAt: number | null;
}

export interface BlockedGroup {
  groupId: string;
  /** The oldest blocking job: the one retryBlockedGroup and skipBlockedGroup act on. */
  jobId: string;
  /** Every job blocking the group, oldest first (more than one only at group concurrency above 1). */
  jobIds: string[];
}

export interface GroupLag {
  id: string;
  /** How long the group's next job has been ready to run. */
  lagMs: number;
  waiting: number;
  active: number;
}

export interface GroupStats {
  /** Groups with at least one job waiting. */
  waitingGroups: number;
  /** Groups with at least one job released to BullMQ and not finished. */
  activeGroups: number;
  waiting: number;
  inflight: number;
  delayed: number;
  pausedGroups: number;
  blockedGroups: number;
  /** Age of the oldest job that is ready but waiting behind its group, 0 when none. */
  oldestWaitingMs: number;
}

export interface StuckJob {
  jobId: string;
  groupId: string;
  state: string;
  ageMs: number;
}

/** A BullMQ job the worker did not run because it is not the release its group is waiting for. */
export interface SkippedJob {
  jobId: string;
  groupId: string;
  /** 'stale': another release of this job id is in flight. 'ghost': the job already finished; a late or repeated add re-created it. */
  reason: 'stale' | 'ghost';
}

/** A released job BullMQ refused to add (for example sizeLimit exceeded). It is dropped from its group. */
export interface RejectedJob {
  jobId: string;
  groupId: string;
  error: Error;
}
