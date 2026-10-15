import type { ConnectionOptions, JobsOptions, WorkerOptions } from 'bullmq';

/**
 * What a group does when one of its jobs fails after its last attempt.
 * - continue: the job lands in BullMQ's failed set and the group moves on to its next job.
 * - block: the group stops until an operator calls retryBlockedGroup() or skipBlockedGroup().
 * While a job is still retrying the group is always held, so retries never break FIFO.
 */
export type FailurePolicy = 'continue' | 'block';

export interface GroupQueueOptions {
  connection: ConnectionOptions;
  /** BullMQ key prefix. Use a hash-tagged prefix such as '{bull}' on Redis Cluster. */
  prefix?: string;
  /** Default number of jobs per group allowed to run at once. Stored in Redis, so set it the same everywhere. */
  groupConcurrency?: number;
  /** Stored in Redis, so set it the same everywhere. */
  failurePolicy?: FailurePolicy;
  defaultJobOptions?: GroupJobOptions;
  /** How long a released job may stay unconfirmed before the reconciler checks on it. Stored in Redis. Default 30 s. */
  reconcileGraceMs?: number;
}

/** Options BullMQ handles itself, or that make no sense for a job that waits in a group first. */
type NotForGroupJobs =
  | 'jobId'
  | 'delay'
  | 'priority'
  | 'repeat'
  | 'repeatJobKey'
  | 'lifo'
  | 'parent'
  | 'deduplication'
  | 'debounce'
  | 'timestamp'
  | 'prevMillis';

export interface GroupJobOptions extends Omit<JobsOptions, NotForGroupJobs> {
  /** Custom id. Adding an id that is still waiting or running in its group is a no-op. */
  jobId?: string;
  /** Milliseconds before the job joins the tail of its group. */
  delay?: number;
  /** Order inside the group, BullMQ style: 0 (none) runs first, then 1, 2, 3 ... */
  priority?: number;
}

export interface AddResult {
  id: string;
  status: 'waiting' | 'delayed' | 'duplicate';
}

export interface GroupCounts {
  waiting: number;
  prioritized: number;
  active: number;
  concurrency: number;
  paused: boolean;
  blockedJobId: string | null;
  /** When the job this group runs next became ready (ms epoch), or null when nothing is waiting. */
  headReadyAt: number | null;
}

export interface GroupSummary {
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

export interface GroupWorkerOptions extends Omit<WorkerOptions, 'connection' | 'prefix'> {
  connection: ConnectionOptions;
  prefix?: string;
  /** How often this worker runs the reconciler. Default 5 s. */
  reconcileIntervalMs?: number;
  reconcileGraceMs?: number;
  /** A job in flight for longer than this is reported through the 'stuck' event. Default 15 min. */
  stuckAfterMs?: number;
  /** Upper bound on how late a delayed job joins its group. Default 1 s. */
  delayedPollMs?: number;
}
