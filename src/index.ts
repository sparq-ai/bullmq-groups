export { GroupQueue } from './group-queue';
export type { AddStatus, GroupQueueListener } from './group-queue';
export { GroupWorker } from './group-worker';
export type { GroupProcessor, GroupWorkerListener, ReconcileSummary } from './group-worker';
export { GroupJob } from './group-job';
export { GroupMaxSizeExceededError, GroupStatus } from './types';
export type {
  BlockedGroup,
  FailurePolicy,
  GroupCounts,
  GroupJobsOptions,
  GroupLag,
  GroupQueueOptions,
  GroupStats,
  GroupWorkerOptions,
  JobGroupOptions,
  RejectedJob,
  SkippedJob,
  StuckJob,
  WorkerGroupOptions,
} from './types';

/** Group id of a job (the same field BullMQ Pro uses); undefined for a job added without a group. */
export function groupIdOf(job: { opts: unknown }): string | undefined {
  return (job.opts as { group?: { id?: string } }).group?.id;
}
