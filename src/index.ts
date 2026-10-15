export { GroupQueue } from './group-queue';
export { GroupWorker } from './group-worker';
export type {
  AddResult,
  FailurePolicy,
  GroupCounts,
  GroupJobOptions,
  GroupQueueOptions,
  GroupStats,
  GroupSummary,
  GroupWorkerOptions,
  StuckJob,
} from './types';

/** Group id of a job processed by a GroupWorker (same field BullMQ Pro uses). */
export function groupIdOf(job: { opts: unknown }): string | undefined {
  return (job.opts as { group?: { id?: string } }).group?.id;
}
