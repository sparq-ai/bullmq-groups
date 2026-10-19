import { Job } from 'bullmq';
import type { JobState } from 'bullmq';
import type { GroupJobsOptions } from './types';

/** Implemented by GroupQueue and GroupWorker, so their jobs can see group state BullMQ does not have. */
export interface GroupAware {
  /** The state of a job still waiting in its group, or null when no group holds it. */
  _pendingState(jobId: string): Promise<'waiting' | 'prioritized' | 'delayed' | null>;
  /** Remove a job still waiting in its group. False when no group holds it. */
  _removePending(jobId: string): Promise<boolean>;
  /** A released job was removed from BullMQ before it finished: free its group slot. */
  _releaseRemoved(jobId: string, nonce: number): Promise<void>;
}

function groupAware(queue: unknown): GroupAware | undefined {
  return typeof (queue as Partial<GroupAware>)?._pendingState === 'function' ? (queue as GroupAware) : undefined;
}

/**
 * A BullMQ job that knows its group (BullMQ Pro's JobPro). Every job a GroupQueue or GroupWorker
 * returns is one, grouped or not. A grouped job that is still waiting in its group, and so not yet
 * in BullMQ, answers getState() and remove() as well.
 */
export class GroupJob<DataType = any, ReturnType = any, NameType extends string = string> extends Job<
  DataType,
  ReturnType,
  NameType
> {
  declare opts: GroupJobsOptions;

  /** The job's group id (BullMQ Pro's JobPro.gid), or undefined for a job added without a group. */
  get gid(): string | undefined {
    return this.opts.group?.id;
  }

  async getState(): Promise<JobState | 'unknown'> {
    const state = await super.getState();
    if (state !== 'unknown') return state;
    return (await groupAware(this.queue)?._pendingState(this.id!)) ?? state;
  }

  async remove(opts?: { removeChildren?: boolean }): Promise<void> {
    const aware = groupAware(this.queue);
    if (aware && (await aware._removePending(this.id!))) return;
    await super.remove(opts);
    // A released job removed before it finished frees its group slot now, not at the reconciler's next check.
    const nonce = (this.opts.group as { r?: number } | undefined)?.r;
    if (aware && nonce !== undefined) await aware._releaseRemoved(this.id!, nonce);
  }
}
