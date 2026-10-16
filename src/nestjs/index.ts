/**
 * NestJS integration: @nestjs/bullmq with GroupQueue and GroupWorker.
 *
 * The same module, decorators and injection tokens as @nestjs/bullmq (and as BullMQ Pro's
 * @taskforcesh/nestjs-bullmq-pro, which is based on it): BullModule.forRoot / forRootAsync /
 * registerQueue, @Processor + WorkerHost, @OnWorkerEvent, @InjectQueue, @QueueEventsListener.
 * Moving an app over is a change of import path.
 *
 * Importing this module tells @nestjs/bullmq, through its public queueClass / workerClass hooks
 * (the ones it provides for BullMQ Pro), to build every queue as a GroupQueue and every worker as a
 * GroupWorker. Both are supersets: jobs added without `group` behave exactly as before, and a
 * worker whose queue never sees a grouped job opens no extra connection. The hooks are global to
 * @nestjs/bullmq, so import BullModule from here, not from '@nestjs/bullmq', in the modules that
 * register queues: a queue registered before this file is loaded stays a plain Queue.
 */
import {
  BullModule as NestBullModule,
  OnWorkerEvent as NestOnWorkerEvent,
  Processor as NestProcessor,
  WorkerHost as NestWorkerHost,
} from '@nestjs/bullmq';
import type { ProcessorOptions } from '@nestjs/bullmq';
import type { WorkerListener } from 'bullmq';
import { GroupQueue } from '../group-queue';
import { GroupWorker } from '../group-worker';
import type { GroupWorkerListener } from '../group-worker';
import type { GroupJob } from '../group-job';
import type { GroupWorkerOptions } from '../types';

/** @nestjs/bullmq's worker options type (not exported by name). */
type NestWorkerOptions = Parameters<typeof NestProcessor>[1];

NestBullModule.queueClass = GroupQueue;
NestBullModule.workerClass = GroupWorker;

export * from '@nestjs/bullmq';
export * from '../index';

/** Worker options for @Processor: a GroupWorker's, connection coming from BullModule.forRoot. */
export type NestGroupWorkerOptions = Omit<GroupWorkerOptions, 'connection'> &
  Partial<Pick<GroupWorkerOptions, 'connection'>> &
  Pick<NestWorkerOptions, 'sharedConnection'>;

/**
 * Marks a class as the processor of a queue, as @nestjs/bullmq's @Processor, with the worker's
 * `group` options typed: @Processor('sync', { concurrency: 20, group: { concurrency: 1 } }).
 */
export function Processor(queueName: string): ClassDecorator;
export function Processor(queueName: string, workerOptions: NestGroupWorkerOptions): ClassDecorator;
export function Processor(processorOptions: ProcessorOptions): ClassDecorator;
export function Processor(processorOptions: ProcessorOptions, workerOptions: NestGroupWorkerOptions): ClassDecorator;
export function Processor(queueOrOptions: string | ProcessorOptions, workerOptions?: NestGroupWorkerOptions): ClassDecorator {
  return NestProcessor(queueOrOptions as ProcessorOptions, workerOptions as NestWorkerOptions);
}

/** @nestjs/bullmq's @OnWorkerEvent, also for a GroupWorker's events ('blocked', 'stuck', 'skipped' ...). */
export const OnWorkerEvent = (eventName: keyof GroupWorkerListener): MethodDecorator =>
  NestOnWorkerEvent(eventName as keyof WorkerListener);

/** Base class of a processor, as @nestjs/bullmq's: `this.worker` is a GroupWorker and process() gets GroupJobs. */
export abstract class WorkerHost<T extends GroupWorker = GroupWorker> extends NestWorkerHost<T> {
  abstract process(job: GroupJob, token?: string): Promise<any>;
}
