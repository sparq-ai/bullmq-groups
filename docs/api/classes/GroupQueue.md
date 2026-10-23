[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupQueue

# Class: GroupQueue\<DataType, ResultType, NameType\>

Defined in: [src/group-queue.ts:113](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L113)

A BullMQ queue with groups, shaped like BullMQ Pro's QueuePro.

A job added with `{ group: { id } }` waits in its group (our Redis keys) and is handed to this
queue's normal BullMQ wait list only when the group has a free slot, so BullMQ never holds more
than the group's share of jobs. BullMQ's wait list then serves groups in turn, and its workers,
locks, retries, stalled-job recovery and events all work unchanged. A job added without a group
is a plain BullMQ job.

Every Queue method works. getJob(), remove() and drain() also see jobs still waiting in a group;
the other getters (getJobCounts, getWaiting ...) count only jobs BullMQ has, as in BullMQ Pro,
where getGroupsJobsCount() counts the rest.

Extra events: 'rejected' (RejectedJob), 'blocked' (groupId, jobId).

## Extends

- `Queue`\<`DataType`, `ResultType`, `NameType`, `DataType`, `ResultType`, `NameType`\>

## Type Parameters

| Type Parameter | Default type |
| ------ | ------ |
| <a id="type-parameter-datatype"></a> `DataType` | `any` |
| <a id="type-parameter-resulttype"></a> `ResultType` | `any` |
| <a id="type-parameter-nametype"></a> `NameType` *extends* `string` | `string` |

## Implements

- `GroupAware`

## Constructors

### Constructor

```ts
new GroupQueue<DataType, ResultType, NameType>(
   name, 
   opts?, 
   Connection?
): GroupQueue<DataType, ResultType, NameType>;
```

Defined in: [src/group-queue.ts:126](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L126)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `name` | `string` |
| `opts?` | [`GroupQueueOptions`](../interfaces/GroupQueueOptions.md) |
| `Connection?` | *typeof* `RedisConnection` |

#### Returns

`GroupQueue`\<`DataType`, `ResultType`, `NameType`\>

#### Overrides

`Queue<DataType, ResultType, NameType, DataType, ResultType, NameType>.constructor`

## Properties

### keyBase

```ts
readonly keyBase: string;
```

Defined in: [src/group-queue.ts:117](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L117)

## Accessors

### redis

#### Get Signature

```ts
get redis(): GroupRedis;
```

Defined in: [src/group-queue.ts:178](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L178)

The connection for the group scripts, opened on first use, so a queue that never sees a
grouped job costs nothing extra. Separate from BullMQ's connections so the package relies only
on BullMQ's public API and works the same on BullMQ 5 and 6.

##### Returns

`GroupRedis`

## Methods

### on()

#### Call Signature

```ts
on<U>(event, listener): this;
```

Defined in: [src/group-queue.ts:143](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L143)

##### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u"></a> `U` *extends* keyof `QueueListener`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\> |

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | `QueueListener`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\>\[`U`\] |

##### Returns

`this`

##### Overrides

`Queue.on`

#### Call Signature

```ts
on<U>(event, listener): this;
```

Defined in: [src/group-queue.ts:147](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L147)

##### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-1"></a> `U` *extends* keyof [`GroupQueueListener`](../interfaces/GroupQueueListener.md) |

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | [`GroupQueueListener`](../interfaces/GroupQueueListener.md)\[`U`\] |

##### Returns

`this`

##### Overrides

`Queue.on`

#### Call Signature

```ts
on(event, listener): this;
```

Defined in: [src/group-queue.ts:148](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L148)

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `string` |
| `listener` | (...`args`) => `void` |

##### Returns

`this`

##### Overrides

`Queue.on`

***

### once()

#### Call Signature

```ts
once<U>(event, listener): this;
```

Defined in: [src/group-queue.ts:153](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L153)

##### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-2"></a> `U` *extends* keyof `QueueListener`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\> |

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | `QueueListener`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\>\[`U`\] |

##### Returns

`this`

##### Overrides

`Queue.once`

#### Call Signature

```ts
once<U>(event, listener): this;
```

Defined in: [src/group-queue.ts:157](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L157)

##### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-3"></a> `U` *extends* keyof [`GroupQueueListener`](../interfaces/GroupQueueListener.md) |

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | [`GroupQueueListener`](../interfaces/GroupQueueListener.md)\[`U`\] |

##### Returns

`this`

##### Overrides

`Queue.once`

#### Call Signature

```ts
once(event, listener): this;
```

Defined in: [src/group-queue.ts:158](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L158)

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `string` |
| `listener` | (...`args`) => `void` |

##### Returns

`this`

##### Overrides

`Queue.once`

***

### off()

#### Call Signature

```ts
off<U>(event, listener): this;
```

Defined in: [src/group-queue.ts:163](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L163)

##### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-4"></a> `U` *extends* keyof `QueueListener`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\> |

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | `QueueListener`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\>\[`U`\] |

##### Returns

`this`

##### Overrides

`Queue.off`

#### Call Signature

```ts
off<U>(event, listener): this;
```

Defined in: [src/group-queue.ts:167](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L167)

##### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-5"></a> `U` *extends* keyof [`GroupQueueListener`](../interfaces/GroupQueueListener.md) |

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | [`GroupQueueListener`](../interfaces/GroupQueueListener.md)\[`U`\] |

##### Returns

`this`

##### Overrides

`Queue.off`

#### Call Signature

```ts
off(event, listener): this;
```

Defined in: [src/group-queue.ts:168](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L168)

##### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `string` |
| `listener` | (...`args`) => `void` |

##### Returns

`this`

##### Overrides

`Queue.off`

***

### add()

```ts
add(
   name, 
   data, 
   opts?
): Promise<GroupJob<DataType, ResultType, NameType>>;
```

Defined in: [src/group-queue.ts:220](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L220)

Add a job. With `opts.group` it joins the tail of its group and runs once the group has room
(BullMQ Pro's QueuePro.add); without, it is a plain BullMQ job.
Throws GroupMaxSizeExceededError when group.maxSize is set and reached.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `name` | `NameType` |
| `data` | `DataType` |
| `opts?` | [`GroupJobsOptions`](../interfaces/GroupJobsOptions.md) |

#### Returns

`Promise`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>\>

#### Overrides

`Queue.add`

***

### addBulk()

```ts
addBulk(jobs): Promise<GroupJob<DataType, ResultType, NameType>[]>;
```

Defined in: [src/group-queue.ts:239](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L239)

Add many jobs, grouped and plain mixed. Jobs of the same group keep their order in the array.
Every grouped job is checked first (including BullMQ's own option checks), so an invalid job
throws before anything is stored. Grouped jobs are stored 1,000 per script call; each call is
atomic. If storing a later chunk fails, the error carries `results` for the jobs already
stored, so a caller can retry only the rest. group.maxSize is not supported here, as in Pro.

Resolves once the jobs are stored. Handing released jobs to BullMQ is retried by the
reconciler, so a failure at that step is reported through 'error' instead of thrown: a caller
that retried would add the job twice.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobs` | \{ `name`: `NameType`; `data`: `DataType`; `opts?`: [`GroupJobsOptions`](../interfaces/GroupJobsOptions.md); \}[] |

#### Returns

`Promise`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>[]\>

#### Overrides

`Queue.addBulk`

***

### getJob()

```ts
getJob(jobId): Promise<
  | GroupJob<DataType, ResultType, NameType> & JobBase<DataType, ResultType, NameType>
| undefined>;
```

Defined in: [src/group-queue.ts:345](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L345)

A job in BullMQ, or one still waiting in its group.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |

#### Returns

`Promise`\<
  \| [`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\> & `JobBase`\<`DataType`, `ResultType`, `NameType`\>
  \| `undefined`\>

#### Overrides

`Queue.getJob`

***

### remove()

```ts
remove(jobId, opts?): Promise<number>;
```

Defined in: [src/group-queue.ts:356](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L356)

Remove a job, including one still waiting in its group. Returns 1 when removed, as BullMQ does.
Removing a released job that BullMQ holds frees its group slot at once.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |
| `opts?` | \{ `removeChildren?`: `boolean`; \} |
| `opts.removeChildren?` | `boolean` |

#### Returns

`Promise`\<`number`\>

#### Overrides

`Queue.remove`

***

### drain()

```ts
drain(delayed?): Promise<void>;
```

Defined in: [src/group-queue.ts:365](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L365)

BullMQ's drain, plus every job waiting in a group (and delayed group jobs when `delayed`).

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `delayed?` | `boolean` | `false` |

#### Returns

`Promise`\<`void`\>

#### Overrides

`Queue.drain`

***

### pauseGroup()

```ts
pauseGroup(groupId): Promise<boolean>;
```

Defined in: [src/group-queue.ts:416](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L416)

Stop starting jobs of this group, on every worker. Running jobs finish; released jobs that have
not started wait for resume. Works for a group that has no jobs yet. False if it was already paused.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`boolean`\>

***

### resumeGroup()

```ts
resumeGroup(groupId): Promise<boolean>;
```

Defined in: [src/group-queue.ts:422](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L422)

False if the group was not paused.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`boolean`\>

***

### setGroupConcurrency()

```ts
setGroupConcurrency(
   groupId, 
   concurrency, 
   opts?
): Promise<number>;
```

Defined in: [src/group-queue.ts:431](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L431)

Give one group its own limit, over the workers' group.concurrency. Stored in Redis until
removeGroupConcurrency(). Resolves to 1 when the group had no limit of its own before, else 0.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |
| `concurrency` | `number` |
| `opts?` | \{ `ttlSeconds?`: `number`; \} |
| `opts.ttlSeconds?` | `number` |

#### Returns

`Promise`\<`number`\>

***

### getGroupConcurrency()

```ts
getGroupConcurrency(groupId): Promise<number>;
```

Defined in: [src/group-queue.ts:439](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L439)

The group's limit: its own, else the workers' group.concurrency, else Infinity (no limit).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`number`\>

***

### removeGroupConcurrency()

```ts
removeGroupConcurrency(groupId): Promise<number>;
```

Defined in: [src/group-queue.ts:446](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L446)

Drop the group's own limit; it falls back to the workers' group.concurrency. Resolves to 1 if it had one.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`number`\>

***

### ~~deleteGroupConcurrency()~~

```ts
deleteGroupConcurrency(groupId): Promise<number>;
```

Defined in: [src/group-queue.ts:452](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L452)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`number`\>

#### Deprecated

BullMQ Pro's former name of removeGroupConcurrency.

***

### getGroupJobs()

```ts
getGroupJobs(
   groupId, 
   start?, 
   end?
): Promise<GroupJob<DataType, ResultType, NameType>[]>;
```

Defined in: [src/group-queue.ts:457](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L457)

Jobs waiting in the group, in the order they will run (unprioritized first). Inclusive range, as Pro.

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `groupId` | `string` \| `number` | `undefined` |
| `start?` | `number` | `0` |
| `end?` | `number` | `-1` |

#### Returns

`Promise`\<[`GroupJob`](GroupJob.md)\<`DataType`, `ResultType`, `NameType`\>[]\>

***

### getGroupJobsCount()

```ts
getGroupJobsCount(groupId): Promise<number>;
```

Defined in: [src/group-queue.ts:479](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L479)

Jobs waiting in the group.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`number`\>

***

### getGroupsJobsCount()

```ts
getGroupsJobsCount(_limit?): Promise<number>;
```

Defined in: [src/group-queue.ts:486](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L486)

Jobs waiting in any group, prioritized or not. O(1); `limit` is accepted for Pro compatibility.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `_limit?` | `number` |

#### Returns

`Promise`\<`number`\>

***

### getGroupActiveCount()

```ts
getGroupActiveCount(groupId): Promise<number>;
```

Defined in: [src/group-queue.ts:491](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L491)

Jobs of the group handed to workers and not finished: running, retrying, or about to start.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`number`\>

***

### getGroups()

```ts
getGroups(start?, end?): Promise<{
  id: string;
  status: GroupStatus;
}[]>;
```

Defined in: [src/group-queue.ts:496](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L496)

Groups with jobs waiting, longest-waiting first, with their status. Inclusive range, as Pro.

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `start?` | `number` | `0` |
| `end?` | `number` | `-1` |

#### Returns

`Promise`\<\{
  `id`: `string`;
  `status`: [`GroupStatus`](../enumerations/GroupStatus.md);
\}[]\>

***

### getGroupsCount()

```ts
getGroupsCount(): Promise<number>;
```

Defined in: [src/group-queue.ts:503](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L503)

Number of groups with jobs waiting.

#### Returns

`Promise`\<`number`\>

***

### getGroupStatus()

```ts
getGroupStatus(groupId): Promise<GroupStatus | null>;
```

Defined in: [src/group-queue.ts:508](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L508)

The group's status, or null when it has no jobs waiting and is neither paused nor blocked.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<[`GroupStatus`](../enumerations/GroupStatus.md) \| `null`\>

***

### getGroupsByStatus()

```ts
getGroupsByStatus(
   status, 
   start?, 
   end?
): Promise<{
  id: string;
  count: number;
}[]>;
```

Defined in: [src/group-queue.ts:514](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L514)

Groups with jobs waiting that are in `status`, with how many jobs wait in each. Reads every group: O(groups).

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `status` | [`GroupStatus`](../enumerations/GroupStatus.md) | `undefined` |
| `start?` | `number` | `0` |
| `end?` | `number` | `-1` |

#### Returns

`Promise`\<\{
  `id`: `string`;
  `count`: `number`;
\}[]\>

***

### getGroupsCountByStatus()

```ts
getGroupsCountByStatus(): Promise<{
  waiting: number;
  paused: number;
  limited: number;
  maxed: number;
  blocked: number;
}>;
```

Defined in: [src/group-queue.ts:521](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L521)

How many groups with jobs waiting are in each status. Reads every group: O(groups).

#### Returns

`Promise`\<\{
  `waiting`: `number`;
  `paused`: `number`;
  `limited`: `number`;
  `maxed`: `number`;
  `blocked`: `number`;
\}\>

***

### getCountsPerPriorityForGroup()

```ts
getCountsPerPriorityForGroup(groupId, priorities): Promise<{
[index: string]: number;
}>;
```

Defined in: [src/group-queue.ts:528](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L528)

Waiting jobs of the group per priority; priority 0 counts the jobs without one.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` |
| `priorities` | `number`[] |

#### Returns

`Promise`\<\{
\[`index`: `string`\]: `number`;
\}\>

***

### deleteGroup()

```ts
deleteGroup(groupId): Promise<void>;
```

Defined in: [src/group-queue.ts:540](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L540)

Remove every job waiting in the group. Running and delayed jobs are not touched.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`void`\>

***

### deleteGroups()

```ts
deleteGroups(): Promise<void>;
```

Defined in: [src/group-queue.ts:546](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L546)

Remove every job waiting in any group.

#### Returns

`Promise`\<`void`\>

***

### repairMaxedGroup()

```ts
repairMaxedGroup(groupId): Promise<{
  previous: number;
  active: number;
}>;
```

Defined in: [src/group-queue.ts:559](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L559)

Recount the group's in-flight jobs and release what fits: repairs a group whose count drifted
(a write lost in a Redis failover) so that it looks full with nothing running.
Reads every in-flight record of the queue.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<\{
  `previous`: `number`;
  `active`: `number`;
\}\>

***

### getBlockedGroups()

```ts
getBlockedGroups(): Promise<BlockedGroup[]>;
```

Defined in: [src/group-queue.ts:569](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L569)

Groups held by jobs that failed for good under failurePolicy 'block'.

#### Returns

`Promise`\<[`BlockedGroup`](../interfaces/BlockedGroup.md)[]\>

***

### retryBlockedGroup()

```ts
retryBlockedGroup(groupId): Promise<string | null>;
```

Defined in: [src/group-queue.ts:586](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L586)

Run the oldest job blocking this group again, with a fresh set of attempts. It keeps the
group's slot, so nothing behind it starts until it finishes. Returns its id, or null when
nothing was blocked, or when the job is gone from BullMQ (the group then moves on without it).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`string` \| `null`\>

***

### skipBlockedGroup()

```ts
skipBlockedGroup(groupId): Promise<string | null>;
```

Defined in: [src/group-queue.ts:608](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L608)

Give up on the oldest job blocking this group. The group moves on once no job blocks it.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<`string` \| `null`\>

***

### getGroupCounts()

```ts
getGroupCounts(groupId): Promise<GroupCounts>;
```

Defined in: [src/group-queue.ts:619](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L619)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` \| `number` |

#### Returns

`Promise`\<[`GroupCounts`](../interfaces/GroupCounts.md)\>

***

### getLaggingGroups()

```ts
getLaggingGroups(start?, end?): Promise<GroupLag[]>;
```

Defined in: [src/group-queue.ts:647](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L647)

Groups with waiting jobs, most behind first, with how long their next job has been ready.

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `start?` | `number` | `0` |
| `end?` | `number` | `99` |

#### Returns

`Promise`\<[`GroupLag`](../interfaces/GroupLag.md)[]\>

***

### getStats()

```ts
getStats(): Promise<GroupStats>;
```

Defined in: [src/group-queue.ts:669](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L669)

Queue-wide numbers for dashboards and alerts. All O(1) or O(log n).

#### Returns

`Promise`\<[`GroupStats`](../interfaces/GroupStats.md)\>

***

### getStuckJobs()

```ts
getStuckJobs(olderThanMs): Promise<StuckJob[]>;
```

Defined in: [src/group-queue.ts:700](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L700)

Jobs that have been in flight longer than `olderThanMs` (running, retrying or waiting in BullMQ).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `olderThanMs` | `number` |

#### Returns

`Promise`\<[`StuckJob`](../interfaces/StuckJob.md)[]\>

***

### obliterate()

```ts
obliterate(opts?): Promise<void>;
```

Defined in: [src/group-queue.ts:723](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L723)

BullMQ's obliterate, then every group key. Stop workers first. Scans one node, so on Redis
Cluster all keys must share a hash-tagged prefix.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `opts?` | `ObliterateOpts` |

#### Returns

`Promise`\<`void`\>

#### Overrides

`Queue.obliterate`

***

### repairAll()

```ts
repairAll(): Promise<string[]>;
```

Defined in: [src/group-queue.ts:738](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L738)

Walk every waiting group once and release what has room, e.g. after a default-concurrency change.

#### Returns

`Promise`\<`string`[]\>

***

### close()

```ts
close(): Promise<void>;
```

Defined in: [src/group-queue.ts:749](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L749)

Close the queue instance.

#### Returns

`Promise`\<`void`\>

#### Overrides

`Queue.close`

***

### \_error()

```ts
_error(err): void;
```

Defined in: [src/group-queue.ts:777](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L777)

Emit 'error' only when someone listens: an unhandled 'error' event would crash the process.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `err` | `unknown` |

#### Returns

`void`

***

### \_bullJob()

```ts
_bullJob(jobId): Promise<Job<any, any, string> | undefined>;
```

Defined in: [src/group-queue.ts:783](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L783)

The job as BullMQ has it, ignoring jobs that wait in a group.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |

#### Returns

`Promise`\<`Job`\<`any`, `any`, `string`\> \| `undefined`\>

***

### \_addReleased()

```ts
_addReleased(jobs): Promise<void>;
```

Defined in: [src/group-queue.ts:788](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L788)

Add released jobs to BullMQ (BullMQ's own addBulk, not the grouped one).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobs` | \{ `name`: `NameType`; `data`: `DataType`; `opts`: `JobsOptions`; \}[] |

#### Returns

`Promise`\<`void`\>

***

### \_ack()

```ts
_ack(
   jobId, 
   outcome, 
   nonce?, 
   requireState?
): Promise<string>;
```

Defined in: [src/group-queue.ts:797](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L797)

A released job reached a final state in BullMQ: free its slot and release the group's next job.
`nonce` limits the ack to that release; `requireState` to a release still in that state.
Returns 'ok', 'blocked' or 'none' (nothing to do).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |
| `outcome` | `FinalOutcome` |
| `nonce?` | `number` |
| `requireState?` | `"P"` \| `"A"` |

#### Returns

`Promise`\<`string`\>

***

### \_markAdded()

```ts
_markAdded(jobId, nonce): Promise<void>;
```

Defined in: [src/group-queue.ts:811](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L811)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |
| `nonce` | `number` |

#### Returns

`Promise`\<`void`\>

***

### \_redispatch()

```ts
_redispatch(jobId, nonce): Promise<boolean>;
```

Defined in: [src/group-queue.ts:816](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L816)

Re-add a released job whose BullMQ add never happened (producer crashed in between, add lost).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |
| `nonce` | `number` |

#### Returns

`Promise`\<`boolean`\>

***

### \_claim()

```ts
_claim(
   jobId, 
   nonce, 
   groupId, 
   ranBefore
): Promise<[ClaimVerdict, FailurePolicy, string[]]>;
```

Defined in: [src/group-queue.ts:825](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L825)

The start check a GroupWorker runs before a job's processor. Also returns jobs released by it.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `jobId` | `string` |
| `nonce` | `number` |
| `groupId` | `string` |
| `ranBefore` | `boolean` |

#### Returns

`Promise`\<\[`ClaimVerdict`, [`FailurePolicy`](../type-aliases/FailurePolicy.md), `string`[]\]\>

***

### \_promoteDelayed()

```ts
_promoteDelayed(limit?): Promise<number | null>;
```

Defined in: [src/group-queue.ts:834](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L834)

Promote due delayed jobs. Returns ms until the next delayed job is due (Redis clock), or null when none.

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `limit?` | `number` | `500` |

#### Returns

`Promise`\<`number` \| `null`\>

***

### \_lease()

```ts
_lease(limit): Promise<{
  now: number;
  records: {
     jobId: string;
     groupId: string;
     added: boolean;
     releasedAt: number;
     nonce: number;
     blocked: boolean;
  }[];
}>;
```

Defined in: [src/group-queue.ts:840](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L840)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `limit` | `number` |

#### Returns

`Promise`\<\{
  `now`: `number`;
  `records`: \{
     `jobId`: `string`;
     `groupId`: `string`;
     `added`: `boolean`;
     `releasedAt`: `number`;
     `nonce`: `number`;
     `blocked`: `boolean`;
  \}[];
\}\>

***

### \_kick()

```ts
_kick(cursor, count): Promise<{
  next: string;
  repaired: string[];
}>;
```

Defined in: [src/group-queue.ts:857](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L857)

One kick pass over the next page of the group index. ZSCAN, not ranks: releases move groups
around the index, and a full scan still visits every group that waits throughout.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `cursor` | `string` |
| `count` | `number` |

#### Returns

`Promise`\<\{
  `next`: `string`;
  `repaired`: `string`[];
\}\>

***

### \_dispatch()

```ts
_dispatch(released): Promise<void>;
```

Defined in: [src/group-queue.ts:871](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L871)

Hand released jobs to BullMQ, then confirm. `released` is [failurePolicy, jobId, nonce,
payload, ...]. If this process dies in between, the job stays unconfirmed and the reconciler
adds it again a few seconds later: the job id makes a double add harmless, and workers skip a
copy that is not the current release.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `released` | `string`[] |

#### Returns

`Promise`\<`void`\>
