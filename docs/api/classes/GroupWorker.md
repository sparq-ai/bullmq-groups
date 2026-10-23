[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupWorker

# Class: GroupWorker\<DataType, ResultType, NameType\>

Defined in: [src/group-worker.ts:100](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L100)

A BullMQ Worker that keeps groups moving, shaped like BullMQ Pro's WorkerPro.

It is a real `Worker`: the processor (a function or a sandboxed file) is BullMQ's, and every
BullMQ worker option, method and event works. Jobs added without a group run as on any worker.
On top of that:
 - When a job completes, or fails after its last attempt, the worker frees the job's group slot
   and releases the group's next job. A job that fails and will be retried keeps its slot.
 - Before the processor runs, a start check makes sure the BullMQ job is the release its group
   is waiting for. A stale copy of the same job id (a late or resent add) is skipped, a job of a
   paused group waits for resume, and a finished job retried from a dashboard runs only in a
   free slot of its group.
 - A small reconciler covers the gaps a crash can leave between BullMQ's scripts and ours
   (see README, "Crash windows").

The `group` options are written to Redis, so every worker of a queue that declares them should
use the same ones. A worker created without `group` options never writes them: it serves group
jobs by what the queue's other workers wrote, and starts its group bookkeeping only once its
queue has group jobs, so a plain queue pays nothing for it.

Extra events: 'stuck', 'blocked', 'repaired', 'skipped', 'rejected' (see GroupWorkerListener).

## Extends

- `Worker`\<`DataType`, `ResultType`, `NameType`\>

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
new GroupWorker<DataType, ResultType, NameType>(
   name, 
   processor, 
   opts
): GroupWorker<DataType, ResultType, NameType>;
```

Defined in: [src/group-worker.ts:125](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L125)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `name` | `string` |
| `processor` | \| `string` \| `URL` \| [`GroupProcessor`](../type-aliases/GroupProcessor.md)\<`DataType`, `ResultType`, `NameType`\> \| `null` |
| `opts` | [`GroupWorkerOptions`](../interfaces/GroupWorkerOptions.md) |

#### Returns

`GroupWorker`\<`DataType`, `ResultType`, `NameType`\>

#### Overrides

`Worker<DataType, ResultType, NameType>.constructor`

## Accessors

### queue

#### Get Signature

```ts
get queue(): GroupQueue<DataType>;
```

Defined in: [src/group-worker.ts:161](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L161)

The group side of this worker's queue. Created on first use.

##### Returns

[`GroupQueue`](GroupQueue.md)\<`DataType`\>

## Methods

### on()

```ts
on<U>(event, listener): this;
```

Defined in: [src/group-worker.ts:165](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L165)

#### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u"></a> `U` *extends* keyof [`GroupWorkerListener`](../interfaces/GroupWorkerListener.md)\<`DataType`, `ResultType`, `NameType`\> |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | [`GroupWorkerListener`](../interfaces/GroupWorkerListener.md)\<`DataType`, `ResultType`, `NameType`\>\[`U`\] |

#### Returns

`this`

#### Overrides

`Worker.on`

***

### once()

```ts
once<U>(event, listener): this;
```

Defined in: [src/group-worker.ts:172](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L172)

#### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-1"></a> `U` *extends* keyof [`GroupWorkerListener`](../interfaces/GroupWorkerListener.md)\<`DataType`, `ResultType`, `NameType`\> |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | [`GroupWorkerListener`](../interfaces/GroupWorkerListener.md)\<`DataType`, `ResultType`, `NameType`\>\[`U`\] |

#### Returns

`this`

#### Overrides

`Worker.once`

***

### off()

```ts
off<U>(event, listener): this;
```

Defined in: [src/group-worker.ts:179](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L179)

#### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-2"></a> `U` *extends* keyof [`GroupWorkerListener`](../interfaces/GroupWorkerListener.md)\<`DataType`, `ResultType`, `NameType`\> |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| `listener` | [`GroupWorkerListener`](../interfaces/GroupWorkerListener.md)\<`DataType`, `ResultType`, `NameType`\>\[`U`\] |

#### Returns

`this`

#### Overrides

`Worker.off`

***

### run()

```ts
run(): Promise<void>;
```

Defined in: [src/group-worker.ts:191](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L191)

Start processing when created with autorun: false. A worker that declares group options writes
them to Redis now, so producers release by them; one created with autorun: false and never run
(a processor switched off by a flag) opens nothing.

#### Returns

`Promise`\<`void`\>

#### Overrides

`Worker.run`

***

### emit()

```ts
emit<U>(event, ...args): boolean;
```

Defined in: [src/group-worker.ts:197](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L197)

#### Type Parameters

| Type Parameter |
| ------ |
| <a id="type-parameter-u-3"></a> `U` *extends* keyof `WorkerListener`\<`DataType`, `ResultType`, `NameType`\> |

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `event` | `U` |
| ...`args` | `Parameters`\<`WorkerListener`\<`DataType`, `ResultType`, `NameType`\>\[`U`\]\> |

#### Returns

`boolean`

#### Overrides

`Worker.emit`

***

### close()

```ts
close(force?): Promise<void>;
```

Defined in: [src/group-worker.ts:211](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L211)

Graceful by default: stops the reconciler, lets running jobs finish, then waits for their
group bookkeeping, so the next job of each group is already in BullMQ for another worker.
With `force`, it waits for neither and drops the group connection at once (in a Redis outage
the bookkeeping would never finish). Another worker's reconciler completes what it left.

#### Parameters

| Parameter | Type | Default value |
| ------ | ------ | ------ |
| `force?` | `boolean` | `false` |

#### Returns

`Promise`\<`void`\>

#### Overrides

`Worker.close`

***

### reconcile()

```ts
reconcile(): Promise<ReconcileSummary>;
```

Defined in: [src/group-worker.ts:283](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L283)

One reconciler pass. Exposed for tests and for running it from a cron instead of every worker.

#### Returns

`Promise`\<[`ReconcileSummary`](../interfaces/ReconcileSummary.md)\>
