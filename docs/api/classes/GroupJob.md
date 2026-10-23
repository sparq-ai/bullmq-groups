[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupJob

# Class: GroupJob\<DataType, ReturnType, NameType\>

Defined in: [src/group-job.ts:24](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-job.ts#L24)

A BullMQ job that knows its group (BullMQ Pro's JobPro). Every job a GroupQueue or GroupWorker
returns is one, grouped or not. A grouped job that is still waiting in its group, and so not yet
in BullMQ, answers getState() and remove() as well.

## Extends

- `Job`\<`DataType`, `ReturnType`, `NameType`\>

## Type Parameters

| Type Parameter | Default type |
| ------ | ------ |
| <a id="type-parameter-datatype"></a> `DataType` | `any` |
| <a id="type-parameter-returntype"></a> `ReturnType` | `any` |
| <a id="type-parameter-nametype"></a> `NameType` *extends* `string` | `string` |

## Constructors

### Constructor

```ts
new GroupJob<DataType, ReturnType, NameType>(
   queue, 
   name, 
   data, 
   opts?, 
   id?
): GroupJob<DataType, ReturnType, NameType>;
```

Defined in: node\_modules/bullmq/dist/esm/classes/job.d.ts:152

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `queue` | `MinimalQueue` | - |
| `name` | `NameType` | The name of the Job |
| `data` | `DataType` | The payload for this job. Note: The `DataType` type parameter describes the *shape* of the payload, not its runtime class. Job data is serialized with `JSON.stringify` before being stored in Redis and deserialized with `JSON.parse` when read back, so when a worker picks up the job `job.data` will contain a JSON-compatible value such as an object, array, string, number, boolean, or `null`. Class instances passed as `data` lose their prototype (and therefore any methods, getters/setters, or non-enumerable properties) on the worker side. If you need methods, store the data as a plain object and re-instantiate the class inside your processor, or rely on `JSON.stringify`'s built-in `toJSON()` hook to control serialization. |
| `opts?` | `JobsOptions` | The options object for this job. |
| `id?` | `string` | - |

#### Returns

`GroupJob`\<`DataType`, `ReturnType`, `NameType`\>

#### Inherited from

`Job<DataType, ReturnType, NameType>.constructor`

## Properties

### opts

```ts
opts: GroupJobsOptions;
```

Defined in: [src/group-job.ts:29](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-job.ts#L29)

The options object for this job.

#### Overrides

`Job.opts`

## Accessors

### gid

#### Get Signature

```ts
get gid(): string | undefined;
```

Defined in: [src/group-job.ts:32](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-job.ts#L32)

The job's group id (BullMQ Pro's JobPro.gid), or undefined for a job added without a group.

##### Returns

`string` \| `undefined`

## Methods

### getState()

```ts
getState(): Promise<JobState | "unknown">;
```

Defined in: [src/group-job.ts:36](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-job.ts#L36)

Get current state.

#### Returns

`Promise`\<`JobState` \| `"unknown"`\>

Returns one of these values:
'completed', 'failed', 'delayed', 'active', 'waiting', 'waiting-children', 'unknown'.

#### Overrides

`Job.getState`

***

### remove()

```ts
remove(opts?): Promise<void>;
```

Defined in: [src/group-job.ts:42](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-job.ts#L42)

Completely remove the job from the queue.
Note, this call will throw an exception if the job
is being processed when the call is performed.

#### Parameters

| Parameter | Type | Description |
| ------ | ------ | ------ |
| `opts?` | \{ `removeChildren?`: `boolean`; \} | Options to remove a job |
| `opts.removeChildren?` | `boolean` | - |

#### Returns

`Promise`\<`void`\>

#### Overrides

`Job.remove`
