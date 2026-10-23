[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupWorkerListener

# Interface: GroupWorkerListener\<DataType, ResultType, NameType\>

Defined in: [src/group-worker.ts:20](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L20)

Events a GroupWorker adds to BullMQ's Worker events.

## Extends

- `WorkerListener`\<`DataType`, `ResultType`, `NameType`\>

## Type Parameters

| Type Parameter | Default type |
| ------ | ------ |
| <a id="type-parameter-datatype"></a> `DataType` | `any` |
| <a id="type-parameter-resulttype"></a> `ResultType` | `any` |
| <a id="type-parameter-nametype"></a> `NameType` *extends* `string` | `string` |

## Properties

### stuck

```ts
stuck: (job) => void;
```

Defined in: [src/group-worker.ts:23](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L23)

A job has been running for longer than stuckAfterMs. Reported by the reconciler, at most once per check.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `job` | [`StuckJob`](StuckJob.md) |

#### Returns

`void`

***

### blocked

```ts
blocked: (groupId, jobId) => void;
```

Defined in: [src/group-worker.ts:25](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L25)

A job failed for good under failurePolicy 'block', so its group stopped.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` |
| `jobId` | `string` |

#### Returns

`void`

***

### repaired

```ts
repaired: (groupIds) => void;
```

Defined in: [src/group-worker.ts:27](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L27)

The reconciler released jobs of groups that had room but no release (lost writes, a new default concurrency).

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupIds` | `string`[] |

#### Returns

`void`

***

### skipped

```ts
skipped: (job) => void;
```

Defined in: [src/group-worker.ts:29](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L29)

A BullMQ job the worker did not run because it is not the release its group is waiting for.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `job` | [`SkippedJob`](SkippedJob.md) |

#### Returns

`void`

***

### rejected

```ts
rejected: (job) => void;
```

Defined in: [src/group-worker.ts:31](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-worker.ts#L31)

BullMQ refused to add a released job; it was dropped from its group.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `job` | [`RejectedJob`](RejectedJob.md) |

#### Returns

`void`
