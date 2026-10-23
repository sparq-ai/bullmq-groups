[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupProcessor

# Type Alias: GroupProcessor\<DataType, ResultType, NameType\>

```ts
type GroupProcessor<DataType, ResultType, NameType> = (job, token?, signal?) => Promise<ResultType>;
```

Defined in: [src/group-worker.ts:44](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/group-worker.ts#L44)

A processor function, as BullMQ's, with the job typed as a GroupJob (job.gid, job.opts.group), as WorkerPro's ProcessorPro.

## Type Parameters

| Type Parameter | Default type |
| ------ | ------ |
| <a id="type-parameter-datatype"></a> `DataType` | `any` |
| <a id="type-parameter-resulttype"></a> `ResultType` | `any` |
| <a id="type-parameter-nametype"></a> `NameType` *extends* `string` | `string` |

## Parameters

| Parameter | Type |
| ------ | ------ |
| `job` | [`GroupJob`](../classes/GroupJob.md)\<`DataType`, `ResultType`, `NameType`\> |
| `token?` | `string` |
| `signal?` | `AbortSignal` |

## Returns

`Promise`\<`ResultType`\>
