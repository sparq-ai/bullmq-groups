[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupQueueListener

# Interface: GroupQueueListener

Defined in: [src/group-queue.ts:68](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L68)

Events a GroupQueue adds to BullMQ's Queue events.

## Properties

### rejected

```ts
rejected: (job) => void;
```

Defined in: [src/group-queue.ts:70](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L70)

BullMQ refused a released job (for example sizeLimit exceeded); it was dropped from its group.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `job` | [`RejectedJob`](RejectedJob.md) |

#### Returns

`void`

***

### blocked

```ts
blocked: (groupId, jobId) => void;
```

Defined in: [src/group-queue.ts:72](https://github.com/sparq-ai/bullmq-groups/blob/main/src/group-queue.ts#L72)

A job failed for good under failurePolicy 'block', so its group stopped.

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` |
| `jobId` | `string` |

#### Returns

`void`
