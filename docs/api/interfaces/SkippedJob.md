[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / SkippedJob

# Interface: SkippedJob

Defined in: [src/types.ts:160](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L160)

A BullMQ job the worker did not run because it is not the release its group is waiting for.

## Properties

### jobId

```ts
jobId: string;
```

Defined in: [src/types.ts:161](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L161)

***

### groupId

```ts
groupId: string;
```

Defined in: [src/types.ts:162](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L162)

***

### reason

```ts
reason: "stale" | "ghost";
```

Defined in: [src/types.ts:164](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L164)

'stale': another release of this job id is in flight. 'ghost': the job already finished; a late or repeated add re-created it.
