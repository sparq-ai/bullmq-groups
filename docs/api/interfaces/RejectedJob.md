[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / RejectedJob

# Interface: RejectedJob

Defined in: [src/types.ts:168](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L168)

A released job BullMQ refused to add (for example sizeLimit exceeded). It is dropped from its group.

## Properties

### jobId

```ts
jobId: string;
```

Defined in: [src/types.ts:169](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L169)

***

### groupId

```ts
groupId: string;
```

Defined in: [src/types.ts:170](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L170)

***

### error

```ts
error: Error;
```

Defined in: [src/types.ts:171](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L171)
