[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupStats

# Interface: GroupStats

Defined in: [src/types.ts:138](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L138)

## Properties

### waitingGroups

```ts
waitingGroups: number;
```

Defined in: [src/types.ts:140](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L140)

Groups with at least one job waiting.

***

### activeGroups

```ts
activeGroups: number;
```

Defined in: [src/types.ts:142](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L142)

Groups with at least one job released to BullMQ and not finished.

***

### waiting

```ts
waiting: number;
```

Defined in: [src/types.ts:143](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L143)

***

### inflight

```ts
inflight: number;
```

Defined in: [src/types.ts:144](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L144)

***

### delayed

```ts
delayed: number;
```

Defined in: [src/types.ts:145](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L145)

***

### pausedGroups

```ts
pausedGroups: number;
```

Defined in: [src/types.ts:146](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L146)

***

### blockedGroups

```ts
blockedGroups: number;
```

Defined in: [src/types.ts:147](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L147)

***

### oldestWaitingMs

```ts
oldestWaitingMs: number;
```

Defined in: [src/types.ts:149](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L149)

Age of the oldest job that is ready but waiting behind its group, 0 when none.
