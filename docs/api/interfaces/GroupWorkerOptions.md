[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupWorkerOptions

# Interface: GroupWorkerOptions

Defined in: [src/types.ts:67](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L67)

## Extends

- `WorkerOptions`

## Properties

### group?

```ts
group?: WorkerGroupOptions;
```

Defined in: [src/types.ts:68](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L68)

***

### reconcileIntervalMs?

```ts
reconcileIntervalMs?: number;
```

Defined in: [src/types.ts:71](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L71)

How often this worker runs the reconciler. Default 5 s.

***

### reconcileGraceMs?

```ts
reconcileGraceMs?: number;
```

Defined in: [src/types.ts:76](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L76)

How long a confirmed in-flight job goes unchecked before the reconciler looks at it (then with
back-off). Unconfirmed releases are checked after min(this, 5 s). Stored in Redis. Default 30 s.

***

### stuckAfterMs?

```ts
stuckAfterMs?: number;
```

Defined in: [src/types.ts:78](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L78)

A job running longer than this is reported through the 'stuck' event. Default 15 min.

***

### delayedPollMs?

```ts
delayedPollMs?: number;
```

Defined in: [src/types.ts:80](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L80)

Upper bound on how late a delayed job joins its group. Default 1 s.

***

### recheckMs?

```ts
recheckMs?: number;
```

Defined in: [src/types.ts:82](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L82)

A released job of a paused group, or a retried job waiting for a free slot, is looked at again after this. Default 5 s.
