[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupCounts

# Interface: GroupCounts

Defined in: [src/types.ts:108](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L108)

## Properties

### waiting

```ts
waiting: number;
```

Defined in: [src/types.ts:109](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L109)

***

### prioritized

```ts
prioritized: number;
```

Defined in: [src/types.ts:110](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L110)

***

### active

```ts
active: number;
```

Defined in: [src/types.ts:112](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L112)

Jobs handed to workers and not finished: running, retrying or about to start.

***

### concurrency

```ts
concurrency: number;
```

Defined in: [src/types.ts:114](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L114)

Infinity when the group has no limit.

***

### paused

```ts
paused: boolean;
```

Defined in: [src/types.ts:115](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L115)

***

### blockedJobId

```ts
blockedJobId: string | null;
```

Defined in: [src/types.ts:117](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L117)

The oldest job blocking the group under failurePolicy 'block', or null.

***

### headReadyAt

```ts
headReadyAt: number | null;
```

Defined in: [src/types.ts:119](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L119)

When the job this group runs next became ready (ms epoch), or null when nothing is waiting.
