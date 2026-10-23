[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / WorkerGroupOptions

# Interface: WorkerGroupOptions

Defined in: [src/types.ts:55](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L55)

The `group` worker option.

## Properties

### concurrency?

```ts
concurrency?: number;
```

Defined in: [src/types.ts:62](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L62)

How many jobs of one group may run at once, across every worker of the queue (BullMQ Pro's
group.concurrency). 1 runs each group strictly in order. Unset: no per-group limit, but jobs
of a group still start in order and take turns with other groups, as in BullMQ Pro.
Stored in Redis; give every worker of a queue the same value.

***

### failurePolicy?

```ts
failurePolicy?: FailurePolicy;
```

Defined in: [src/types.ts:64](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L64)

Not in BullMQ Pro. Default 'continue'. Stored in Redis; give every worker the same value.
