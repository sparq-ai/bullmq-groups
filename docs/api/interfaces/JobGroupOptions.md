[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / JobGroupOptions

# Interface: JobGroupOptions

Defined in: [src/types.ts:14](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L14)

The `group` job option. Same fields as BullMQ Pro's.

## Properties

### id

```ts
id: string;
```

Defined in: [src/types.ts:16](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L16)

The group the job belongs to. Jobs of a group start in the order they were added.

***

### priority?

```ts
priority?: number;
```

Defined in: [src/types.ts:21](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L21)

Order inside the group, as in BullMQ Pro: jobs without a priority first, then 1 (highest)
up to 2,097,152 (lowest). A prioritized job waits behind every unprioritized one of its group.

***

### maxSize?

```ts
maxSize?: number;
```

Defined in: [src/types.ts:26](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L26)

Refuse the add with GroupMaxSizeExceededError when the group already has this many jobs
waiting. As in BullMQ Pro, not supported by addBulk.
