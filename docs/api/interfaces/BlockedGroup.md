[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / BlockedGroup

# Interface: BlockedGroup

Defined in: [src/types.ts:122](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L122)

## Properties

### groupId

```ts
groupId: string;
```

Defined in: [src/types.ts:123](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L123)

***

### jobId

```ts
jobId: string;
```

Defined in: [src/types.ts:125](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L125)

The oldest blocking job: the one retryBlockedGroup and skipBlockedGroup act on.

***

### jobIds

```ts
jobIds: string[];
```

Defined in: [src/types.ts:127](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L127)

Every job blocking the group, oldest first (more than one only at group concurrency above 1).
