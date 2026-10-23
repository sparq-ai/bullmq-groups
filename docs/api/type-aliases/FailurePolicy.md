[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / FailurePolicy

# Type Alias: FailurePolicy

```ts
type FailurePolicy = "continue" | "block";
```

Defined in: [src/types.ts:11](https://github.com/sparq-ai/bullmq-groups/blob/dc20d36ea32affa5489baa18d51bb368047f77d1/src/types.ts#L11)

What a group does when one of its jobs fails after its last attempt. Not in BullMQ Pro, whose
groups always behave as 'continue'.
- continue: the job lands in BullMQ's failed set and the group moves on to its next job.
- block: the group stops until an operator calls retryBlockedGroup() or skipBlockedGroup().
While a job is still retrying it keeps its slot. At group concurrency 1 that holds the whole
group, so retries never break FIFO; at concurrency n the other n-1 slots keep going.
