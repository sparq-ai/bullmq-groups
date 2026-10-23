[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupJobsOptions

# Interface: GroupJobsOptions

Defined in: [src/types.ts:40](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L40)

Job options of a GroupQueue (BullMQ Pro's JobsProOptions). Without `group` a job is a plain
BullMQ job and every BullMQ option applies. With `group`, jobId, delay, attempts, backoff,
removeOnComplete/Fail and the other BullMQ options work as usual, except:
 - jobId: adding an id that is waiting or running in its group, or that BullMQ still holds,
   is a no-op that returns that job.
 - delay: the job joins the tail of its group when due.
 - priority: use group.priority instead.
 - deduplication: simple mode only ({ id }); ttl, extend and replace are not supported.
 - repeat, lifo, parent (flows) and debounce are not supported.

## Extends

- `JobsOptions`

## Properties

### group?

```ts
group?: JobGroupOptions;
```

Defined in: [src/types.ts:41](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L41)
