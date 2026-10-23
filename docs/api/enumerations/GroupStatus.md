[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupStatus

# Enumeration: GroupStatus

Defined in: [src/types.ts:86](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L86)

A group's status (BullMQ Pro's GroupStatus, plus Blocked).

## Enumeration Members

| Enumeration Member | Value | Description | Defined in |
| ------ | ------ | ------ | ------ |
| <a id="enumeration-member-waiting"></a> `Waiting` | `"waiting"` | - | [src/types.ts:87](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L87) |
| <a id="enumeration-member-paused"></a> `Paused` | `"paused"` | - | [src/types.ts:88](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L88) |
| <a id="enumeration-member-limited"></a> `Limited` | `"limited"` | Rate limited. Never reported: group rate limits are not supported. | [src/types.ts:90](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L90) |
| <a id="enumeration-member-maxed"></a> `Maxed` | `"maxed"` | As many of its jobs are running as its concurrency allows. | [src/types.ts:92](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L92) |
| <a id="enumeration-member-blocked"></a> `Blocked` | `"blocked"` | Not in BullMQ Pro: a job failed for good under failurePolicy 'block'. | [src/types.ts:94](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L94) |
