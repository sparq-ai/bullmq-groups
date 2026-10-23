[**@sparq/bullmq-groups**](../README.md)

***

[@sparq/bullmq-groups](../README.md) / GroupMaxSizeExceededError

# Class: GroupMaxSizeExceededError

Defined in: [src/types.ts:98](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L98)

Thrown by GroupQueue.add when the job's group already has group.maxSize jobs waiting.

## Extends

- `Error`

## Constructors

### Constructor

```ts
new GroupMaxSizeExceededError(groupId, maxSize): GroupMaxSizeExceededError;
```

Defined in: [src/types.ts:99](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L99)

#### Parameters

| Parameter | Type |
| ------ | ------ |
| `groupId` | `string` |
| `maxSize` | `number` |

#### Returns

`GroupMaxSizeExceededError`

#### Overrides

`Error.constructor`

## Properties

### groupId

```ts
readonly groupId: string;
```

Defined in: [src/types.ts:100](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L100)

***

### maxSize

```ts
readonly maxSize: number;
```

Defined in: [src/types.ts:101](https://github.com/sparq-ai/bullmq-groups/blob/main/src/types.ts#L101)
