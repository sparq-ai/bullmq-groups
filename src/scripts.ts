import { readFileSync } from 'fs';
import { join } from 'path';
import IORedis, { Redis, Cluster, RedisOptions } from 'ioredis';
import type { ConnectionOptions } from 'bullmq';

const LUA_DIR = join(__dirname, 'lua');
const SCRIPTS = ['enqueue', 'ack', 'markAdded', 'promote', 'control', 'lease', 'kick', 'claim', 'remove', 'status', 'repair'] as const;
type ScriptName = (typeof SCRIPTS)[number];

let sources: Record<ScriptName, string> | undefined;

function loadSources(): Record<ScriptName, string> {
  if (!sources) {
    const helpers = readFileSync(join(LUA_DIR, 'helpers.lua'), 'utf8');
    sources = Object.fromEntries(
      SCRIPTS.map(name => [name, `${helpers}\n${readFileSync(join(LUA_DIR, `${name}.lua`), 'utf8')}`]),
    ) as Record<ScriptName, string>;
  }
  return sources;
}

type Reply = string | number | Reply[];

/** ioredis client with the group scripts defined as commands. Every command takes (metaKey, keyBase, ...args). */
export type GroupRedis = (Redis | Cluster) & {
  [K in `gq_${ScriptName}`]: (metaKey: string, base: string, ...args: (string | number | (string | number)[])[]) => Promise<Reply[]>;
};

/**
 * A dedicated connection for the group scripts, separate from BullMQ's own connections so the
 * package relies only on BullMQ's public API and works the same on BullMQ 5 and 6.
 * It is built the way BullMQ builds its clients: a `url` option is honoured, and a worker's
 * connection never gives up on a command (maxRetriesPerRequest: null), as BullMQ requires for
 * workers.
 */
export function createGroupRedis(connection: ConnectionOptions | undefined, opts: { worker?: boolean } = {}): GroupRedis {
  const override: Partial<RedisOptions> = opts.worker ? { maxRetriesPerRequest: null } : {};
  connection ??= {}; // as BullMQ: no connection means localhost:6379
  let client: Redis | Cluster;
  if (connection instanceof IORedis) {
    client = connection.duplicate(override);
  } else if (connection instanceof Cluster) {
    client = connection.duplicate();
  } else if (typeof (connection as { duplicate?: unknown }).duplicate === 'function') {
    throw new Error('Pass ioredis options or an ioredis instance; other client types are not supported by bullmq-groups');
  } else {
    const { url, ...rest } = connection as RedisOptions & { url?: string };
    client = url ? new IORedis(url, { ...rest, ...override }) : new IORedis({ ...rest, ...override });
  }
  for (const [name, lua] of Object.entries(loadSources())) {
    client.defineCommand(`gq_${name}`, { numberOfKeys: 1, lua });
  }
  return client as GroupRedis;
}

/**
 * The Redis client a BullMQ Queue or Worker already holds: `client` on BullMQ 5, its Redis
 * backend's `client` on BullMQ 6. Used for cheap reads (is there any group state?) that should not
 * open a connection of their own on a queue that never sees a grouped job. Undefined if this
 * BullMQ version exposes neither.
 */
export async function bullmqClient(owner: object): Promise<Redis | undefined> {
  const o = owner as { client?: unknown; backend?: { client?: unknown } };
  const client = o.client ?? o.backend?.client;
  return client ? ((await client) as Redis) : undefined;
}

/**
 * Resolves once the client can take commands. Only needed with enableOfflineQueue: false, where a
 * command sent before the connection is ready fails instead of waiting.
 */
export async function untilReady(client: Redis | Cluster): Promise<void> {
  if (client.status === 'ready') return;
  if (client.status === 'wait') {
    await client.connect();
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const onReady = () => {
      client.off('end', onEnd);
      resolve();
    };
    const onEnd = () => {
      client.off('ready', onReady);
      reject(new Error('Connection is closed.'));
    };
    client.once('ready', onReady);
    client.once('end', onEnd);
  });
}
