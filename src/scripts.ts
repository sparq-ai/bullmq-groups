import { readFileSync } from 'fs';
import { join } from 'path';
import IORedis, { Redis, Cluster, RedisOptions } from 'ioredis';
import type { ConnectionOptions } from 'bullmq';

const LUA_DIR = join(__dirname, 'lua');
const SCRIPTS = ['enqueue', 'ack', 'markAdded', 'promote', 'control', 'lease', 'kick'] as const;
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
  [K in `gq_${ScriptName}`]: (metaKey: string, base: string, ...args: (string | number)[]) => Promise<Reply[]>;
};

/**
 * A dedicated connection for the group scripts. It is separate from BullMQ's own connections so
 * the package only relies on BullMQ's public API and works the same on BullMQ 5 and 6.
 */
export function createGroupRedis(connection: ConnectionOptions): GroupRedis {
  let client: Redis | Cluster;
  if (connection instanceof IORedis || connection instanceof Cluster) {
    client = connection.duplicate();
  } else if (typeof (connection as { duplicate?: unknown }).duplicate === 'function') {
    throw new Error('Pass ioredis options or an ioredis instance; other client types are not supported by this POC');
  } else {
    client = new IORedis(connection as RedisOptions);
  }
  for (const [name, lua] of Object.entries(loadSources())) {
    client.defineCommand(`gq_${name}`, { numberOfKeys: 1, lua });
  }
  return client as GroupRedis;
}
