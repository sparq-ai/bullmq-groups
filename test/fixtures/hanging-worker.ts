// Started as a child process by crash.test.ts and killed with SIGKILL while its job runs.
import { GroupWorker } from '../../src';

const [name, port] = process.argv.slice(2);
new GroupWorker(
  name,
  async job => {
    process.stdout.write(`started ${job.data.seq}\n`);
    await new Promise(() => undefined); // never finishes
  },
  { connection: { host: '127.0.0.1', port: Number(port) }, concurrency: 1, lockDuration: 1_000, stalledInterval: 300 },
);
