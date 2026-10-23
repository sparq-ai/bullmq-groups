# Guarantees

What survives a crash, a reconnect and a failover — and what does not.

## Delivery

**At least once**, the same as BullMQ itself. With `maxStalledCount: 0`, a job whose worker crashed is failed instead of rerun, also as in BullMQ.

Through crashes, deploys and reconnects: a job that finished never runs again, a crashed one is rerun (which is the point), and a group never has more than `n` jobs running — except in two cases that plain BullMQ and BullMQ Pro share.

### Lock loss

A processor that blocks the event loop longer than `lockDuration`, or a worker cut off from Redis for that long, loses its job lock. BullMQ then hands the job to another worker while the first is still running it.

With `maxStalledCount: 0` the job is failed instead, and the group's next job can start while the first worker is still running the old one.

BullMQ's `lockRenewalFailed` event is the signal. Listen for it.

### Redis failover with async replication

The last writes before a failover can be lost — BullMQ's and ours alike — and a client can then act on state the new master never received.

The nonce and the start check prevent double runs and stale copies. But a job whose enqueue, BullMQ add or completion was among the lost writes can still be lost, or held for a grace period. **Plain BullMQ loses jobs in the same windows.**

## Crash windows and the reconciler

BullMQ's scripts and ours are separate Redis calls, so a process can die between them. Every `GroupWorker` runs a reconciler every 5 s. `reconcile()` can also be called from a cron.

It reads each due in-flight job with `Queue.getJob` — one hash read, whatever the queue size.

| What happened | What the reconciler sees | What it does |
| --- | --- | --- |
| A process released a job and died (or its reply was lost) before adding it to BullMQ | unconfirmed, no BullMQ job | Adds it, after min(`reconcileGraceMs`, 5 s). A double add is harmless: same job id, and the start check skips stale copies. |
| Died after the add, before confirming it | unconfirmed, BullMQ job with this nonce | Confirms it. The worker's start check also confirms it when the job starts. |
| A worker died after BullMQ finished the job, before the group was told. Also: BullMQ threw `Missing lock` on a completion resent after a reconnect and never emitted `completed` | confirmed, BullMQ job finished | Frees the slot, releases the group's next job. Confirmed jobs are checked after `reconcileGraceMs` (30 s), then with back-off up to every 5 minutes. |
| Someone removed the job from a dashboard | confirmed, no BullMQ job | Same as above. |
| A late or resent `Queue.add` re-created a job that already ran | caught by the start check | The copy is skipped, not run, and removed from BullMQ (`skipped` event). No `completed` is emitted for it; BullMQ's `QueueEvents` stream still records one. |
| BullMQ refuses a released job | caught at release | The job is dropped from its group (`rejected` event). |
| A group has a free slot and waiting jobs, but nothing released them — lost writes in a failover | the `kick` pass over the group index, 2,000 groups per pass, a full `ZSCAN` sweep | Releases them, emits `repaired`. A new default concurrency reaches every waiting group as soon as it is written. |
| A job has been running longer than `stuckAfterMs` (default 15 min) | BullMQ job active since `processedOn` | Emits `stuck`. |

The reconciler is not a cleanup afterthought. It is the mechanism that makes every non-atomic sequence safe, which is why it runs in **every** worker rather than one elected process.

## Edge cases

Each is covered by the tests named. The `edge-cases` suite holds the rarer failure modes: commands resent after a reconnect, a late `Queue.add`, reused job ids, options BullMQ refuses, a Redis out-of-memory error while handing jobs to BullMQ, races between the reconciler, retries and skips, and a Redis client made by another copy of ioredis.

### Two workers must never serve the same group

Picking a group and popping its job happen in one Lua script, inside the same script as the event that freed the slot. The release nonce stops resent or late commands from freeing a slot twice.

*Tests:* `fifo-fairness` (3 workers, 50 groups, 0 overlaps); `edge-cases` (resent ack, late add, reconciler race).

### Failure and retry ordering

**Head-of-line block while retrying.** A failing job keeps its slot through all its attempts and backoff. At group concurrency 1, nothing behind it starts. At concurrency `n` it holds only its own slot, and later jobs use the other `n`-1.

After the last attempt, `failurePolicy` decides: `continue` (default — the job goes to BullMQ's failed set, the group moves on) or `block` (the group stops until `retryBlockedGroup` or `skipBlockedGroup`).

A job retried from a dashboard runs only in a free slot of its group.

BullMQ Pro does **not** hold order across retries; its maintainers have acknowledged this since March 2024 ([pro-support #68](https://github.com/taskforcesh/bullmq-pro-support/issues/68), quoted in [#107](https://github.com/taskforcesh/bullmq-pro-support/issues/107)).

*Tests:* `failures`, `edge-cases`.

### Worker crash mid-job

**Depends on `maxStalledCount`.**

| Setting | Behaviour |
| --- | --- |
| `>= 1` (BullMQ default: 1) | the crashed job keeps its group's slot, BullMQ's stalled checker puts it back, and it reruns before anything behind it (at group concurrency 1) |
| `0` | BullMQ fails the stalled job instead of rerunning it. The group moves on without it (`continue`) or stops (`block`). `GroupWorker` warns at startup. |

Recovery takes up to about `lockDuration` + 2 × `stalledInterval` — roughly 30 minutes with 10-minute settings.

*Tests:* `crash` (real `SIGKILL`, both settings).

### Empty-group cleanup and the last-pop/new-job race

An idle group has 0 keys. Redis drops empty lists, and index entries are removed in the same script that empties them. "Last job finished" and "new job arrived" are each one script, so they cannot interleave.

Per-group settings (`pauseGroup`, `setGroupConcurrency`) are config and persist until changed.

*Tests:* `lifecycle`.

### Fairness under skew

BullMQ holds at most `n` jobs of the big group — one, with no group limit — so the small groups' jobs sit right behind them in the wait list.

Fairness is **by turns, not by time**: a slow group's turn lasts as long as its job.

*Tests:* `fifo-fairness`, bench skew runs (20,000 and 100,000 jobs).

### Delayed and priority jobs

A delayed job joins the **tail** of its group when it comes due, like BullMQ moving a due job to the end of its wait list. Jobs due in the same millisecond keep the order they were added in.

Priority orders jobs **still waiting** in the group, by BullMQ's rules: no priority first, then lower numbers. A job already released into BullMQ keeps its place. Released jobs carry no BullMQ priority, so there is no priority across groups — Pro's group priority is also only within a group.

*Tests:* `lifecycle`, `edge-cases`.

### Graceful shutdown, deploy mid-drain, pausing one group

`close()` waits for running jobs and their bookkeeping, so each group's next job is already in BullMQ for the next pod.

`close(true)` waits for neither and drops the group connection at once, so it returns even in a Redis outage; another worker's reconciler finishes what it left.

`pauseGroup` stops releases. A released job that has not started yet waits too — re-checked every `recheckMs`, default 5 s — until `resumeGroup`. Running jobs finish.

*Tests:* `lifecycle`, `edge-cases`.

### Redis memory, key count, TTLs

About 1 key per group with waiting jobs, 0 per idle group, plus a fixed handful per queue.

Memory per waiting job is **lower** than plain BullMQ for deep per-group backlogs with payloads over ~64 B, and **higher** for one job per group. See [benchmarks](../bench/RESULTS.md).

No TTLs are needed because cleanup is deterministic. BullMQ's `removeOnComplete`/`removeOnFail` still govern finished jobs.

### Reconnect and failover

Every script is safe to run twice. A resent `add` reports its first outcome while its job is pending or in flight, and a late or resent `Queue.add` cannot run a job twice.

A Redis error while handing jobs to BullMQ — connection loss, out of memory — never drops a job: the reconciler adds it later.

*Tests:* `reconnect` (every connection killed 3 times mid-run), `edge-cases`.

## Next

- [Production](./production.md) — the settings that decide whether these guarantees hold
- [Concepts](./concepts.md) — why the nonce and the start check exist
- [API reference](./api/README.md)
