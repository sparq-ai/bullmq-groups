# Benchmark results

Machine: 11th Gen Intel(R) Core(TM) i5-11320H @ 3.20GHz, 8 threads, 15 GB RAM. Redis 7.4.10 in Docker on the same machine, persistence off. Node v22.22.3, BullMQ 5.81.5. Producer and all workers run in one Node process per run; each run starts from an empty Redis. Every row comes from one run of bench/all.sh on 2026-10-16.

Redis commands per job count every command Redis executed, including the ones inside Lua scripts. Script calls per job approximate network round trips, since BullMQ and this package do almost everything in scripts. Order inversions count jobs that started before an earlier job of the same group; overlaps count jobs that started while another job of their group was running.

## Drain throughput: 1,000 groups x 20 jobs waiting, no-op jobs, worker concurrency 50

Redis cost is counted end to end (adding the jobs plus running them): plain BullMQ pays for its `add` up front, this package pays for it when a job is released.

| Approach | Drain jobs/s | Redis commands/job | Script calls/job | Redis CPU per 1k jobs | Redis connections | Order inversions | Overlaps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 19,286 | 32 | 2 | 74 ms | 4 | 0 | 0 |
| This package, group concurrency 1 | 5,394 | 89 | 5 | 196 ms | 7 | 0 | 0 |
| This package, no group limit (Pro default) | 5,081 | 100.1 | 5 | 208 ms | 7 | 0 | 0 |
| Queue per group | 9,732 | 32.5 | 2.1 | 101 ms | 1,002 | 0 | 0 |
| Delay out-of-turn jobs | 11,648 | 38 | 4 | 100 ms | 5 | 0 | 0 |

## Open-loop latency: jobs arrive at a fixed rate across 1,000 groups for 30 s, each job takes 5 ms

Latency is from `add()` being called to the processor starting the job.

| Approach | Offered rate | Achieved | p50 | p95 | p99 | Redis commands/job | Redis CPU per 1k jobs | Order inversions | Overlaps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 1,000/s | 1,000/s | 1 ms | 2 ms | 3 ms | 43.7 | 380 ms | 1 | 119 |
| This package, group concurrency 1 | 1,000/s | 1,000/s | 1 ms | 2 ms | 3 ms | 109.1 | 455 ms | 0 | 0 |
| Queue per group | 1,000/s | 1,000/s | 1 ms | 2 ms | 5 ms | 52.3 | 402 ms | 0 | 0 |
| Delay out-of-turn jobs | 1,000/s | 1,000/s | 1 ms | 2 ms | 3 ms | 49.9 | 395 ms | 0 | 0 |
| Plain BullMQ (no groups) | 3,000/s | 2,998/s | 1 ms | 3 ms | 5 ms | 37.6 | 147 ms | 9 | 1,233 |
| This package, group concurrency 1 | 3,000/s | 2,998/s | 4 ms | 16 ms | 26 ms | 101 | 267 ms | 0 | 0 |

## Many groups: enqueue and drain, worker concurrency 50

| Approach | Groups x jobs | Enqueue jobs/s | Drain jobs/s | Redis memory per waiting job | Keys while waiting | Keys after drain | Redis connections |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| This package, no group limit (Pro default) | 1 x 20000 | 50,125 | 2,357 | 337 B | 14 | 5 | 7 |
| Queue per group | 3,000 x 2 | 12,605 | 1,907 | 3,045 B | 21,000 | 12,000 | 3,002 |
| Plain BullMQ (no groups) | 10,000 x 2 | 27,285 | 18,605 | 656 B | 20,005 | 4 | 4 |
| This package, group concurrency 1 | 10,000 x 2 | 17,256 | 7,087 | 903 B | 20,011 | 8 | 7 |
| Plain BullMQ (no groups) | 100,000 x 1 | 29,283 | 18,929 | 630 B | 100,005 | 4 | 4 |
| This package, group concurrency 1 | 100,000 x 1 | 11,533 | 9,817 | 1,087 B | 100,009 | 5 | 7 |

## Skew: one group with 20,000 jobs added first, then 999 groups with one job each; 10 ms jobs, worker concurrency 20

The question is how long the small groups wait behind the big one.

| Approach | All small groups done after | Small-group start latency p50 | p99 | Big-group jobs done meanwhile | Big-group jobs dropped at add | Wasted picks | Redis commands/job | Redis connections |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 12.5 s | 12.2 s | 12.5 s | 20,000 | 0 |  | 23 | 4 |
| Plain BullMQ, one job per group (deduplication) | 631 ms | 361 ms | 652 ms | 1 | 19,999 |  | 27.2 | 4 |
| This package, group concurrency 1 | 728 ms | 464 ms | 815 ms | 2 | 0 |  | 49.3 | 7 |
| This package, group concurrency 4 | 829 ms | 539 ms | 920 ms | 8 | 0 |  | 49.5 | 7 |
| This package, no group limit (Pro default) | 753 ms | 471 ms | 842 ms | 2 | 0 |  | 60 | 7 |
| Queue per group | 981 ms | 1,065 ms | 1,072 ms | 2 | 0 |  | 48.9 | 1,002 |
| Delay out-of-turn jobs | 2,497 ms | 2,196 ms | 2,514 ms | 1 | 0 | 20,134 | 614.9 | 5 |

The flip side of strict per-group order: at group concurrency 1 the big group runs one job at a time, so its 20,000 x 10 ms jobs need at least 200 s to drain (50 s at concurrency 4), while plain BullMQ runs them 20 at a time. That cap is the point when a group's jobs must not overlap, but it is a real cost for a group with a big backlog. These drain times are arithmetic, not measured. Deduplicating by group keeps the small groups fast by dropping the big group's other jobs at add time.

## Skew: one group with 100,000 jobs added first, then 999 groups with one job each; 10 ms jobs, worker concurrency 20

The question is how long the small groups wait behind the big one.

| Approach | All small groups done after | Small-group start latency p50 | p99 | Big-group jobs done meanwhile | Big-group jobs dropped at add | Wasted picks | Redis commands/job | Redis connections |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 60.3 s | 60 s | 60.3 s | 100,000 | 0 |  | 23 | 4 |
| Plain BullMQ, one job per group (deduplication) | 616 ms | 349 ms | 629 ms | 1 | 99,999 |  | 27.2 | 4 |
| This package, group concurrency 1 | 771 ms | 504 ms | 858 ms | 2 | 0 |  | 49.3 | 7 |
| This package, group concurrency 4 | 798 ms | 503 ms | 882 ms | 8 | 0 |  | 49.2 | 7 |
| This package, no group limit (Pro default) | 747 ms | 487 ms | 835 ms | 2 | 0 |  | 59.9 | 7 |

The flip side of strict per-group order: at group concurrency 1 the big group runs one job at a time, so its 100,000 x 10 ms jobs need at least 1,000 s to drain (250 s at concurrency 4), while plain BullMQ runs them 20 at a time. That cap is the point when a group's jobs must not overlap, but it is a real cost for a group with a big backlog. These drain times are arithmetic, not measured. Deduplicating by group keeps the small groups fast by dropping the big group's other jobs at add time.

## Memory: 100,000 jobs waiting across 10,000 groups, no workers

| Approach | Redis memory per waiting job | Total Redis memory | Keys | Keys per group | Enqueue jobs/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 630 B | 63.3 MB | 100,005 | 10 | 28,960 |
| This package, group concurrency 1 | 387 B | 40.1 MB | 20,011 | 2 | 33,772 |
| Queue per group | 1,096 B | 109 MB | 150,000 | 15 | 21,048 |

## Backlog: 20,000 groups with one 5 s job each, 4 worker processes' worth of workers at concurrency 10, measured for 75 s

Slow jobs keep most released jobs waiting in BullMQ's wait list for longer than the reconciler's 30 s grace period, as minutes-long syncs do. Every worker runs the reconciler. A separate client pings Redis every 10 ms to measure what everyone else sees.

| Approach | Redis CPU | PING p99 | PING max | Top Redis time |
| --- | ---: | ---: | ---: | --- |
| Plain BullMQ (no groups) | 2.3% | 1.7 ms | 9.5 ms | evalsha 122 ms, ping 15 ms, xadd 9 ms |
| This package, group concurrency 1 | 3.4% | 1.8 ms | 23.4 ms | evalsha 655 ms, hgetall 111 ms, zadd 57 ms |
