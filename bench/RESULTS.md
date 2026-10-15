# Benchmark results

Machine: 11th Gen Intel(R) Core(TM) i5-11320H @ 3.20GHz, 8 threads, 15 GB RAM. Redis 7.4.10 in Docker on the same machine, persistence off. Node v22.22.3, BullMQ 5.81.5. Producer and all workers run in one Node process per run; each run starts from an empty Redis.

Redis commands per job count every command Redis executed, including the ones inside Lua scripts. Script calls per job approximate network round trips, since BullMQ and this package do almost everything in scripts. Order inversions count jobs that started before an earlier job of the same group; overlaps count jobs that started while another job of their group was running.

## Drain throughput: 1,000 groups x 20 jobs waiting, no-op jobs, worker concurrency 50

Redis cost is counted end to end (adding the jobs plus running them): plain BullMQ pays for its `add` up front, this package pays for it when a job is released.

| Approach | Drain jobs/s | Redis commands/job | Script calls/job | Redis CPU per 1k jobs | Redis connections | Order inversions | Overlaps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 18,587 | 32 | 2 | 76 ms | 4 | 0 | 0 |
| This package, group concurrency 1 | 7,138 | 76.1 | 3.9 | 152 ms | 7 | 0 | 0 |
| Queue per group | 9,515 | 32.8 | 2.1 | 103 ms | 1,002 | 0 | 0 |
| Delay out-of-turn jobs | 12,430 | 38 | 4 | 94 ms | 5 | 0 | 0 |

## Open-loop latency: jobs arrive at a fixed rate across 1,000 groups for 30 s, each job takes 5 ms

Latency is from `add()` being called to the processor starting the job.

| Approach | Offered rate | Achieved | p50 | p95 | p99 | Redis commands/job | Redis CPU per 1k jobs | Order inversions | Overlaps |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 1,000/s | 1,000/s | 1 ms | 2 ms | 3 ms | 44.1 | 356 ms | 3 | 113 |
| This package, group concurrency 1 | 1,000/s | 1,000/s | 1 ms | 2 ms | 3 ms | 96.2 | 438 ms | 0 | 0 |
| Queue per group | 1,000/s | 1,000/s | 1 ms | 2 ms | 6 ms | 52.4 | 380 ms | 0 | 0 |
| Delay out-of-turn jobs | 1,000/s | 998/s | 1 ms | 2 ms | 3 ms | 49.7 | 379 ms | 0 | 0 |
| Plain BullMQ (no groups) | 3,000/s | 2,998/s | 1 ms | 3 ms | 5 ms | 37.8 | 147 ms | 9 | 1,228 |
| This package, group concurrency 1 | 3,000/s | 2,998/s | 2 ms | 5 ms | 9 ms | 89.9 | 225 ms | 0 | 0 |

## Many groups: enqueue and drain, worker concurrency 50

| Approach | Groups x jobs | Enqueue jobs/s | Drain jobs/s | Redis memory per waiting job | Keys while waiting | Keys after drain | Redis connections |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Queue per group | 3,000 x 2 | 8,863 | 2,141 | 3,081 B | 21,000 | 13,976 | 3,002 |
| Plain BullMQ (no groups) | 10,000 x 2 | 26,667 | 18,002 | 659 B | 20,005 | 4 | 4 |
| This package, group concurrency 1 | 10,000 x 2 | 20,986 | 8,799 | 867 B | 20,011 | 5 | 7 |
| Plain BullMQ (no groups) | 100,000 x 1 | 28,877 | 18,758 | 629 B | 100,005 | 4 | 4 |
| This package, group concurrency 1 | 100,000 x 1 | 13,470 | 12,157 | 1,008 B | 100,009 | 8 | 7 |

## Skew: one group with 20,000 jobs added first, then 999 groups with one job each; 10 ms jobs, worker concurrency 20

The question is how long the small groups wait behind the big one.

| Approach | All small groups done after | Small-group start latency p50 | p99 | Big-group jobs done meanwhile | Big-group jobs/s | Wasted picks | Redis commands/job | Redis connections |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 12.3 s | 12 s | 12.3 s | 20,000 | 1,630 |  | 23 | 4 |
| This package, group concurrency 1 | 603 ms | 383 ms | 669 ms | 2 | 3 |  | 41.3 | 7 |
| This package, group concurrency 4 | 615 ms | 412 ms | 686 ms | 8 | 13 |  | 41.5 | 7 |
| Queue per group | 1,015 ms | 1,115 ms | 1,123 ms | 2 | 2 |  | 49.9 | 1,002 |
| Delay out-of-turn jobs | 2,354 ms | 2,029 ms | 2,367 ms | 1 | 0 | 20,181 | 616.4 | 5 |

The flip side of strict per-group order: at group concurrency 1 the big group runs one job at a time, so its 20,000 x 10 ms jobs need at least 200 s to drain (50 s at concurrency 4), while plain BullMQ runs them 20 at a time. That cap is the point when a shop's jobs must not overlap, but it is a real cost for a shop with a big backlog. These drain times are arithmetic, not measured.

## Memory: 100,000 jobs waiting across 10,000 groups, no workers

| Approach | Redis memory per waiting job | Total Redis memory | Keys | Keys per group | Enqueue jobs/s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Plain BullMQ (no groups) | 629 B | 62.2 MB | 100,005 | 10 | 29,265 |
| This package, group concurrency 1 | 374 B | 37.7 MB | 20,011 | 2 | 44,405 |
| Queue per group | 1,095 B | 107.9 MB | 150,000 | 15 | 20,425 |
