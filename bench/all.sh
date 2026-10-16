#!/usr/bin/env bash
# Runs the whole benchmark matrix; results land in bench/results/*.json, then bench/report.ts
# turns them into bench/RESULTS.md. Takes about 25 minutes on a laptop. Run it on a quiet machine.
# WARNING: flushes the Redis database at REDIS_HOST:REDIS_PORT (default 127.0.0.1:6390).
set -uo pipefail
cd "$(dirname "$0")/.."
run() {
  echo ">> $*"
  npx tsx bench/run.ts "$@" > /dev/null || echo "!! run failed: $*"
}

# 1. Drain throughput: 1,000 groups x 20 jobs already waiting, no-op jobs, one worker process.
for a in plain groups groups-nolimit queue-per-group delay-gate; do
  run --scenario throughput --approach "$a" --groups 1000 --jobs 20 --concurrency 50
done
# 1b. One busy group on its own with no group limit (BullMQ Pro's default): its jobs start one
#     release apart, so this is the slowest case for that mode.
run --scenario throughput --approach groups-nolimit --groups 1 --jobs 20000 --concurrency 50

# 2. Open-loop latency: jobs arrive at a fixed rate over 1,000 groups for 30 s, each takes 5 ms.
for a in plain groups queue-per-group delay-gate; do
  run --scenario latency --approach "$a" --groups 1000 --rate 1000 --duration 30 --concurrency 50 --work 5
done
for a in plain groups; do
  run --scenario latency --approach "$a" --groups 1000 --rate 3000 --duration 30 --concurrency 50 --work 5
done

# 3. Many groups.
for a in plain groups; do
  run --scenario throughput --approach "$a" --groups 10000 --jobs 2 --concurrency 50
  run --scenario throughput --approach "$a" --groups 100000 --jobs 1 --concurrency 50
done
run --scenario throughput --approach queue-per-group --groups 3000 --jobs 2 --concurrency 50

# 4. Skew: one group with 20,000 jobs added first, then 999 groups with one job each; 10 ms jobs.
for a in plain plain-dedup groups groups-c4 groups-nolimit queue-per-group delay-gate; do
  run --scenario skew --approach "$a" --groups 999 --hot 20000 --concurrency 20 --work 10 --timeout 180
done
# 4b. Same with a bigger backlog: 100,000 jobs for one group vs 999 groups with one job each.
for a in plain plain-dedup groups groups-c4 groups-nolimit; do
  run --scenario skew --approach "$a" --groups 999 --hot 100000 --concurrency 20 --work 10 --timeout 300
done

# 5. Memory and keys with 100,000 jobs waiting across 10,000 groups, no workers.
for a in plain groups queue-per-group; do
  run --scenario memory --approach "$a" --groups 10000 --jobs 10
done

# 6. Backlog: 20,000 groups with one 5 s job each, 4 workers at concurrency 10, measured for 75 s.
#    Released jobs wait in BullMQ past the reconciler's grace period, as minutes-long syncs do.
for a in plain groups; do
  run --scenario backlog --approach "$a" --groups 20000 --jobs 1 --concurrency 10 --work 5000 --workers 4 --duration 75
done
