#!/usr/bin/env bash
# Runs the whole benchmark matrix; results land in bench/results/*.json, then bench/report.ts
# turns them into bench/RESULTS.md. Takes about 15 minutes on a laptop.
# WARNING: flushes the Redis database at REDIS_HOST:REDIS_PORT (default 127.0.0.1:6390).
set -uo pipefail
cd "$(dirname "$0")/.."
run() {
  echo ">> $*"
  npx tsx bench/run.ts "$@" > /dev/null || echo "!! run failed: $*"
}

# 1. Drain throughput: 1,000 groups x 20 jobs already waiting, no-op jobs, one worker process.
for a in plain groups queue-per-group delay-gate; do
  run --scenario throughput --approach "$a" --groups 1000 --jobs 20 --concurrency 50
done

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

# 4. Skew: one shop with 20,000 jobs added first, then 999 shops with one job each; 10 ms jobs.
for a in plain groups groups-c4 queue-per-group delay-gate; do
  run --scenario skew --approach "$a" --groups 999 --hot 20000 --concurrency 20 --work 10 --timeout 180
done

# 5. Memory and keys with 100,000 jobs waiting across 10,000 groups, no workers.
for a in plain groups queue-per-group; do
  run --scenario memory --approach "$a" --groups 10000 --jobs 10
done
