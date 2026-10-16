#!/usr/bin/env bash
# Runs the test suite against another BullMQ version in a throwaway copy of this repo.
# The same tests should pass on every BullMQ in the peer range (5.40.0 up to 6.x).
#   bash scripts/test-bullmq.sh [version]   (default 6.3.15)
set -euo pipefail
VERSION=${1:-6.3.15}
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT
# Copy what git would see (tracked and new files, as they are on disk), not node_modules, dist or
# other ignored files.
git ls-files -co --exclude-standard | while IFS= read -r f; do if [ -e "$f" ]; then printf '%s\0' "$f"; fi; done \
  | tar --null -T - -cf - | tar -xf - -C "$DIR"
cd "$DIR"
npm install --silent --no-audit --no-fund
# @nestjs/bullmq accepts BullMQ 6 from 11.0.5 on.
NEST_BULLMQ=$([[ "$VERSION" == 6* ]] && echo "@nestjs/bullmq@11.0.5" || true)
npm install --silent --no-audit --no-fund "bullmq@$VERSION" $NEST_BULLMQ
echo "bullmq $(node -p "require('bullmq/package.json').version")"
npm test
