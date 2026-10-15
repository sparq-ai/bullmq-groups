#!/usr/bin/env bash
# Runs the test suite against another BullMQ version in a throwaway copy of this repo.
# The package only uses BullMQ's public API, so the same tests should pass on 5.x and 6.x.
#   bash scripts/test-bullmq-v6.sh [version]   (default 6.3.15)
set -euo pipefail
VERSION=${1:-6.3.15}
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT
tar --exclude=node_modules --exclude=.git -cf - . | tar -xf - -C "$DIR"
cd "$DIR"
npm install --silent --no-audit --no-fund
npm install --silent --no-audit --no-fund "bullmq@$VERSION"
echo "bullmq $(node -p "require('bullmq/package.json').version")"
npm test
