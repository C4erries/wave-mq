#!/usr/bin/env bash

set -euo pipefail

# Run the Raft-backed two-broker replication restart test.
# This script exercises leader-only produce/fetch behavior, Raft metadata recovery,
# and replication continuity across controller/broker restarts.

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

go test ./internal/broker -run TestRaftReplicationSurvivesRestarts -count=1 "$@"
