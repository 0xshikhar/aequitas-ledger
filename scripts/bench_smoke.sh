#!/usr/bin/env bash
# Bench smoke (P5.1): catches catastrophic throughput regressions in CI.
#
# The floors are deliberately generous (3-5x headroom over measured values on
# Apple M4 Pro: unary ~4.7ms/op, batch-8192 ~30ms/op) so shared CI runners do
# not flake. This gate exists to catch orders-of-magnitude collapses (e.g. an
# accidental extra fsync per transfer), not small percentage regressions —
# those need dedicated hardware and benchstat baselines (S2.4).
set -euo pipefail

cd "$(dirname "$0")/.."

check() {
  local name="$1" floor_ns="$2" ns
  # No early exit inside awk: closing the pipe early SIGPIPEs go test, which
  # pipefail treats as a failure. Consume the whole stream, take line 1.
  ns=$(go test -run '^$' -bench "^${name}\$" -benchtime 50x -timeout 10m \
      ./tests/integration 2>/dev/null \
      | awk '/^Benchmark.*ns\/op/ {print $3}' \
      | sed -n '1p')
  if [ -z "$ns" ]; then
    echo "FAIL: no benchmark output for ${name}"
    exit 1
  fi
  if [ "$ns" -gt "$floor_ns" ]; then
    echo "FAIL: ${name} ns/op=${ns} exceeds smoke floor ${floor_ns}"
    exit 1
  fi
  echo "ok: ${name} ns/op=${ns} (floor ${floor_ns})"
}

check BenchmarkGRPCUnaryCreateTransfer 25000000
check BenchmarkGRPCBatchCreateTransfers8192 150000000
