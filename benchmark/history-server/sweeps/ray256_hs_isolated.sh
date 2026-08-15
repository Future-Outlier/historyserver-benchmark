#!/usr/bin/env bash
# Formal isolated-request History Server campaign. Each accepted source corpus
# (1k/5k/10k/50k) is run in a separate invocation, with five fresh pods.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# Reuse only the audited preflight/provenance/arm primitives; this file owns the
# matrix kind, fixed profile, and exact five-arm order.
source "$SCRIPT_DIR/ray256_hs_cpu.sh"

main() {
  HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
  REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel)
  BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
  BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
  SOURCE_ACCEPTED_DIR=${BENCH_HS_SOURCE_ACCEPTED_DIR:-}
  OUT=${BENCH_SWEEP_OUT:-$HS/test/benchmark/out/ray256-hs-isolated-$(date +%Y%m%d-%H%M%S)}
  source "$SCRIPT_DIR/sweep_lib.sh"
  export KUBECONFIG=${BENCH_KUBECONFIG:-${KUBECONFIG:-$BENCH_SCRATCH/kubeconfig-bench}}
  export GOPATH=${BENCH_GOPATH:-$BENCH_SCRATCH/gopath}
  export GOMODCACHE=${BENCH_GOMODCACHE:-$BENCH_SCRATCH/gomodcache}
  export GOCACHE=${BENCH_GOCACHE:-$BENCH_SCRATCH/gocache}
  hs_build_env_unset

  [ -n "$SOURCE_ACCEPTED_DIR" ] && [ -d "$SOURCE_ACCEPTED_DIR" ] || {
    echo "PREFLIGHT-FAILED: BENCH_HS_SOURCE_ACCEPTED_DIR must name one accepted source bundle" >&2; exit 1;
  }
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" verify-completion --accepted-dir "$SOURCE_ACCEPTED_DIR" >/dev/null
  SOURCE_REPORT="$SOURCE_ACCEPTED_DIR/source-report.json"
  SOURCE_PROVENANCE="$SOURCE_ACCEPTED_DIR/provenance-final.txt"
  SOURCE_COMPLETION="$SOURCE_ACCEPTED_DIR/source-completion.json"
  [ ! -e "$OUT" ] || { echo "PREFLIGHT-FAILED: refusing to reuse output directory: $OUT" >&2; exit 1; }
  mkdir -p "$BENCH_SCRATCH" "$GOPATH" "$GOMODCACHE" "$GOCACHE"
  require_kubeconfig_files
  require_formal_kind_target
  hs_assert_no_parallel_campaign
  require_no_active_host_kuberay_operator
  require_no_active_incluster_kuberay_operator
  require_no_reconcilable_kuberay_workloads
  require_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"
  HS_CAMPAIGN_LOCK_DIR="$BENCH_SCRATCH/hs-formal-campaign.lock"
  mkdir "$HS_CAMPAIGN_LOCK_DIR" 2>/dev/null || { echo "PREFLIGHT-FAILED: HS campaign lock exists" >&2; exit 1; }
  trap 'rmdir "$HS_CAMPAIGN_LOCK_DIR" 2>/dev/null || true' EXIT
  mkdir "$OUT"

  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" isolated --output "$OUT/expected-matrix.json" \
    --source-report "$SOURCE_REPORT" --source-provenance "$SOURCE_PROVENANCE" --kind-node "$BENCH_KIND_NODE"
  hs_load_matrix_source "$OUT/expected-matrix.json"
  case "$HS_SOURCE_TASK_COUNT" in 1000|5000|10000|50000) ;; *) echo "PREFLIGHT-FAILED: unsupported N" >&2; exit 1;; esac
  hs_capture_initial_provenance hs-isolated-request "$SOURCE_REPORT"
  local repeat
  for repeat in 1 2 3 4 5; do
    hs_run_arm "isolated-r${repeat}" 1 1Gi 2 12Gi isolated-request-v1 10s 8s \
      'GOMAXPROCS=2,GODEBUG=gctrace=1'
  done
  printf 'SWEEP-SUCCEEDED arms=5\n' | tee -a "$OUT/status.txt"
  hs_revalidate_provenance hs-isolated-request "$SOURCE_REPORT"
  echo "ALL-DONE out=$OUT"
}

main "$@"
