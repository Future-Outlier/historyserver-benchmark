#!/usr/bin/env bash
# Formal Ray 2.56 History Server memory confirmation.
#
# The immutable memory matrix is derived from a fully validated CPU campaign:
# candidate = roundUp32Mi(1.25 * selected-CPU max lifetime memory.peak).
# Request equals limit, and all five fresh-pod arms must pass.  S3 is read-only.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/ray256_hs_cpu.sh"

main() {
  HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
  REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel)
  BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
  BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
  CPU_ROOT=${BENCH_HS_CPU_CAMPAIGN:-}
  SOURCE_ACCEPTED_DIR=${BENCH_HS_SOURCE_ACCEPTED_DIR:-}
  OUT=${BENCH_SWEEP_OUT:-$HS/test/benchmark/out/ray256-hs-memory-$(date +%Y%m%d-%H%M%S)}

  source "$SCRIPT_DIR/sweep_lib.sh"
  export KUBECONFIG=${BENCH_KUBECONFIG:-${KUBECONFIG:-$BENCH_SCRATCH/kubeconfig-bench}}
  export GOPATH=${BENCH_GOPATH:-$BENCH_SCRATCH/gopath}
  export GOMODCACHE=${BENCH_GOMODCACHE:-$BENCH_SCRATCH/gomodcache}
  export GOCACHE=${BENCH_GOCACHE:-$BENCH_SCRATCH/gocache}
  hs_build_env_unset

  [ -n "$CPU_ROOT" ] && [ -d "$CPU_ROOT" ] || {
    echo "PREFLIGHT-FAILED: BENCH_HS_CPU_CAMPAIGN must name a completed CPU campaign" >&2
    exit 1
  }
  [ -z "${BENCH_HS_SOURCE_REPORT:-}" ] \
    && [ -z "${BENCH_HS_SOURCE_PROVENANCE:-}" ] || {
    echo "PREFLIGHT-FAILED: use BENCH_HS_SOURCE_ACCEPTED_DIR, not arbitrary source report/provenance paths" >&2
    exit 1
  }
  [ -n "$SOURCE_ACCEPTED_DIR" ] && [ -d "$SOURCE_ACCEPTED_DIR" ] || {
    echo "PREFLIGHT-FAILED: BENCH_HS_SOURCE_ACCEPTED_DIR must name one accepted source bundle" >&2
    exit 1
  }
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" verify-completion \
    --accepted-dir "$SOURCE_ACCEPTED_DIR" >/dev/null
  SOURCE_REPORT="$SOURCE_ACCEPTED_DIR/source-report.json"
  SOURCE_PROVENANCE="$SOURCE_ACCEPTED_DIR/provenance-final.txt"
  SOURCE_COMPLETION="$SOURCE_ACCEPTED_DIR/source-completion.json"
  [ ! -e "$OUT" ] || {
    echo "PREFLIGHT-FAILED: refusing to reuse output directory: $OUT" >&2
    exit 1
  }
  python3 "$SCRIPT_DIR/validate_hs_sweep.py" \
    --expected-kind hs-cpu "$CPU_ROOT" >/dev/null
  require_same_provenance_value "CPU/source expected matrix binding" \
    "$(file_sha256 "$SOURCE_ACCEPTED_DIR/expected-hs-cpu-matrix.json")" \
    "$(file_sha256 "$CPU_ROOT/expected-matrix.json")"
  require_same_provenance_value "CPU/source completion binding" \
    "$(file_sha256 "$SOURCE_COMPLETION")" \
    "$(provenance_value "$CPU_ROOT/provenance-final.txt" source_completion_sha256)"

  mkdir -p "$BENCH_SCRATCH" "$GOPATH" "$GOMODCACHE" "$GOCACHE"
  require_kubeconfig_files
  require_formal_kind_target
  hs_assert_no_parallel_campaign
  require_no_active_host_kuberay_operator
  require_no_active_incluster_kuberay_operator
  require_no_reconcilable_kuberay_workloads
  require_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"
  HS_CAMPAIGN_LOCK_DIR="$BENCH_SCRATCH/hs-formal-campaign.lock"
  mkdir "$HS_CAMPAIGN_LOCK_DIR" 2>/dev/null || {
    echo "PREFLIGHT-FAILED: HS campaign lock already exists: $HS_CAMPAIGN_LOCK_DIR" >&2
    exit 1
  }
  trap 'rmdir "$HS_CAMPAIGN_LOCK_DIR" 2>/dev/null || true' EXIT
  mkdir "$OUT"

  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" memory \
    --output "$OUT/expected-matrix.json" \
    --cpu-campaign-root "$CPU_ROOT" \
    --kind-node "$BENCH_KIND_NODE"
  hs_load_matrix_source "$OUT/expected-matrix.json"

  local selection selected_cpu candidate_memory
  selection=$(python3 -c '
import json, sys
doc = json.load(open(sys.argv[1]))
e = doc["selectionEvidence"]
print(e["selectedCPU"], e["candidateMemoryQuantity"])
' "$OUT/expected-matrix.json")
  read -r selected_cpu candidate_memory <<<"$selection"
  [ -n "$selected_cpu" ] && [ -n "$candidate_memory" ] || {
    echo "PREFLIGHT-FAILED: memory matrix selection is empty" >&2
    exit 1
  }
  hs_capture_initial_provenance hs-memory "$SOURCE_REPORT" "$CPU_ROOT"

  local repeat name
  for repeat in 1 2 3 4 5; do
    name="memory-${candidate_memory}-r${repeat}"
    hs_run_arm "$name" "$selected_cpu" "$candidate_memory"
  done

  printf 'SWEEP-SUCCEEDED arms=5\n' | tee -a "$OUT/status.txt"
  hs_revalidate_provenance hs-memory "$SOURCE_REPORT" "$CPU_ROOT"
  echo "ALL-DONE out=$OUT"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
