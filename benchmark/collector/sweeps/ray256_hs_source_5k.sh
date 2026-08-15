#!/usr/bin/env bash
# Generate one immutable Ray 2.56 source session for formal HS campaigns.
#
# The RayJob owns its RayCluster and uses shutdownAfterJob=true with a 30-second
# TTL.  The Python driver contains no post-job sleep.  S3 cleanup is disabled so
# a successful source remains immutable and reusable by CPU and memory campaigns.
# BENCH_HS_SOURCE_TASK_COUNT selects 1k/5k/10k/50k and defaults to 5k for
# compatibility with the original script name and invocation.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel)
BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
RAY_IMAGE=rayproject/ray:2.56.0
BENCHMARK_S3_BUCKET=ray-historyserver-benchmark
HS_S3_LOCAL_PORT=19003
HS_API_LOCAL_PORT=30080
SOURCE_TASK_COUNT=${BENCH_HS_SOURCE_TASK_COUNT:-5000}
REJECTED_N50_BASELINE_REPORT_SHA256=7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264
REJECTED_N50_WAVE100_R1_REPORT_SHA256=2c191e0133dce4a95a19fe7835daf0628784276fd421de608ed8fd796d3915ef
REJECTED_N50_WAVE100_R2_REPORT_SHA256=01274cdc416f6a6709706389042e5aa8e6ab382677b1886b9bf2b8459d47e677
case "$SOURCE_TASK_COUNT" in
  1000|5000|10000)
    SOURCE_WAVE_SIZE=2000
    SOURCE_TARGET_TASK_RATE=0
    SOURCE_PACING_VARIANT=baseline-wave2000-v1
    SOURCE_REJECTED_BASELINE_REPORT_SHA256=none
    SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S=none
    SOURCE_COLLECTOR_CPU_REQUEST=""
    SOURCE_COLLECTOR_CPU_LIMIT=""
    SOURCE_CAMPAIGN_NAME="ray256-hs-source-n${SOURCE_TASK_COUNT}"
    ;;
  50000)
    # This is one predeclared recovery protocol, not a numeric knob to search
    # until a stochastic source happens to pass. Three prior Ray 2.56 runs at
    # 500 task/s and wave=2000 were complete. At that rate the head aggregator's
    # sequential HTTP publisher does not accumulate the queue observed in the
    # rejected unpaced wave=100 attempts. Exact post-run event gates remain the
    # authority: missing even one benchmark attempt rejects the source.
    SOURCE_WAVE_SIZE=2000
    SOURCE_TARGET_TASK_RATE=500
    SOURCE_PACING_VARIANT=single-driver-rate500-wave2000-v2
    SOURCE_REJECTED_BASELINE_REPORT_SHA256=$REJECTED_N50_BASELINE_REPORT_SHA256
    SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S="${REJECTED_N50_WAVE100_R1_REPORT_SHA256},${REJECTED_N50_WAVE100_R2_REPORT_SHA256}"
    SOURCE_COLLECTOR_CPU_REQUEST=100m
    SOURCE_COLLECTOR_CPU_LIMIT=2
    SOURCE_CAMPAIGN_NAME="ray256-hs-source-n50000-rate500-v2"
    ;;
  *)
    echo "PREFLIGHT-FAILED: BENCH_HS_SOURCE_TASK_COUNT=$SOURCE_TASK_COUNT, want one of 1000,5000,10000,50000" >&2
    exit 1
    ;;
esac
OUT=${BENCH_SWEEP_OUT:-$HS/test/benchmark/out/${SOURCE_CAMPAIGN_NAME}-$(date +%Y%m%d-%H%M%S)}
OPERATOR_BIN=${BENCH_OPERATOR_BIN:-$OUT/kuberay-operator}
SOURCE_RUNNER_LOCK_DIR=""
SOURCE_RUNNER_LOCK_OWNER_DIR=""
SOURCE_RUNNER_LOCK_TOKEN=""
SOURCE_RUNNER_SHELL_PID=""
SOURCE_RUNNER_SUPERVISOR_PID=""
SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
SOURCE_RUNNER_OPERATOR_EXIT_FILE=""
SOURCE_PROVENANCE_INITIAL_FILE=""

source "$SCRIPT_DIR/sweep_lib.sh"

export KUBECONFIG=${BENCH_KUBECONFIG:-${KUBECONFIG:-$BENCH_SCRATCH/kubeconfig-bench}}
export GOPATH=${BENCH_GOPATH:-$BENCH_SCRATCH/gopath}
export GOMODCACHE=${BENCH_GOMODCACHE:-$BENCH_SCRATCH/gomodcache}
export GOCACHE=${BENCH_GOCACHE:-$BENCH_SCRATCH/gocache}

BENCH_ENV_UNSET=(-u __KUBERAY_BENCH_ENV_SENTINEL__)
while IFS= read -r name; do
  case "$name" in
    BENCH_*) BENCH_ENV_UNSET+=(-u "$name") ;;
  esac
done < <(compgen -e)

source_runner_pid_is_active_job() {
  local wanted_pid="$1"
  local active_pid
  for active_pid in $(jobs -pr); do
    [ "$active_pid" = "$wanted_pid" ] && return 0
  done
  return 1
}

source_runner_process_identity() {
  local pid="$1"
  /usr/sbin/lsof -nP -a -p "$pid" -d cwd,txt -F pcn 2>/dev/null \
    | LC_ALL=C sort
}

source_runner_acquire_lock() {
  local lock_dir="$1"
  local token owner_dir
  SOURCE_RUNNER_LOCK_DIR="$lock_dir"
  SOURCE_RUNNER_LOCK_OWNER_DIR=""
  SOURCE_RUNNER_LOCK_TOKEN=""
  mkdir "$lock_dir" 2>/dev/null || {
    echo "PREFLIGHT-FAILED: HS campaign lock already exists: $lock_dir" >&2
    return 1
  }
  token=$(python3 -c 'import secrets; print(secrets.token_hex(32))') || {
    rmdir "$lock_dir" 2>/dev/null || true
    return 1
  }
  owner_dir="$lock_dir/.owner-$token"
  mkdir "$owner_dir" 2>/dev/null || {
    rmdir "$lock_dir" 2>/dev/null || true
    return 1
  }
  SOURCE_RUNNER_LOCK_TOKEN="$token"
  SOURCE_RUNNER_LOCK_OWNER_DIR="$owner_dir"
}

source_runner_supervise_operator() {
  local exit_file="$1"
  local log_file="$2"
  shift 2
  local operator_pid=""
  local hold_pid=""
  local operator_rc

  source_runner_stop_supervised_processes() {
    trap - TERM INT
    if [ -n "$operator_pid" ] && source_runner_pid_is_active_job "$operator_pid"; then
      kill "$operator_pid" 2>/dev/null || true
      wait "$operator_pid" 2>/dev/null || true
    fi
    if [ -n "$hold_pid" ] && source_runner_pid_is_active_job "$hold_pid"; then
      kill "$hold_pid" 2>/dev/null || true
      wait "$hold_pid" 2>/dev/null || true
    fi
    exit 0
  }
  trap source_runner_stop_supervised_processes TERM INT

  "$@" > "$log_file" 2>&1 &
  operator_pid=$!
  set +e
  wait "$operator_pid"
  operator_rc=$?
  set -e
  operator_pid=""
  printf '%s\n' "$operator_rc" > "$exit_file"

  while true; do
    sleep 3600 &
    hold_pid=$!
    wait "$hold_pid" 2>/dev/null || true
    hold_pid=""
  done
}

source_runner_require_operator_running() {
  local exit_file="$1"
  local failure_prefix="$2"
  [ ! -e "$exit_file" ] || {
    echo "$failure_prefix-FAILED: benchmark operator exited rc=$(<"$exit_file")" >&2
    return 1
  }
}

source_runner_require_supervisor_healthy() {
  local failure_prefix="$1"
  local current_identity
  [ -n "${SOURCE_RUNNER_SUPERVISOR_PID:-}" ] \
    && [ -n "${SOURCE_RUNNER_SUPERVISOR_IDENTITY:-}" ] \
    && [ -n "${SOURCE_RUNNER_SHELL_PID:-}" ] || {
    echo "$failure_prefix-FAILED: operator supervisor identity is incomplete" >&2
    return 1
  }
  source_runner_pid_is_active_job "$SOURCE_RUNNER_SUPERVISOR_PID" || {
    echo "$failure_prefix-FAILED: operator supervisor is not an active shell job" >&2
    return 1
  }
  current_identity=$(source_runner_process_identity \
    "$SOURCE_RUNNER_SUPERVISOR_PID" 2>/dev/null || true)
  [ "$current_identity" = "$SOURCE_RUNNER_SUPERVISOR_IDENTITY" ] \
    && [ "$SOURCE_RUNNER_SHELL_PID" = "$$" ] || {
    echo "$failure_prefix-FAILED: operator supervisor identity changed" >&2
    return 1
  }
  source_runner_require_operator_running \
    "$SOURCE_RUNNER_OPERATOR_EXIT_FILE" "$failure_prefix"
}

source_runner_stop_supervisor() {
  local current_identity wait_rc
  [ -n "${SOURCE_RUNNER_SUPERVISOR_PID:-}" ] || return 0
  if ! source_runner_pid_is_active_job "$SOURCE_RUNNER_SUPERVISOR_PID"; then
    wait "$SOURCE_RUNNER_SUPERVISOR_PID" 2>/dev/null || true
    SOURCE_RUNNER_SUPERVISOR_PID=""
    SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
    echo "CLEANUP-FAILED: operator supervisor was not an active shell job" >&2
    return 1
  fi
  current_identity=$(source_runner_process_identity \
    "$SOURCE_RUNNER_SUPERVISOR_PID" 2>/dev/null || true)
  if [ -z "${SOURCE_RUNNER_SUPERVISOR_IDENTITY:-}" ] \
    || [ "$current_identity" != "$SOURCE_RUNNER_SUPERVISOR_IDENTITY" ] \
    || [ "${SOURCE_RUNNER_SHELL_PID:-}" != "$$" ]; then
    echo "CLEANUP-FAILED: supervisor identity changed; refusing to signal PID $SOURCE_RUNNER_SUPERVISOR_PID" >&2
    return 1
  fi
  kill "$SOURCE_RUNNER_SUPERVISOR_PID" 2>/dev/null || {
    echo "CLEANUP-FAILED: cannot signal owned supervisor PID $SOURCE_RUNNER_SUPERVISOR_PID" >&2
    return 1
  }
  wait_rc=0
  wait "$SOURCE_RUNNER_SUPERVISOR_PID" 2>/dev/null || wait_rc=$?
  SOURCE_RUNNER_SUPERVISOR_PID=""
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
  [ "$wait_rc" -eq 0 ] || {
    echo "CLEANUP-FAILED: operator supervisor wait rc=$wait_rc" >&2
    return 1
  }
}

source_runner_release_lock() {
  [ -n "${SOURCE_RUNNER_LOCK_TOKEN:-}" ] || return 0
  if [ -n "${SOURCE_RUNNER_LOCK_TOKEN:-}" ] \
    && [ "${SOURCE_RUNNER_LOCK_OWNER_DIR:-}" = \
      "${SOURCE_RUNNER_LOCK_DIR:-}/.owner-${SOURCE_RUNNER_LOCK_TOKEN:-}" ] \
    && rmdir "$SOURCE_RUNNER_LOCK_OWNER_DIR" 2>/dev/null; then
    if rmdir "$SOURCE_RUNNER_LOCK_DIR" 2>/dev/null; then
      SOURCE_RUNNER_LOCK_DIR=""
      SOURCE_RUNNER_LOCK_OWNER_DIR=""
      SOURCE_RUNNER_LOCK_TOKEN=""
      return 0
    fi
  fi
  echo "CLEANUP-FAILED: campaign lock ownership changed; refusing to remove foreign lock" >&2
  return 1
}

source_runner_release_resources() {
  local cleanup_failed=0
  source_runner_stop_supervisor || cleanup_failed=1
  source_runner_release_lock || cleanup_failed=1
  return "$cleanup_failed"
}

source_runner_cleanup() {
  local exit_code=$?
  local cleanup_failed=0
  trap - EXIT
  source_runner_release_resources || cleanup_failed=1
  if [ "$cleanup_failed" -ne 0 ] && [ "$exit_code" -eq 0 ]; then
    exit_code=70
  fi
  exit "$exit_code"
}

source_runtime_values() {
  local collector_requested collector_tag ray_meta collector_meta
  local ray_id ray_digests ray_version ray_commit unused_source
  local collector_id collector_digests unused_version unused_commit collector_source
  collector_requested=$(awk '$1 == "image:" && $2 ~ /^collector:/ { print $2; exit }' \
    "$HS/config/raycluster.yaml")
  [ -n "$collector_requested" ] || {
    echo "PROVENANCE-FAILED: Collector requested image is empty" >&2
    return 1
  }
  collector_tag=${collector_requested##*:}
  ray_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" rayproject/ray 2.56.0) || return 1
  collector_meta=$(runtime_image_metadata "$BENCH_KIND_NODE" collector "$collector_tag") || return 1
  IFS=$'\t' read -r ray_id ray_digests ray_version ray_commit unused_source <<<"$ray_meta"
  IFS=$'\t' read -r collector_id collector_digests unused_version unused_commit collector_source <<<"$collector_meta"
  is_full_image_id "$ray_id" && is_full_image_id "$collector_id" \
    && is_full_sha256 "$collector_source" || {
      echo "PROVENANCE-FAILED: runtime image identity is incomplete" >&2
      return 1
    }
  [ "$ray_version" = "2.56.0" ] || {
    echo "PROVENANCE-FAILED: Ray runtime version=$ray_version, want 2.56.0" >&2
    return 1
  }
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$ray_id" "$ray_digests" "$ray_version" "$ray_commit" \
    "$collector_requested" "$collector_id" "$collector_source"
}

capture_source_provenance() {
  local runtime ray_id ray_digests ray_version ray_commit collector_requested collector_id collector_source
  local collector_build_source
  runtime=$(source_runtime_values) || {
    echo "PROVENANCE-FAILED: cannot resolve Ray/Collector runtime images" >&2
    return 1
  }
  IFS=$'\t' read -r ray_id ray_digests ray_version ray_commit collector_requested collector_id collector_source <<<"$runtime"
  collector_build_source=$(image_build_source_sha256 collector)
  require_same_provenance_value "Collector image/source binding" \
    "$collector_build_source" "$collector_source"
  {
    printf 'campaign_kind=hs-source\n'
    printf 'campaign_name=%s\n' "$SOURCE_CAMPAIGN_NAME"
    printf 'source_task_count=%s\n' "$SOURCE_TASK_COUNT"
    printf 'source_wave_size=%s\n' "$SOURCE_WAVE_SIZE"
    printf 'source_drivers=1\n'
    printf 'source_target_task_rate=%s\n' "$SOURCE_TARGET_TASK_RATE"
    printf 'source_rayjob_backoff_limit=0\n'
    printf 'source_submitter_backoff_limit=0\n'
    printf 'source_pacing_variant=%s\n' "$SOURCE_PACING_VARIANT"
    printf 'source_rejected_baseline_report_sha256=%s\n' \
      "$SOURCE_REJECTED_BASELINE_REPORT_SHA256"
    printf 'source_rejected_attempt_report_sha256s=%s\n' \
      "$SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S"
    printf 'source_collector_cpu_request=%s\n' \
      "${SOURCE_COLLECTOR_CPU_REQUEST:-none}"
    printf 'source_collector_cpu_limit=%s\n' \
      "${SOURCE_COLLECTOR_CPU_LIMIT:-none}"
    printf 'source_bucket=%s\n' "$BENCHMARK_S3_BUCKET"
    printf 'repo_head=%s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD)"
    printf 'tracked_diff_sha256=%s\n' "$(tracked_workspace_diff_sha256 "$REPO_ROOT")"
    printf 'benchmark_source_sha256=%s\n' "$(benchmark_tree_sha256 "$HS/test/benchmark")"
    printf 'raycluster_manifest_sha256=%s\n' "$(file_sha256 "$HS/config/raycluster.yaml")"
    printf 'planned_config_sha256=%s\n' "$(file_sha256 "$OUT/planned-config.json")"
    printf 'source_lineage_sha256=%s\n' "$(file_sha256 "$OUT/lineage.json")"
    printf 'operator_sha256=%s\n' "$(file_sha256 "$OPERATOR_BIN")"
    printf 'ray_image_requested=%s\n' "$RAY_IMAGE"
    printf 'ray_runtime_id=%s\n' "$ray_id"
    printf 'ray_runtime_repo_digests=%s\n' "$ray_digests"
    printf 'ray_runtime_version=%s\n' "$ray_version"
    printf 'ray_runtime_commit=%s\n' "$ray_commit"
    printf 'collector_image_requested=%s\n' "$collector_requested"
    printf 'collector_build_source_sha256=%s\n' "$collector_build_source"
    printf 'collector_runtime_id=%s\n' "$collector_id"
    printf 'collector_runtime_build_source_sha256=%s\n' "$collector_source"
  } > "$OUT/provenance.txt"
}

require_source_provenance_unchanged() {
  local key="$1"
  local current="$2"
  local provenance_file="${SOURCE_PROVENANCE_INITIAL_FILE:-$OUT/provenance.txt}"
  require_same_provenance_value "$key" \
    "$(provenance_value "$provenance_file" "$key")" "$current"
}

source_contract_runtime_values() {
  local source_contract="$1"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" source-runtime-values \
    --source-contract "$source_contract"
}

revalidate_source_provenance() {
  local source_report="$1"
  local bundle_dir="$2"
  local source_contract="$bundle_dir/source-contract.json"
  local final_provenance="$bundle_dir/provenance-final.txt"
  SOURCE_PROVENANCE_INITIAL_FILE="$bundle_dir/provenance.txt"
  local runtime ray_id unused_digests unused_version unused_commit unused_requested collector_id collector_source
  require_source_provenance_unchanged repo_head "$(git -C "$REPO_ROOT" rev-parse HEAD)"
  require_source_provenance_unchanged tracked_diff_sha256 "$(tracked_workspace_diff_sha256 "$REPO_ROOT")"
  require_source_provenance_unchanged benchmark_source_sha256 "$(benchmark_tree_sha256 "$HS/test/benchmark")"
  require_source_provenance_unchanged raycluster_manifest_sha256 "$(file_sha256 "$HS/config/raycluster.yaml")"
  require_source_provenance_unchanged planned_config_sha256 \
    "$(file_sha256 "$bundle_dir/planned-config.json")"
  require_source_provenance_unchanged source_lineage_sha256 \
    "$(file_sha256 "$bundle_dir/lineage.json")"
  require_source_provenance_unchanged operator_sha256 "$(file_sha256 "$OPERATOR_BIN")"
  require_source_provenance_unchanged collector_build_source_sha256 "$(image_build_source_sha256 collector)"
  runtime=$(source_runtime_values)
  IFS=$'\t' read -r ray_id unused_digests unused_version unused_commit unused_requested collector_id collector_source <<<"$runtime"
  require_source_provenance_unchanged ray_runtime_id "$ray_id"
  require_source_provenance_unchanged collector_runtime_id "$collector_id"
  require_source_provenance_unchanged collector_runtime_build_source_sha256 "$collector_source"

  local source_values source_kind source_spec source_task_count source_bucket
  local source_wave_size source_drivers source_target_task_rate source_pacing_variant
  local source_driver_tasks source_driver_wall_sec source_driver_rate_tps
  local source_lineage_sha256 source_rejected_baseline_report_sha256
  local source_rayjob_backoff_limit source_submitter_backoff_limit
  local source_contract_end
  source_values=$(source_contract_runtime_values "$source_contract") || return 1
  IFS=$'\t' read -r source_kind source_spec source_task_count source_bucket \
    source_wave_size source_drivers source_target_task_rate source_pacing_variant \
    source_driver_tasks source_driver_wall_sec source_driver_rate_tps \
    source_lineage_sha256 source_rejected_baseline_report_sha256 \
    source_rayjob_backoff_limit source_submitter_backoff_limit \
    source_contract_end \
    <<<"$source_values"
  [ "$source_contract_end" = SOURCE-CONTRACT-END ] || {
    echo "PROVENANCE-FAILED: source contract runtime field arity differs" >&2
    return 1
  }
  [ "$source_kind" = 'hs-source' ] || {
    echo "PROVENANCE-FAILED: source contract kind=$source_kind, want hs-source" >&2
    return 1
  }
  require_source_provenance_unchanged campaign_name "$SOURCE_CAMPAIGN_NAME"
  require_source_provenance_unchanged source_task_count "$source_task_count"
  require_source_provenance_unchanged source_wave_size "$source_wave_size"
  require_source_provenance_unchanged source_drivers "$source_drivers"
  require_source_provenance_unchanged source_target_task_rate \
    "$source_target_task_rate"
  require_source_provenance_unchanged source_pacing_variant "$source_pacing_variant"
  require_source_provenance_unchanged source_lineage_sha256 \
    "$source_lineage_sha256"
  require_source_provenance_unchanged source_rejected_baseline_report_sha256 \
    "$source_rejected_baseline_report_sha256"
  require_source_provenance_unchanged source_rejected_attempt_report_sha256s \
    "$SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S"
  require_source_provenance_unchanged source_collector_cpu_request \
    "${SOURCE_COLLECTOR_CPU_REQUEST:-none}"
  require_source_provenance_unchanged source_collector_cpu_limit \
    "${SOURCE_COLLECTOR_CPU_LIMIT:-none}"
  require_source_provenance_unchanged source_rayjob_backoff_limit \
    "$source_rayjob_backoff_limit"
  require_source_provenance_unchanged source_submitter_backoff_limit \
    "$source_submitter_backoff_limit"
  require_source_provenance_unchanged source_bucket "$source_bucket"
  require_same_provenance_value "source bucket/dedicated benchmark bucket" \
    "$BENCHMARK_S3_BUCKET" "$source_bucket"
  require_same_provenance_value "source task count/selected task count" \
    "$SOURCE_TASK_COUNT" "$source_task_count"
  require_same_provenance_value "source wave size/selected wave size" \
    "$SOURCE_WAVE_SIZE" "$source_wave_size"
  require_same_provenance_value "source driver count/single-driver contract" \
    1 "$source_drivers"
  require_same_provenance_value "source target rate/selected protocol" \
    "$SOURCE_TARGET_TASK_RATE" "$source_target_task_rate"
  require_same_provenance_value "source pacing variant/selected pacing variant" \
    "$SOURCE_PACING_VARIANT" "$source_pacing_variant"
  require_same_provenance_value "source rejected baseline lineage" \
    "$SOURCE_REJECTED_BASELINE_REPORT_SHA256" \
    "$source_rejected_baseline_report_sha256"
  require_same_provenance_value "source RayJob retry limit" \
    0 "$source_rayjob_backoff_limit"
  require_same_provenance_value "source submitter retry limit" \
    0 "$source_submitter_backoff_limit"
  {
    cat "$SOURCE_PROVENANCE_INITIAL_FILE"
    printf 'revalidation_status=valid\n'
    printf 'source_report_sha256=%s\n' "$(file_sha256 "$source_report")"
    printf 'source_initial_provenance_sha256=%s\n' \
      "$(file_sha256 "$SOURCE_PROVENANCE_INITIAL_FILE")"
    printf 'source_contract_sha256=%s\n' "$(file_sha256 "$source_contract")"
    printf 'source_session=%s\n' "$source_spec"
    printf 'source_driver_tasks=%s\n' "$source_driver_tasks"
    printf 'source_driver_wall_sec=%s\n' "$source_driver_wall_sec"
    printf 'source_driver_rate_tps=%s\n' "$source_driver_rate_tps"
  } > "$final_provenance"
}

write_source_lineage() {
  local output="$1"
  python3 - "$output" "$SOURCE_TASK_COUNT" "$SOURCE_WAVE_SIZE" \
    "$SOURCE_TARGET_TASK_RATE" "$SOURCE_PACING_VARIANT" \
    "$SOURCE_REJECTED_BASELINE_REPORT_SHA256" \
    "$SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S" \
    "${SOURCE_COLLECTOR_CPU_REQUEST:-none}" \
    "${SOURCE_COLLECTOR_CPU_LIMIT:-none}" <<'PY'
import json
import sys

rejected_sha = None if sys.argv[6] == "none" else sys.argv[6]
rejected_attempts = (
    [] if sys.argv[7] == "none" else sys.argv[7].split(",")
)
document = {
    "schemaVersion": 1,
    "kind": "hs-source-generation-lineage",
    "current": {
        "taskCount": int(sys.argv[2]),
        "waveSize": int(sys.argv[3]),
        "drivers": 1,
        "targetTaskRate": int(sys.argv[4]),
        "pacingVariant": sys.argv[5],
        "collectorCPURequest": None if sys.argv[8] == "none" else sys.argv[8],
        "collectorCPULimit": None if sys.argv[9] == "none" else sys.argv[9],
        "rayJobBackoffLimit": 0,
        "submitterBackoffLimit": 0,
    },
    "rejectedPredecessor": None if rejected_sha is None else {
        "taskCount": 50000,
        "waveSize": 2000,
        "reportSHA256": rejected_sha,
        "verdict": "rejected-incomplete-source",
    },
    "rejectedAttempts": [
        {
            "taskCount": 50000,
            "waveSize": 100,
            "targetTaskRate": 0,
            "reportSHA256": sha,
            "verdict": "rejected-incomplete-source",
        }
        for sha in rejected_attempts
    ],
}
with open(sys.argv[1], "x", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
PY
}

write_source_planned_config() {
  local output="$1"
  python3 - "$output" "$SOURCE_TASK_COUNT" "$BENCHMARK_S3_BUCKET" \
    "$SOURCE_WAVE_SIZE" "$SOURCE_TARGET_TASK_RATE" "$SOURCE_PACING_VARIANT" \
    "$SOURCE_REJECTED_BASELINE_REPORT_SHA256" \
    "$SOURCE_REJECTED_ATTEMPT_REPORT_SHA256S" \
    "${SOURCE_COLLECTOR_CPU_REQUEST:-none}" \
    "${SOURCE_COLLECTOR_CPU_LIMIT:-none}" <<'PY'
import json
import sys

document = {
    "TaskCount": int(sys.argv[2]),
    "WaveSize": int(sys.argv[4]),
    "PacingVariant": sys.argv[6],
    "RejectedBaselineReportSHA256": (
        None if sys.argv[7] == "none" else sys.argv[7]
    ),
    "RejectedAttemptReportSHA256s": (
        [] if sys.argv[8] == "none" else sys.argv[8].split(",")
    ),
    "LineageFile": "lineage.json",
    "TaskNumCPUs": "0.5",
    "RayImage": "rayproject/ray:2.56.0",
    "Compression": True,
    "S3Bucket": sys.argv[3],
    "Drivers": 1,
    "TargetTaskRate": int(sys.argv[5]),
    "CollectorCPURequest": None if sys.argv[9] == "none" else sys.argv[9],
    "CollectorCPULimit": None if sys.argv[10] == "none" else sys.argv[10],
    "DrainSleepSec": 0,
    "ShutdownAfterJob": True,
    "JobTTLSeconds": 30,
    "RayJobBackoffLimit": 0,
    "SubmitterBackoffLimit": 0,
    "SkipHistoryServer": True,
    "SkipCleanup": True,
    "S3LocalPort": 19003,
    "HistoryServerLocalPort": 30080,
    "ExecutionNamespaceIdentityFile": "execution-namespace.json",
    "WaitForNamespaceDeletion": True,
}
with open(sys.argv[1], "x", encoding="utf-8") as stream:
    json.dump(document, stream, indent=2, sort_keys=True)
    stream.write("\n")
PY
}

main() {
  [ ! -e "$OUT" ] || {
    echo "PREFLIGHT-FAILED: refusing to reuse output directory: $OUT" >&2
    exit 1
  }
  require_kubeconfig_files
  require_formal_kind_target
  hs_assert_no_parallel_campaign
  require_no_active_host_kuberay_operator
  require_no_active_incluster_kuberay_operator
  require_no_reconcilable_kuberay_workloads
  require_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"
  require_local_tcp_port_free 8083
  require_local_tcp_port_free 8085
  mkdir -p "$OUT" "$BENCH_SCRATCH" "$GOPATH" "$GOMODCACHE" "$GOCACHE"
  SOURCE_RUNNER_SHELL_PID=""
  SOURCE_RUNNER_SUPERVISOR_PID=""
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=""
  SOURCE_RUNNER_OPERATOR_EXIT_FILE="$OUT/operator-exit-status.txt"
  SOURCE_RUNNER_LOCK_DIR="$BENCH_SCRATCH/hs-formal-campaign.lock"
  trap source_runner_cleanup EXIT
  source_runner_acquire_lock "$SOURCE_RUNNER_LOCK_DIR"

  if [ -z "${BENCH_OPERATOR_BIN:-}" ]; then
    (cd "$REPO_ROOT/ray-operator" && go build -o "$OPERATOR_BIN" .)
  elif [ ! -x "$OPERATOR_BIN" ]; then
    echo "PREFLIGHT-FAILED: BENCH_OPERATOR_BIN is not executable: $OPERATOR_BIN" >&2
    exit 1
  fi

  write_source_lineage "$OUT/lineage.json"
  write_source_planned_config "$OUT/planned-config.json"
  capture_source_provenance

  source_runner_supervise_operator \
    "$SOURCE_RUNNER_OPERATOR_EXIT_FILE" "$OUT/operator.log" \
    "$OPERATOR_BIN" --metrics-addr=:8083 --health-probe-bind-address=:8085 \
    --enable-leader-election=false --use-kubernetes-proxy &
  SOURCE_RUNNER_SUPERVISOR_PID=$!
  SOURCE_RUNNER_SHELL_PID=$$
  SOURCE_RUNNER_SUPERVISOR_IDENTITY=$(source_runner_process_identity \
    "$SOURCE_RUNNER_SUPERVISOR_PID") || {
    echo "PREFLIGHT-FAILED: cannot capture operator supervisor identity" >&2
    exit 1
  }
  sleep 6
  source_runner_require_supervisor_healthy PREFLIGHT

  mkdir "$OUT/run"
  require_no_active_incluster_kuberay_operator
  require_no_reconcilable_kuberay_workloads
  require_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"
  local run_rc cleanup_rc port_cleanup_rc
  set +e
  (
    cd "$HS"
    env "${BENCH_ENV_UNSET[@]}" \
      BENCH_RUN=1 \
      BENCH_KIND_NODE="$BENCH_KIND_NODE" \
      BENCH_OUT_DIR="$OUT/run" \
      BENCH_RAY_IMAGE="$RAY_IMAGE" \
      BENCH_S3_LOCAL_PORT="$HS_S3_LOCAL_PORT" \
      BENCH_EXECUTION_IDENTITY_FILE="$OUT/run/execution-namespace.json" \
      BENCH_TASK_COUNT="$SOURCE_TASK_COUNT" \
      BENCH_WAVE_SIZE="$SOURCE_WAVE_SIZE" \
      BENCH_TASK_NUM_CPUS=0.5 \
      BENCH_COMPRESSION=true \
      BENCH_DRIVERS=1 \
      BENCH_TARGET_TASK_RATE="$SOURCE_TARGET_TASK_RATE" \
      BENCH_DRIVER_DRAIN_SLEEP=0 \
      BENCH_SHUTDOWN_AFTER_JOB=true \
      BENCH_JOB_TTL_SECONDS=30 \
      BENCH_COLLECTOR_CPU_REQUEST="$SOURCE_COLLECTOR_CPU_REQUEST" \
      BENCH_COLLECTOR_CPU_LIMIT="$SOURCE_COLLECTOR_CPU_LIMIT" \
      BENCH_SKIP_HISTORY_SERVER=true \
      BENCH_SKIP_CLEANUP=1 \
      BENCH_WARM_ITERATIONS=3 \
      BENCH_HS_CPU_REQUEST=4 \
      BENCH_HS_CPU_LIMIT=4 \
      BENCH_HS_ARGS=--session-process-timeout=30m \
      BENCH_JOB_TIMEOUT=30m \
      go test -vet=off ./test/benchmark -run 'TestHistoryServerBenchmark$' \
        -count=1 -v -timeout 45m > "$OUT/source.log" 2>&1
  )
  run_rc=$?
  set -e
  cleanup_rc=0
  port_cleanup_rc=0
  wait_for_execution_namespace_deletion "$OUT/run/execution-namespace.json" \
    > "$OUT/run/namespace-cleanup-sentinel.txt" || cleanup_rc=$?
  if [ "$cleanup_rc" -ne 0 ]; then
    echo "SOURCE-FAILED: execution namespace did not fully terminate" >&2
    exit 1
  fi
  wait_for_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT" \
    > "$OUT/run/local-port-cleanup-sentinel.txt" || port_cleanup_rc=$?
  if [ "$port_cleanup_rc" -ne 0 ]; then
    echo "SOURCE-FAILED: localhost ports were not fully released" >&2
    exit 1
  fi
  source_runner_require_supervisor_healthy SOURCE
  if [ "$run_rc" -ne 0 ]; then
    echo "SOURCE-FAILED: benchmark harness rc=$run_rc" >&2
    exit 1
  fi

  local reports=("$OUT"/run/*/bench-report.json)
  [ "${#reports[@]}" -eq 1 ] && [ -f "${reports[0]}" ] || {
    echo "SOURCE-FAILED: expected exactly one bench-report.json" >&2
    exit 1
  }
  python3 "$SCRIPT_DIR/formal_runner_guard.py" validate-cleanup-artifacts \
    --identity "$OUT/run/execution-namespace.json" \
    --report "${reports[0]}" \
    --sentinel "$OUT/run/namespace-cleanup-sentinel.txt" \
    --namespace-field namespace \
    --uid-field namespaceUID
  source_runner_release_resources || {
    echo "SOURCE-FAILED: runner-owned operator or campaign lock cleanup failed" >&2
    exit 1
  }
  trap - EXIT

  local accepted_stage="$OUT/.accepted-staging"
  local accepted_dir="$OUT/accepted"
  [ ! -e "$accepted_stage" ] && [ ! -e "$accepted_dir" ] || {
    echo "SOURCE-FAILED: source acceptance directory already exists" >&2
    exit 1
  }
  mkdir "$accepted_stage"
  cp "${reports[0]}" "$accepted_stage/source-report.json"
  cp "$OUT/provenance.txt" "$accepted_stage/provenance.txt"
  cp "$OUT/planned-config.json" "$accepted_stage/planned-config.json"
  cp "$OUT/lineage.json" "$accepted_stage/lineage.json"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" source \
    --output "$accepted_stage/source-contract.json" \
    --source-report "$accepted_stage/source-report.json" \
    --source-provenance "$accepted_stage/provenance.txt"
  revalidate_source_provenance \
    "$accepted_stage/source-report.json" "$accepted_stage"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" status \
    --output "$accepted_stage/status.txt" \
    --source-report "$accepted_stage/source-report.json" \
    --source-provenance "$accepted_stage/provenance-final.txt"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" cpu \
    --output "$accepted_stage/expected-hs-cpu-matrix.json" \
    --source-report "$accepted_stage/source-report.json" \
    --source-provenance "$accepted_stage/provenance-final.txt" \
    --kind-node "$BENCH_KIND_NODE"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" completion \
    --output "$accepted_stage/source-completion.json" \
    --accepted-dir "$accepted_stage"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" verify-completion \
    --accepted-dir "$accepted_stage" \
    --allow-staging
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" publish \
    --staging-dir "$accepted_stage" \
    --accepted-dir "$accepted_dir"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
