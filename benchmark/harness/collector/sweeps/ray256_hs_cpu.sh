#!/usr/bin/env bash
# Formal Ray 2.56 History Server CPU discovery against one immutable session.
#
# This script never creates, updates, or deletes an S3 object.  The source
# session is read-only; the Go harness creates a fresh ephemeral History Server
# namespace/pod/cache for each arm.  Do not run this beside any Collector or
# other History Server campaign because they share one macOS Kind VM.
set -euo pipefail

HS_FINGERPRINT_ALGORITHM='s3-key-size-etag-content-sha256-v1'
HS_ARGS='--session-cache-size=1,--session-process-timeout=10m'
HS_CLIENT_TIMEOUT='12m'
HS_SETTLE='0s'
HS_S3_BUCKET='ray-historyserver-benchmark'
HS_S3_LOCAL_PORT=19003
HS_API_LOCAL_PORT=30080

hs_sha256() {
  LC_ALL=C shasum -a 256 "$1" | awk '{print $1}'
}

hs_build_env_unset() {
  BENCH_ENV_UNSET=(-u __KUBERAY_BENCH_ENV_SENTINEL__)
  while IFS= read -r name; do
    case "$name" in
      BENCH_*) BENCH_ENV_UNSET+=(-u "$name") ;;
    esac
  done < <(compgen -e)
}

hs_load_matrix_source() {
  local matrix="$1"
  local values
  values=$(python3 - "$matrix" <<'PY'
import json
import math
import re
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    source = json.load(stream)["source"]
metadata = source["taskLogMetadata"]
counts = metadata["counts"]
attempts = source["expectedBenchmarkAttempts"]
if type(attempts) is not int:
    raise SystemExit("matrix source expectedBenchmarkAttempts must be an integer")
for field in ("expectedObjectCount", "expectedTotalBytes"):
    if type(source.get(field)) is not int or source[field] <= 0:
        raise SystemExit(f"matrix source {field} must be a positive integer")
generation = source.get("sourceGeneration")
if generation is None:
    if attempts not in (1000, 5000, 10000):
        raise SystemExit("N=50k matrix sourceGeneration is required")
else:
    legacy_generation_fields = {
        "waveSize",
        "drivers",
        "targetTaskRate",
        "pacingVariant",
        "driverTasks",
        "driverWallSec",
        "driverRateTPS",
        "lineageSHA256",
        "rejectedBaselineReportSHA256",
    }
    required_generation_fields = legacy_generation_fields | {
        "rejectedAttemptReportSHA256s",
        "collectorCPURequest",
        "collectorCPULimit",
    }
    legacy_lower = (
        attempts in (1000, 5000, 10000)
        and set(generation) == legacy_generation_fields
    )
    if not legacy_lower and set(generation) != required_generation_fields:
        raise SystemExit("matrix sourceGeneration fields differ")
    if attempts == 50000:
        expected_wave = 2000
        expected_rate = 500
        expected_variant = "single-driver-rate500-wave2000-v2"
        expected_rejected = (
            "7873ec98d6e1d376a5994a86aaaa18ee41b6f5e1a124f67249589c152ad94264"
        )
        expected_rejected_attempts = [
            "2c191e0133dce4a95a19fe7835daf0628784276fd421de608ed8fd796d3915ef",
            "01274cdc416f6a6709706389042e5aa8e6ab382677b1886b9bf2b8459d47e677",
        ]
        expected_collector_request = "100m"
        expected_collector_limit = "2"
    elif attempts in (1000, 5000, 10000):
        expected_wave = 2000
        expected_rate = 0
        expected_variant = "baseline-wave2000-v1"
        expected_rejected = None
        expected_rejected_attempts = []
        expected_collector_request = None
        expected_collector_limit = None
    else:
        raise SystemExit("matrix source task count is unsupported")
    exact_integers = {
        "waveSize": expected_wave,
        "drivers": 1,
        "targetTaskRate": expected_rate,
        "driverTasks": attempts,
    }
    if any(
        type(generation.get(key)) is not int or generation[key] != value
        for key, value in exact_integers.items()
    ):
        raise SystemExit("matrix sourceGeneration integer controls differ")
    if generation.get("pacingVariant") != expected_variant:
        raise SystemExit("matrix sourceGeneration pacingVariant differs")
    if generation.get("rejectedBaselineReportSHA256") != expected_rejected:
        raise SystemExit("matrix sourceGeneration rejected baseline differs")
    if not legacy_lower:
        if generation.get("rejectedAttemptReportSHA256s") != expected_rejected_attempts:
            raise SystemExit("matrix sourceGeneration rejected attempts differ")
        if generation.get("collectorCPURequest") != expected_collector_request:
            raise SystemExit("matrix sourceGeneration Collector CPU request differs")
        if generation.get("collectorCPULimit") != expected_collector_limit:
            raise SystemExit("matrix sourceGeneration Collector CPU limit differs")
    lineage_sha = generation.get("lineageSHA256")
    if not isinstance(lineage_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", lineage_sha):
        raise SystemExit("matrix sourceGeneration lineage SHA-256 is invalid")
    if any(
        isinstance(generation.get(key), bool)
        or not isinstance(generation.get(key), (int, float))
        or not math.isfinite(generation[key])
        or generation[key] <= 0
        for key in ("driverWallSec", "driverRateTPS")
    ):
        raise SystemExit("matrix sourceGeneration driver measurements are invalid")
    if attempts == 50000 and not 490.0 <= float(generation["driverRateTPS"]) <= 510.0:
        raise SystemExit("matrix sourceGeneration driver rate is outside pacing band")
required_count_keys = (
    "nil",
    "present",
    "structurallyInvalid",
    "incompleteNonNil",
    "stdoutExactResolvable",
    "stderrExactResolvable",
    "legacyWholeWorkerFallback",
)
if (
    metadata.get("algorithm") != "task-log-metadata-sha256-v1"
    or not isinstance(metadata.get("sha256"), str)
    or type(metadata.get("attempts")) is not int
    or metadata.get("attempts") != attempts
    or set(counts) != set(required_count_keys)
    or any(type(counts[key]) is not int or counts[key] < 0 for key in required_count_keys)
    or counts["nil"] + counts["present"] != attempts
    or counts["structurallyInvalid"] != 0
    or counts["incompleteNonNil"] > counts["present"]
    or counts["stdoutExactResolvable"] > counts["present"]
    or counts["stderrExactResolvable"] > counts["present"]
    or counts["legacyWholeWorkerFallback"] > counts["nil"]
    or metadata.get("valid") is not True
    or metadata.get("problems") != []
):
    raise SystemExit("matrix source taskLogMetadata is invalid")
lifecycle = source.get("rayJobLifecycle")
legacy_fields = {
    "ownedCluster",
    "shutdownAfterJobFinishes",
    "ttlSecondsAfterFinished",
}
retry_fields = legacy_fields | {
    "rayJobBackoffLimit",
    "submitterBackoffLimit",
}
if not isinstance(lifecycle, dict):
    raise SystemExit("matrix source rayJobLifecycle is invalid")
if generation is None:
    if set(lifecycle) not in (legacy_fields, retry_fields):
        raise SystemExit("legacy matrix source rayJobLifecycle fields differ")
else:
    if set(lifecycle) != retry_fields:
        raise SystemExit("matrix source rayJobLifecycle fields differ")
if (
    lifecycle.get("ownedCluster") is not True
    or lifecycle.get("shutdownAfterJobFinishes") is not True
    or type(lifecycle.get("ttlSecondsAfterFinished")) is not int
    or lifecycle["ttlSecondsAfterFinished"] != 30
):
    raise SystemExit("matrix source rayJobLifecycle controls differ")
if set(lifecycle) == retry_fields and (
    type(lifecycle.get("rayJobBackoffLimit")) is not int
    or lifecycle["rayJobBackoffLimit"] != 0
    or type(lifecycle.get("submitterBackoffLimit")) is not int
    or lifecycle["submitterBackoffLimit"] != 0
):
    raise SystemExit("matrix source RayJob retry controls differ")
print("\t".join(str(source[key]) for key in (
    "spec",
    "expectedBenchmarkAttempts",
    "expectedObjectCount",
    "expectedTotalBytes",
    "sourceReportSHA256",
    "bucket",
    "collectorRuntimeID",
    "collectorImageRequested",
)))
print("\t".join((
    str(generation is not None).lower(),
    str(lifecycle["ownedCluster"]).lower(),
    str(lifecycle["shutdownAfterJobFinishes"]).lower(),
    str(lifecycle["ttlSecondsAfterFinished"]),
    str(lifecycle.get("rayJobBackoffLimit", "legacy")),
    str(lifecycle.get("submitterBackoffLimit", "legacy")),
)))
print("\t".join(str(value) for value in (
    metadata["algorithm"],
    metadata["sha256"],
    metadata["attempts"],
    *(counts[key] for key in required_count_keys),
)))
PY
  ) || return 1
  local source_values remaining lifecycle_values metadata_values
  source_values=${values%%$'\n'*}
  remaining=${values#*$'\n'}
  lifecycle_values=${remaining%%$'\n'*}
  metadata_values=${remaining#*$'\n'}
  IFS=$'\t' read -r HS_SOURCE_SPEC HS_SOURCE_TASK_COUNT \
    HS_SOURCE_OBJECT_COUNT HS_SOURCE_TOTAL_BYTES HS_SOURCE_REPORT_SHA256 HS_SOURCE_BUCKET \
    HS_SOURCE_COLLECTOR_RUNTIME_ID HS_SOURCE_COLLECTOR_IMAGE <<<"$source_values"
  IFS=$'\t' read -r HS_SOURCE_GENERATION_PRESENT HS_SOURCE_RAYJOB_OWNED \
    HS_SOURCE_SHUTDOWN_AFTER_JOB HS_SOURCE_JOB_TTL_SECONDS \
    HS_SOURCE_RAYJOB_BACKOFF_LIMIT HS_SOURCE_SUBMITTER_BACKOFF_LIMIT \
    <<<"$lifecycle_values"
  IFS=$'\t' read -r HS_SOURCE_TASK_LOG_METADATA_ALGORITHM \
    HS_SOURCE_TASK_LOG_METADATA_SHA256 HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS \
    HS_SOURCE_TASK_LOG_METADATA_NIL HS_SOURCE_TASK_LOG_METADATA_PRESENT \
    HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID \
    HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL \
    HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE \
    HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE \
    HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK <<<"$metadata_values"
  [ -n "$HS_SOURCE_SPEC" ] \
    && [ "$HS_SOURCE_TASK_COUNT" -gt 0 ] \
    && [ "$HS_SOURCE_OBJECT_COUNT" -gt 0 ] \
    && [ "$HS_SOURCE_TOTAL_BYTES" -gt 0 ] \
    && [ "$HS_SOURCE_BUCKET" = "$HS_S3_BUCKET" ] \
    && is_full_image_id "$HS_SOURCE_COLLECTOR_RUNTIME_ID" \
    && [ -n "$HS_SOURCE_COLLECTOR_IMAGE" ] \
    && [ "$HS_SOURCE_RAYJOB_OWNED" = true ] \
    && [ "$HS_SOURCE_SHUTDOWN_AFTER_JOB" = true ] \
    && [ "$HS_SOURCE_JOB_TTL_SECONDS" = 30 ] \
    && { [ "$HS_SOURCE_GENERATION_PRESENT" = false ] \
      || { [ "$HS_SOURCE_RAYJOB_BACKOFF_LIMIT" = 0 ] \
        && [ "$HS_SOURCE_SUBMITTER_BACKOFF_LIMIT" = 0 ]; }; } \
    && [ "$HS_SOURCE_TASK_LOG_METADATA_ALGORITHM" = task-log-metadata-sha256-v1 ] \
    && is_full_sha256 "$HS_SOURCE_TASK_LOG_METADATA_SHA256" \
    && [ "$HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS" = "$HS_SOURCE_TASK_COUNT" ] \
    && is_full_sha256 "$HS_SOURCE_REPORT_SHA256" || {
      echo "PREFLIGHT-FAILED: expected matrix source binding is incomplete" >&2
      return 1
    }
}

hs_runtime_values() {
  local requested tag metadata runtime_id runtime_digests unused_version unused_commit runtime_source
  requested=$(awk '$1 == "image:" && $2 ~ /historyserver/ { print $2; exit }' \
    "$HS/config/historyserver.yaml")
  [ -n "$requested" ] || {
    echo "PROVENANCE-FAILED: History Server requested image is empty" >&2
    return 1
  }
  tag=${requested##*:}
  metadata=$(runtime_image_metadata "$BENCH_KIND_NODE" historyserver "$tag") || {
    echo "PROVENANCE-FAILED: cannot resolve runtime image for $requested" >&2
    return 1
  }
  IFS=$'\t' read -r runtime_id runtime_digests unused_version unused_commit runtime_source <<<"$metadata"
  is_full_image_id "$runtime_id" && is_full_sha256 "$runtime_source" || {
    echo "PROVENANCE-FAILED: History Server runtime image metadata is incomplete" >&2
    return 1
  }
  printf '%s\t%s\t%s\n' "$requested" "$runtime_id" "$runtime_source"
}

hs_capture_initial_provenance() {
  local kind="$1"
  local source_report="$2"
  local cpu_root="${3:-}"
  local repo_head tracked_diff benchmark_source historyserver_source manifest_sha matrix_sha source_report_sha source_provenance_sha source_completion_sha
  local build_source runtime requested runtime_id runtime_source

  [ ! -e "$OUT/provenance.txt" ] || {
    echo "PROVENANCE-FAILED: refusing to overwrite $OUT/provenance.txt" >&2
    return 1
  }
  repo_head=$(git -C "$REPO_ROOT" rev-parse HEAD)
  tracked_diff=$(tracked_workspace_diff_sha256 "$REPO_ROOT")
  benchmark_source=$(benchmark_tree_sha256 "$HS/test/benchmark")
  historyserver_source=$(benchmark_tree_sha256 "$HS")
  manifest_sha=$(file_sha256 "$HS/config/historyserver.yaml")
  matrix_sha=$(file_sha256 "$OUT/expected-matrix.json")
  source_report_sha=$(file_sha256 "$source_report")
  source_provenance_sha=$(file_sha256 "$SOURCE_PROVENANCE")
  source_completion_sha=$(file_sha256 "$SOURCE_COMPLETION")
  require_same_provenance_value "source report/matrix binding" \
    "$HS_SOURCE_REPORT_SHA256" "$source_report_sha"
  build_source=$(image_build_source_sha256 historyserver)
  runtime=$(hs_runtime_values)
  IFS=$'\t' read -r requested runtime_id runtime_source <<<"$runtime"
  require_same_provenance_value "History Server image/source binding" \
    "$build_source" "$runtime_source"

  {
    printf 'campaign_kind=%s\n' "$kind"
    printf 'repo_head=%s\n' "$repo_head"
    printf 'tracked_diff_sha256=%s\n' "$tracked_diff"
    printf 'benchmark_source_sha256=%s\n' "$benchmark_source"
    printf 'historyserver_source_sha256=%s\n' "$historyserver_source"
    printf 'historyserver_manifest_sha256=%s\n' "$manifest_sha"
    printf 'expected_matrix_sha256=%s\n' "$matrix_sha"
    printf 'source_report_sha256=%s\n' "$source_report_sha"
    printf 'source_provenance_sha256=%s\n' "$source_provenance_sha"
    printf 'source_completion_sha256=%s\n' "$source_completion_sha"
    printf 'source_session=%s\n' "$HS_SOURCE_SPEC"
    printf 'source_bucket=%s\n' "$HS_SOURCE_BUCKET"
    printf 'source_fingerprint_algorithm=%s\n' "$HS_FINGERPRINT_ALGORITHM"
    printf 'source_collector_runtime_id=%s\n' "$HS_SOURCE_COLLECTOR_RUNTIME_ID"
    printf 'source_rayjob_owned=%s\n' "$HS_SOURCE_RAYJOB_OWNED"
    printf 'source_shutdown_after_job=%s\n' "$HS_SOURCE_SHUTDOWN_AFTER_JOB"
    printf 'source_job_ttl_seconds=%s\n' "$HS_SOURCE_JOB_TTL_SECONDS"
    printf 'source_rayjob_backoff_limit=%s\n' "$HS_SOURCE_RAYJOB_BACKOFF_LIMIT"
    printf 'source_submitter_backoff_limit=%s\n' "$HS_SOURCE_SUBMITTER_BACKOFF_LIMIT"
    printf 'source_task_log_metadata_algorithm=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_ALGORITHM"
    printf 'source_task_log_metadata_sha256=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_SHA256"
    printf 'source_task_log_metadata_attempts=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS"
    printf 'source_task_log_metadata_nil=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_NIL"
    printf 'source_task_log_metadata_present=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_PRESENT"
    printf 'source_task_log_metadata_structurally_invalid=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID"
    printf 'source_task_log_metadata_incomplete_non_nil=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL"
    printf 'source_task_log_metadata_stdout_exact_resolvable=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE"
    printf 'source_task_log_metadata_stderr_exact_resolvable=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE"
    printf 'source_task_log_metadata_legacy_whole_worker_fallback=%s\n' "$HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK"
    printf 'historyserver_image_requested=%s\n' "$requested"
    printf 'historyserver_build_source_sha256=%s\n' "$build_source"
    printf 'historyserver_runtime_id=%s\n' "$runtime_id"
    printf 'historyserver_runtime_build_source_sha256=%s\n' "$runtime_source"
    if [ "$kind" = 'hs-memory' ]; then
      [ -n "$cpu_root" ] || return 1
      printf 'cpu_campaign_expected_matrix_sha256=%s\n' \
        "$(file_sha256 "$cpu_root/expected-matrix.json")"
      printf 'cpu_campaign_final_provenance_sha256=%s\n' \
        "$(file_sha256 "$cpu_root/provenance-final.txt")"
    fi
  } > "$OUT/provenance.txt"
}

hs_require_initial_value_unchanged() {
  local key="$1"
  local current="$2"
  local initial
  initial=$(provenance_value "$OUT/provenance.txt" "$key")
  require_same_provenance_value "$key" "$initial" "$current"
}

hs_revalidate_provenance() {
  local kind="$1"
  local source_report="$2"
  local cpu_root="${3:-}"
  local runtime requested runtime_id runtime_source source_fingerprint

  hs_require_initial_value_unchanged repo_head "$(git -C "$REPO_ROOT" rev-parse HEAD)"
  hs_require_initial_value_unchanged tracked_diff_sha256 \
    "$(tracked_workspace_diff_sha256 "$REPO_ROOT")"
  hs_require_initial_value_unchanged benchmark_source_sha256 \
    "$(benchmark_tree_sha256 "$HS/test/benchmark")"
  hs_require_initial_value_unchanged historyserver_source_sha256 \
    "$(benchmark_tree_sha256 "$HS")"
  hs_require_initial_value_unchanged historyserver_manifest_sha256 \
    "$(file_sha256 "$HS/config/historyserver.yaml")"
  hs_require_initial_value_unchanged expected_matrix_sha256 \
    "$(file_sha256 "$OUT/expected-matrix.json")"
  hs_require_initial_value_unchanged source_report_sha256 \
    "$(file_sha256 "$source_report")"
  hs_require_initial_value_unchanged source_provenance_sha256 \
    "$(file_sha256 "$SOURCE_PROVENANCE")"
  python3 "$SCRIPT_DIR/write_hs_expected_matrix.py" verify-completion \
    --accepted-dir "$SOURCE_ACCEPTED_DIR" >/dev/null
  hs_require_initial_value_unchanged source_completion_sha256 \
    "$(file_sha256 "$SOURCE_COMPLETION")"
  hs_require_initial_value_unchanged source_bucket "$HS_SOURCE_BUCKET"
  hs_require_initial_value_unchanged source_rayjob_backoff_limit \
    "$HS_SOURCE_RAYJOB_BACKOFF_LIMIT"
  hs_require_initial_value_unchanged source_submitter_backoff_limit \
    "$HS_SOURCE_SUBMITTER_BACKOFF_LIMIT"
  hs_require_initial_value_unchanged source_task_log_metadata_algorithm \
    "$HS_SOURCE_TASK_LOG_METADATA_ALGORITHM"
  hs_require_initial_value_unchanged source_task_log_metadata_sha256 \
    "$HS_SOURCE_TASK_LOG_METADATA_SHA256"
  hs_require_initial_value_unchanged source_task_log_metadata_attempts \
    "$HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS"
  hs_require_initial_value_unchanged source_task_log_metadata_nil \
    "$HS_SOURCE_TASK_LOG_METADATA_NIL"
  hs_require_initial_value_unchanged source_task_log_metadata_present \
    "$HS_SOURCE_TASK_LOG_METADATA_PRESENT"
  hs_require_initial_value_unchanged source_task_log_metadata_structurally_invalid \
    "$HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID"
  hs_require_initial_value_unchanged source_task_log_metadata_incomplete_non_nil \
    "$HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL"
  hs_require_initial_value_unchanged source_task_log_metadata_stdout_exact_resolvable \
    "$HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE"
  hs_require_initial_value_unchanged source_task_log_metadata_stderr_exact_resolvable \
    "$HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE"
  hs_require_initial_value_unchanged source_task_log_metadata_legacy_whole_worker_fallback \
    "$HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK"
  hs_require_initial_value_unchanged historyserver_build_source_sha256 \
    "$(image_build_source_sha256 historyserver)"
  runtime=$(hs_runtime_values)
  IFS=$'\t' read -r requested runtime_id runtime_source <<<"$runtime"
  hs_require_initial_value_unchanged historyserver_image_requested "$requested"
  hs_require_initial_value_unchanged historyserver_runtime_id "$runtime_id"
  hs_require_initial_value_unchanged historyserver_runtime_build_source_sha256 "$runtime_source"
  if [ "$kind" = 'hs-memory' ]; then
    hs_require_initial_value_unchanged cpu_campaign_expected_matrix_sha256 \
      "$(file_sha256 "$cpu_root/expected-matrix.json")"
    hs_require_initial_value_unchanged cpu_campaign_final_provenance_sha256 \
      "$(file_sha256 "$cpu_root/provenance-final.txt")"
  fi

  local validator_args=(--expected-kind "$kind" --artifacts-only --print-source-fingerprint)
  if [ "$kind" = 'hs-memory' ]; then
    validator_args+=(--cpu-campaign-root "$cpu_root")
  fi
  source_fingerprint=$(python3 "$SCRIPT_DIR/validate_hs_sweep.py" \
    "${validator_args[@]}" "$OUT")
  is_full_sha256 "$source_fingerprint" || {
    echo "PROVENANCE-FAILED: source fingerprint is invalid" >&2
    return 1
  }
  [ ! -e "$OUT/provenance-final.txt" ] || {
    echo "PROVENANCE-FAILED: refusing to overwrite provenance-final.txt" >&2
    return 1
  }
  {
    cat "$OUT/provenance.txt"
    printf 'revalidation_status=valid\n'
    printf 'source_fingerprint_sha256=%s\n' "$source_fingerprint"
  } > "$OUT/provenance-final.txt"

  local final_args=(--expected-kind "$kind")
  if [ "$kind" = 'hs-memory' ]; then
    final_args+=(--cpu-campaign-root "$cpu_root")
  fi
  python3 "$SCRIPT_DIR/validate_hs_sweep.py" "${final_args[@]}" "$OUT"
}

hs_write_arm_provenance() {
  local name="$1"
  local arm_dir="$2"
  {
    printf 'arm=%s\n' "$name"
    printf 'expected_matrix_sha256=%s\n' "$(file_sha256 "$OUT/expected-matrix.json")"
    printf 'initial_provenance_sha256=%s\n' "$(hs_sha256 "$OUT/provenance.txt")"
    printf 'historyserver_runtime_id=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" historyserver_runtime_id)"
    printf 'historyserver_runtime_build_source_sha256=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" historyserver_runtime_build_source_sha256)"
    printf 'source_collector_runtime_id=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_collector_runtime_id)"
    printf 'source_rayjob_owned=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_rayjob_owned)"
    printf 'source_shutdown_after_job=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_shutdown_after_job)"
    printf 'source_job_ttl_seconds=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_job_ttl_seconds)"
    printf 'source_rayjob_backoff_limit=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_rayjob_backoff_limit)"
    printf 'source_submitter_backoff_limit=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_submitter_backoff_limit)"
    printf 'source_completion_sha256=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_completion_sha256)"
    printf 'source_task_log_metadata_algorithm=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_algorithm)"
    printf 'source_task_log_metadata_sha256=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_sha256)"
    printf 'source_task_log_metadata_attempts=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_attempts)"
    printf 'source_task_log_metadata_nil=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_nil)"
    printf 'source_task_log_metadata_present=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_present)"
    printf 'source_task_log_metadata_structurally_invalid=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_structurally_invalid)"
    printf 'source_task_log_metadata_incomplete_non_nil=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_incomplete_non_nil)"
    printf 'source_task_log_metadata_stdout_exact_resolvable=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_stdout_exact_resolvable)"
    printf 'source_task_log_metadata_stderr_exact_resolvable=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_stderr_exact_resolvable)"
    printf 'source_task_log_metadata_legacy_whole_worker_fallback=%s\n' \
      "$(provenance_value "$OUT/provenance.txt" source_task_log_metadata_legacy_whole_worker_fallback)"
  } > "$arm_dir/arm-provenance.txt"
}

hs_run_arm() {
  local name="$1"
  local cpu="$2"
  local memory="$3"
  local arm_dir="$OUT/$name"
  local start rc duration

  require_no_active_incluster_kuberay_operator
  require_no_reconcilable_kuberay_workloads
  require_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT"
  mkdir "$arm_dir"
  hs_write_arm_provenance "$name" "$arm_dir"
  start=$(date +%s)
  set +e
  (
    cd "$HS"
    env "${BENCH_ENV_UNSET[@]}" \
      BENCH_RUN=1 \
      BENCH_KIND_NODE="$BENCH_KIND_NODE" \
      BENCH_OUT_DIR="$arm_dir" \
      BENCH_TASK_COUNT="$HS_SOURCE_TASK_COUNT" \
      BENCH_HS_ONLY="$HS_SOURCE_SPEC" \
      BENCH_HS_SOURCE_OBJECT_COUNT="$HS_SOURCE_OBJECT_COUNT" \
      BENCH_HS_SOURCE_TOTAL_BYTES="$HS_SOURCE_TOTAL_BYTES" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_ALGORITHM="$HS_SOURCE_TASK_LOG_METADATA_ALGORITHM" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_SHA256="$HS_SOURCE_TASK_LOG_METADATA_SHA256" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS="$HS_SOURCE_TASK_LOG_METADATA_ATTEMPTS" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_NIL="$HS_SOURCE_TASK_LOG_METADATA_NIL" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_PRESENT="$HS_SOURCE_TASK_LOG_METADATA_PRESENT" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID="$HS_SOURCE_TASK_LOG_METADATA_STRUCTURALLY_INVALID" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL="$HS_SOURCE_TASK_LOG_METADATA_INCOMPLETE_NON_NIL" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE="$HS_SOURCE_TASK_LOG_METADATA_STDOUT_EXACT_RESOLVABLE" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE="$HS_SOURCE_TASK_LOG_METADATA_STDERR_EXACT_RESOLVABLE" \
      BENCH_HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK="$HS_SOURCE_TASK_LOG_METADATA_LEGACY_WHOLE_WORKER_FALLBACK" \
      BENCH_S3_LOCAL_PORT="$HS_S3_LOCAL_PORT" \
      BENCH_EXECUTION_IDENTITY_FILE="$arm_dir/execution-namespace.json" \
      BENCH_HS_SOURCE_RAYJOB_OWNED="$HS_SOURCE_RAYJOB_OWNED" \
      BENCH_HS_SOURCE_SHUTDOWN_AFTER_JOB="$HS_SOURCE_SHUTDOWN_AFTER_JOB" \
      BENCH_HS_SOURCE_JOB_TTL_SECONDS="$HS_SOURCE_JOB_TTL_SECONDS" \
      BENCH_HS_SOURCE_RAYJOB_BACKOFF_LIMIT="$HS_SOURCE_RAYJOB_BACKOFF_LIMIT" \
      BENCH_HS_SOURCE_SUBMITTER_BACKOFF_LIMIT="$HS_SOURCE_SUBMITTER_BACKOFF_LIMIT" \
      BENCH_HS_CPU_REQUEST="$cpu" \
      BENCH_HS_CPU_LIMIT="$cpu" \
      BENCH_HS_MEMORY_REQUEST="$memory" \
      BENCH_HS_MEMORY_LIMIT="$memory" \
      BENCH_HS_ARGS="$HS_ARGS" \
      BENCH_HS_COLD_SLO=120s \
      BENCH_HS_ENTER_TIMEOUT="$HS_CLIENT_TIMEOUT" \
      BENCH_HS_WARM_WAIT=0s \
      BENCH_HS_SESSION_SETTLE="$HS_SETTLE" \
      BENCH_WARM_ITERATIONS=1 \
      BENCH_HS_STRICT_COLD=true \
      BENCH_HS_QUERY_CONCURRENCY=1 \
      BENCH_SKIP_CLEANUP=1 \
      go test ./test/benchmark -run 'TestHistoryServerBenchmark$' \
        -count=1 -v -timeout 20m > "$OUT/$name.log" 2>&1
  )
  rc=$?
  set -e
  local cleanup_rc=0
  local port_cleanup_rc=0
  wait_for_execution_namespace_deletion "$arm_dir/execution-namespace.json" \
    > "$arm_dir/namespace-cleanup-sentinel.txt" || cleanup_rc=$?
  if [ "$cleanup_rc" -eq 0 ]; then
    wait_for_formal_host_ports_free "$HS_S3_LOCAL_PORT" "$HS_API_LOCAL_PORT" \
      > "$arm_dir/local-port-cleanup-sentinel.txt" || port_cleanup_rc=$?
  fi
  duration=$(( $(date +%s) - start ))
  if [ "$cleanup_rc" -ne 0 ]; then
    printf '%s rc=%d duration=%ds namespace-cleanup=failed\n' "$name" "$rc" "$duration" >> "$OUT/status.txt"
    echo "ARM-FAILED: $name execution namespace did not fully terminate" >&2
    return 1
  fi
  if [ "$port_cleanup_rc" -ne 0 ]; then
    printf '%s rc=%d duration=%ds local-port-cleanup=failed\n' "$name" "$rc" "$duration" >> "$OUT/status.txt"
    echo "ARM-FAILED: $name localhost ports were not fully released" >&2
    return 1
  fi
  if [ "$rc" -ne 0 ]; then
    printf '%s rc=%d duration=%ds\n' "$name" "$rc" "$duration" | tee -a "$OUT/status.txt"
    echo "ARM-FAILED: $name rc=$rc" >&2
    return 1
  fi
  printf 'ARM-SUCCEEDED name=%s rc=0\n' "$name" > "$arm_dir/arm-sentinel.txt"
  python3 "$SCRIPT_DIR/validate_hs_sweep.py" --single-arm "$name" "$OUT" >/dev/null || {
    echo "ARM-FAILED: $name semantic validation failed" >&2
    return 1
  }
  printf '%s rc=0 duration=%ds\n' "$name" "$duration" | tee -a "$OUT/status.txt"
}

main() {
  SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
  HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
  REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel)
  BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
  BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
  SOURCE_ACCEPTED_DIR=${BENCH_HS_SOURCE_ACCEPTED_DIR:-}
  OUT=${BENCH_SWEEP_OUT:-$HS/test/benchmark/out/ray256-hs-cpu-$(date +%Y%m%d-%H%M%S)}

  source "$SCRIPT_DIR/sweep_lib.sh"
  export KUBECONFIG=${BENCH_KUBECONFIG:-${KUBECONFIG:-$BENCH_SCRATCH/kubeconfig-bench}}
  export GOPATH=${BENCH_GOPATH:-$BENCH_SCRATCH/gopath}
  export GOMODCACHE=${BENCH_GOMODCACHE:-$BENCH_SCRATCH/gomodcache}
  export GOCACHE=${BENCH_GOCACHE:-$BENCH_SCRATCH/gocache}
  hs_build_env_unset

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
  local source_expected_matrix="$SOURCE_ACCEPTED_DIR/expected-hs-cpu-matrix.json"
  [ ! -e "$OUT" ] || {
    echo "PREFLIGHT-FAILED: refusing to reuse output directory: $OUT" >&2
    exit 1
  }
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

  cp "$source_expected_matrix" "$OUT/expected-matrix.json"
  hs_load_matrix_source "$OUT/expected-matrix.json"
  hs_capture_initial_provenance hs-cpu "$SOURCE_REPORT"

  local repeat cpu name
  local -a order_r1=(500m 1 4 2)
  local -a order_r2=(1 2 500m 4)
  local -a order_r3=(2 4 1 500m)
  for repeat in 1 2 3; do
    local order_name="order_r${repeat}[@]"
    for cpu in "${!order_name}"; do
      name="cpu-${cpu}-r${repeat}"
      hs_run_arm "$name" "$cpu" 8Gi
    done
  done

  printf 'SWEEP-SUCCEEDED arms=12\n' | tee -a "$OUT/status.txt"
  hs_revalidate_provenance hs-cpu "$SOURCE_REPORT"
  echo "ALL-DONE out=$OUT"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
