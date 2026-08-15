#!/usr/bin/env bash
# Ray 2.56 Collector resources as a function of the configured task submit rate.
#
# This is a fixed-size, single-driver matrix: every arm submits exactly 50,000
# no-op tasks with num_cpus=0.5 in waves of 2,000. The paced arms use the
# workload generator's in-loop target-rate wait; that wait controls the input
# variable and is not a post-job drain. Every arm has DrainSleepSec=0. The owned
# RayJob and its 30-second TTL are the only post-job shutdown policy.
# BENCH_SKIP_HISTORY_SERVER=true makes every arm stop after Collector shutdown,
# storage decoding, and artifact capture; this campaign never applies a History
# Server because History Server sizing is a separate experiment.
#
# The x axis for sizing remains the measured per-Collector HTTP ingress rate,
# joined to cgroup usage by pod, NodeID, and the same 10-second window. The
# configured task rate is only the controlled input and may differ from the
# achieved task or event rate.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel) || exit 1
BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
RAY_IMAGE=rayproject/ray:2.56.0
S3_LOCAL_PORT=19002
CAP_VALIDATION=${BENCH_COLLECTOR_CAP_VALIDATION:-false}
case "$CAP_VALIDATION" in
  true) default_out=$HS/test/benchmark/out/ray256-collector-cap-formal-$(date +%Y%m%d-%H%M%S) ;;
  false) default_out=$HS/test/benchmark/out/ray256-collector-rate-formal-$(date +%Y%m%d-%H%M%S) ;;
  *) echo "PREFLIGHT-FAILED: BENCH_COLLECTOR_CAP_VALIDATION must be true or false" >&2; exit 1 ;;
esac
OUT=${BENCH_SWEEP_OUT:-$default_out}
OPERATOR_BIN=$OUT/kuberay-operator

source "$HS/test/benchmark/sweeps/sweep_lib.sh"

export KUBECONFIG=${BENCH_KUBECONFIG:-${KUBECONFIG:-$BENCH_SCRATCH/kubeconfig-bench}}
export GOMODCACHE=${GOMODCACHE:-$BENCH_SCRATCH/gomodcache}
export GOCACHE=${GOCACHE:-$BENCH_SCRATCH/gocache}

BENCH_ENV_UNSET=(-u __KUBERAY_BENCH_ENV_SENTINEL__)
while IFS= read -r name; do
  case "$name" in
    BENCH_*) BENCH_ENV_UNSET+=(-u "$name") ;;
  esac
done < <(compgen -e)

require_local_tcp_port_free "$S3_LOCAL_PORT" || exit 1
[ ! -e "$OUT" ] || {
  echo "PREFLIGHT-FAILED: refusing to overwrite existing output path $OUT" >&2
  exit 1
}
mkdir -p "$OUT" "$GOMODCACHE" "$GOCACHE"
require_kubeconfig_files || exit 1
current_context=$(kubectl config current-context) || {
  echo "PREFLIGHT-FAILED: cannot read the current kubectl context" >&2
  exit 1
}
if [ "$current_context" != "kind-bench" ]; then
  echo "PREFLIGHT-FAILED: expected kubectl context kind-bench, found $current_context" >&2
  exit 1
fi
if [ "$BENCH_KIND_NODE" != "bench-control-plane" ]; then
  echo "PREFLIGHT-FAILED: expected kind node bench-control-plane, found $BENCH_KIND_NODE" >&2
  exit 1
fi
(cd "$REPO_ROOT/ray-operator" && go build -o "$OPERATOR_BIN" .) || exit 1

kind_images=$(docker exec "$BENCH_KIND_NODE" crictl images 2>/dev/null) || {
  echo "PREFLIGHT-FAILED: cannot list images on $BENCH_KIND_NODE" >&2
  exit 1
}
grep -q '2\.56\.0' <<<"$kind_images" || {
  echo "PREFLIGHT-FAILED: $RAY_IMAGE not on the kind node" >&2
  exit 1
}

TASK_COUNT=50000
REPEATS=3
WAVE_SIZE=2000
TASK_NUM_CPUS=0.5
WARM_ITERATIONS=3
HS_CPU=4
HS_ARGS=--session-process-timeout=30m
DRIVERS=1

matrix_kind=by-rate
matrix_collector_args=()
common_collector_args=()
if [ "$CAP_VALIDATION" = true ]; then
  # The uncapped formal sweep's highest measured load came from the 3,000
  # tasks/s arm, not its unpaced arm. Re-run only that controlled input and
  # report the achieved per-Collector event rates; target rate is never used as
  # a substitute for measured ingress.
  TARGET_TASK_RATES=(3000)
  matrix_kind=collector-cap
  matrix_collector_args=(
    --collector-cpu-request 150m --collector-cpu-limit 1200m
    --collector-memory-request 160Mi --collector-memory-limit 192Mi
  )
  common_collector_args=(
    BENCH_COLLECTOR_CPU_REQUEST=150m BENCH_COLLECTOR_CPU_LIMIT=1200m
    BENCH_COLLECTOR_MEMORY_REQUEST=160Mi BENCH_COLLECTOR_MEMORY_LIMIT=192Mi
  )
else
  TARGET_TASK_RATES=(250 500 1000 2000 3000 0)
fi

matrix_rate_args=()
for target_rate in "${TARGET_TASK_RATES[@]}"; do
  matrix_rate_args+=(--target-task-rate "$target_rate")
done
python3 "$SCRIPT_DIR/write_expected_matrix.py" \
  --output "$OUT/expected-matrix.json" --kind "$matrix_kind" \
  --task-count "$TASK_COUNT" --repeats "$REPEATS" \
  --wave-size "$WAVE_SIZE" --task-num-cpus "$TASK_NUM_CPUS" \
  --ray-image "$RAY_IMAGE" --compression true --shutdown-after-job true \
  --job-ttl-seconds 30 --drain-sleep-seconds 0 \
  --warm-iterations "$WARM_ITERATIONS" --hs-cpu-request "$HS_CPU" \
  --hs-cpu-limit "$HS_CPU" --hs-args="$HS_ARGS" --skip-history-server true \
  --drivers "$DRIVERS" \
  "${matrix_collector_args[@]}" \
  "${matrix_rate_args[@]}" || exit 1

unset DELETE_RAYJOB_CR_AFTER_JOB_FINISHES || exit 1
capture_sweep_provenance "$RAY_IMAGE" || exit 1
cat "$OUT/provenance.txt"

"$OPERATOR_BIN" --metrics-addr=:8083 --health-probe-bind-address=:8085 \
  --enable-leader-election=false --use-kubernetes-proxy > "$OUT/operator.log" 2>&1 &
OP=$!
trap 'kill $OP 2>/dev/null' EXIT
sleep 6
kill -0 "$OP" 2>/dev/null || { echo OPERATOR-DEAD; tail -5 "$OUT/operator.log"; exit 1; }

EXPECTED_ARMS=$((${#TARGET_TASK_RATES[@]} * REPEATS))
run_one() {
  local name="$1"; shift
  local start rc
  start=$(date +%s)
  (cd "$HS" && env "${BENCH_ENV_UNSET[@]}" \
     BENCH_RUN=1 BENCH_KIND_NODE="$BENCH_KIND_NODE" BENCH_OUT_DIR="$OUT/$name" \
     BENCH_RAY_IMAGE="$RAY_IMAGE" BENCH_S3_LOCAL_PORT="$S3_LOCAL_PORT" "$@" \
     go test ./test/benchmark -run 'TestHistoryServerBenchmark$' -v -timeout 60m \
     > "$OUT/$name.log" 2>&1)
  rc=$?
  echo "$name rc=$rc duration=$(( $(date +%s) - start ))s" | tee -a "$OUT/status.txt"
  if [ "$rc" -ne 0 ]; then
    echo "ARM-FAILED $name" >&2
    exit 1
  fi
  if ! python3 "$SCRIPT_DIR/validate_sweep.py" "$OUT" --single-arm "$name"; then
    echo "SWEEP-FAILED: semantic validation rejected $name" | tee -a "$OUT/status.txt"
    exit 1
  fi
  return 0
}

common=(BENCH_TASK_COUNT="$TASK_COUNT" BENCH_COMPRESSION=true
        BENCH_HS_CPU_REQUEST="$HS_CPU" BENCH_HS_CPU_LIMIT="$HS_CPU"
        BENCH_WARM_ITERATIONS="$WARM_ITERATIONS" BENCH_JOB_TIMEOUT=30m
        BENCH_SKIP_CLEANUP=1 BENCH_DRIVER_DRAIN_SLEEP=0
        BENCH_SKIP_HISTORY_SERVER=true
        BENCH_SHUTDOWN_AFTER_JOB=true BENCH_JOB_TTL_SECONDS=30
        BENCH_WAVE_SIZE="$WAVE_SIZE" BENCH_TASK_NUM_CPUS="$TASK_NUM_CPUS"
        BENCH_DRIVERS="$DRIVERS" BENCH_HS_ARGS="$HS_ARGS"
        "${common_collector_args[@]}")

# Repeat is the outer loop so every target is revisited over the campaign rather
# than measuring all three samples in one adjacent, drift-prone block.
for ((rep = 1; rep <= REPEATS; rep++)); do
  for target_rate in "${TARGET_TASK_RATES[@]}"; do
    if [ "$target_rate" -eq 0 ]; then
      arm=unpaced
    else
      arm=rate$target_rate
    fi
    run_one "${arm}-r${rep}" "${common[@]}" BENCH_TARGET_TASK_RATE="$target_rate"
  done
done

validate_completed_sweep "$EXPECTED_ARMS" || {
  echo "SWEEP-FAILED: artifact validity gate rejected one or more arms" | tee -a "$OUT/status.txt"
  exit 1
}
if ! echo "SWEEP-SUCCEEDED arms=$EXPECTED_ARMS" | tee -a "$OUT/status.txt"; then
  echo "SWEEP-FAILED: could not persist success sentinel" >&2
  exit 1
fi
echo "ALL-DONE out=$OUT"
cat "$OUT/status.txt"
