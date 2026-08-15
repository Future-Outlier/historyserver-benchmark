#!/usr/bin/env bash
# Ray 2.56: collector resources as a function of TASK COUNT, per node role.
#
# The practical question a user asks is "my job has N tasks — what do I give the
# collector?", not "what is my events/s". The manifest already matches the shape
# they describe: the head has num-cpus 0 so it only submits, the worker only
# executes. Those two collectors see different loads (the head carries the
# owner's definition and lifecycle events, the worker the executor's profile
# events), so both are reported separately.
#
# This also settles an open question: the 2.56 rate sweep held tasks at 50k, so
# it showed the shutdown flush does not track the event RATE — but not whether
# it tracks session SIZE. Varying N with the rate unpaced answers that.
#
# The RayJob owns the cluster. Its fixed 30-second TTL is the only post-job
# drain window, matching the lifecycle users deploy rather than adding a hidden
# driver sleep before manual deletion.
set -uo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
HS=$(cd "$SCRIPT_DIR/../../.." && pwd)
REPO_ROOT=$(git -C "$HS" rev-parse --show-toplevel) || exit 1
BENCH_KIND_NODE=${BENCH_KIND_NODE:-bench-control-plane}
BENCH_SCRATCH=${BENCH_SCRATCH:-${TMPDIR:-/tmp}/kuberay-benchmark}
RAY_IMAGE=rayproject/ray:2.56.0
OUT=${BENCH_SWEEP_OUT:-$HS/test/benchmark/out/ray256-bytask-$(date +%Y%m%d-%H%M%S)}
OPERATOR_BIN=${BENCH_OPERATOR_BIN:-$OUT/kuberay-operator}

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

mkdir -p "$OUT" "$GOMODCACHE" "$GOCACHE"
require_kubeconfig_files || exit 1
if [ -z "${BENCH_OPERATOR_BIN:-}" ]; then
  (cd "$REPO_ROOT/ray-operator" && go build -o "$OPERATOR_BIN" .) || exit 1
elif [ ! -x "$OPERATOR_BIN" ]; then
  echo "PREFLIGHT-FAILED: BENCH_OPERATOR_BIN is not executable: $OPERATOR_BIN" >&2
  exit 1
fi

kind_images=$(docker exec "$BENCH_KIND_NODE" crictl images 2>/dev/null) || {
  echo "PREFLIGHT-FAILED: cannot list images on $BENCH_KIND_NODE" >&2
  exit 1
}
grep -q '2\.56\.0' <<<"$kind_images" || {
  echo "PREFLIGHT-FAILED: $RAY_IMAGE not on the kind node" >&2
  exit 1
}

TASK_COUNTS=(1000 10000 50000 100000)
REPEATS=3
WAVE_SIZE=2000
TASK_NUM_CPUS=${BENCH_TASK_NUM_CPUS:-0.5}
WARM_ITERATIONS=3
HS_CPU=4
HS_ARGS=--session-process-timeout=30m
DRIVERS=1
TARGET_TASK_RATE=0
python3 "$SCRIPT_DIR/write_expected_matrix.py" \
  --output "$OUT/expected-matrix.json" \
  --task-count 1000 --task-count 10000 --task-count 50000 --task-count 100000 \
  --repeats "$REPEATS" --wave-size "$WAVE_SIZE" --task-num-cpus "$TASK_NUM_CPUS" \
  --ray-image "$RAY_IMAGE" --compression true --shutdown-after-job true \
  --job-ttl-seconds 30 --drain-sleep-seconds 0 --warm-iterations "$WARM_ITERATIONS" \
  --hs-cpu-request "$HS_CPU" --hs-cpu-limit "$HS_CPU" --hs-args="$HS_ARGS" \
  --skip-history-server false \
  --drivers "$DRIVERS" --target-task-rate "$TARGET_TASK_RATE" || exit 1

unset DELETE_RAYJOB_CR_AFTER_JOB_FINISHES || exit 1
capture_sweep_provenance "$RAY_IMAGE" || exit 1
cat "$OUT/provenance.txt"

"$OPERATOR_BIN" --metrics-addr=:8083 --health-probe-bind-address=:8085 \
  --enable-leader-election=false --use-kubernetes-proxy > "$OUT/operator.log" 2>&1 &
OP=$!
trap 'kill $OP 2>/dev/null' EXIT
sleep 6
kill -0 "$OP" 2>/dev/null || { echo OPERATOR-DEAD; tail -5 "$OUT/operator.log"; exit 1; }

EXPECTED_ARMS=$((${#TASK_COUNTS[@]} * REPEATS))
FAILED=0
run_one() {
  local name="$1"; shift
  local start rc
  start=$(date +%s)
  (cd "$HS" && env "${BENCH_ENV_UNSET[@]}" \
     BENCH_RUN=1 BENCH_KIND_NODE="$BENCH_KIND_NODE" BENCH_OUT_DIR="$OUT/$name" \
     BENCH_RAY_IMAGE="$RAY_IMAGE" "$@" \
     go test ./test/benchmark -run 'TestHistoryServerBenchmark$' -v -timeout 60m \
     > "$OUT/$name.log" 2>&1)
  rc=$?
  echo "$name rc=$rc duration=$(( $(date +%s) - start ))s" | tee -a "$OUT/status.txt"
  [ "$rc" -ne 0 ] && { FAILED=$((FAILED + 1)); echo "ARM-FAILED $name" >&2; }
  return 0
}

common=(BENCH_COMPRESSION=true BENCH_HS_CPU_REQUEST="$HS_CPU" BENCH_HS_CPU_LIMIT="$HS_CPU"
        BENCH_WARM_ITERATIONS="$WARM_ITERATIONS" BENCH_JOB_TIMEOUT=30m
        BENCH_SKIP_CLEANUP=1 BENCH_DRIVER_DRAIN_SLEEP=0 BENCH_SHUTDOWN_AFTER_JOB=true
        BENCH_JOB_TTL_SECONDS=30 BENCH_WAVE_SIZE="$WAVE_SIZE" BENCH_TASK_NUM_CPUS="$TASK_NUM_CPUS"
        BENCH_DRIVERS="$DRIVERS" BENCH_TARGET_TASK_RATE="$TARGET_TASK_RATE" BENCH_HS_ARGS="$HS_ARGS")

# Interleaved so machine drift spreads across sizes rather than landing on one.
for ((rep = 1; rep <= REPEATS; rep++)); do
  for n in "${TASK_COUNTS[@]}"; do
    run_one "n${n}-r${rep}" "${common[@]}" BENCH_TASK_COUNT=$n
  done
done

if [ "$FAILED" -ne 0 ]; then
  echo "SWEEP-FAILED $FAILED arms rc!=0 — results are NOT publishable" | tee -a "$OUT/status.txt"
  cat "$OUT/status.txt"; exit 1
fi
validate_completed_sweep "$EXPECTED_ARMS" || {
  echo "SWEEP-FAILED: artifact validity gate rejected one or more arms" | tee -a "$OUT/status.txt"
  exit 1
}
echo "SWEEP-SUCCEEDED arms=$EXPECTED_ARMS" | tee -a "$OUT/status.txt"
echo "ALL-DONE out=$OUT"
cat "$OUT/status.txt"
