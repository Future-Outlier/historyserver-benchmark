#!/usr/bin/env bash
# Benchmark matrix runner — creates a DEDICATED kind cluster ("bench") so the
# main development cluster and its e2e runs are never touched, builds the
# collector/historyserver images from this checkout, and runs the full matrix:
#
#   A axis (storage/HS curves):  N = 1k, 5k, 10k, 50k, 100k     @ num_cpus=0.2
#   B axis (collector rate):     N = 20k @ num_cpus 0.5/0.2/0.1/0.05
#   C axis (gzip on, savings):   N = 1k, 5k, 10k, 50k, 100k     @ num_cpus=0.2
#
# Each run is end-to-end (collector -> flush -> storage scan -> history server)
# and deletes its bucket data afterwards, so disk usage never accumulates.
#
# Usage:
#   ./run_matrix.sh                # full 14-run matrix (~2-2.5h)
#   BENCH_ONLY=A ./run_matrix.sh   # only one axis (A, B, or C)
#   BENCH_TEARDOWN=1 ...           # delete the bench kind cluster at the end
#
# If your Go module cache has permission problems, export GOMODCACHE/GOCACHE
# to writable directories before invoking.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HS_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"   # historyserver/
REPO_ROOT="$(cd "$HS_ROOT/.." && pwd)"       # kuberay/

CLUSTER="${BENCH_CLUSTER:-bench}"
NODE="${CLUSTER}-control-plane"
RAY_IMAGE="rayproject/ray:2.54.0"
OUT="${BENCH_MATRIX_OUT:-$SCRIPT_DIR/out}/matrix-$(date +%Y%m%d-%H%M%S)"
KCFG="$OUT/kubeconfig-$CLUSTER"
mkdir -p "$OUT"

log() { printf '\n=== [%s] %s ===\n' "$(date +%H:%M:%S)" "$*"; }

# --- 1. dedicated kind cluster (never the main "kind" cluster) ---------------
if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  log "creating dedicated kind cluster '$CLUSTER'"
  kind create cluster --name "$CLUSTER" --wait 180s
else
  log "reusing existing kind cluster '$CLUSTER'"
fi
kind get kubeconfig --name "$CLUSTER" > "$KCFG"
export KUBECONFIG="$KCFG"   # pinned: immune to kubectl context switches elsewhere

# --- 2. CRDs -----------------------------------------------------------------
log "installing Ray CRDs"
kubectl apply --server-side -f "$REPO_ROOT/ray-operator/config/crd/bases/" >/dev/null

# --- 3. images ---------------------------------------------------------------
GIT_SHA="$(git -C "$HS_ROOT" rev-parse --short HEAD)"
log "building collector/historyserver images from $GIT_SHA"
make -C "$HS_ROOT" localimage-build
kind load docker-image collector:v0.1.0 --name "$CLUSTER"
kind load docker-image historyserver:v0.1.0 --name "$CLUSTER"
{
  echo "git_sha: $GIT_SHA"
  docker image inspect --format 'collector: {{.Id}}' collector:v0.1.0
  docker image inspect --format 'historyserver: {{.Id}}' historyserver:v0.1.0
} > "$OUT/images.txt"

# NOTE: exporting the image from another kind node does not work — kind's
# containerd discards the compressed layer blobs after unpacking, so
# `ctr images export` fails with "content digest not found". If the node lacks
# the image, kubelet pulls it on first pod schedule anyway; pre-pulling on the
# host just makes the first run faster.
if ! docker image inspect "$RAY_IMAGE" >/dev/null 2>&1; then
  log "pulling $RAY_IMAGE"
  docker pull "$RAY_IMAGE"
fi
kind load docker-image "$RAY_IMAGE" --name "$CLUSTER"

# --- 4. local operator against the bench cluster -----------------------------
log "building + starting the operator (local binary)"
(cd "$REPO_ROOT/ray-operator" && go build -o "$OUT/kuberay-operator" .)
"$OUT/kuberay-operator" \
  --metrics-addr=:8083 --health-probe-bind-address=:8085 \
  --enable-leader-election=false --use-kubernetes-proxy \
  > "$OUT/operator.log" 2>&1 &
OPERATOR_PID=$!
trap 'kill "$OPERATOR_PID" 2>/dev/null' EXIT
sleep 5
if ! kill -0 "$OPERATOR_PID" 2>/dev/null; then
  echo "operator failed to start; see $OUT/operator.log" >&2
  exit 1
fi

# --- 5. the matrix ------------------------------------------------------------
run_one() {
  local name="$1"; shift
  local attempt rc start
  for attempt in 1 2; do
    log "RUN $name attempt=$attempt ($*)"
    start=$(date +%s)
    (cd "$HS_ROOT" && env BENCH_RUN=1 BENCH_KIND_NODE="$NODE" BENCH_OUT_DIR="$OUT/$name" "$@" \
       go test ./test/benchmark -run 'TestHistoryServerBenchmark$' -v -timeout 60m \
       > "$OUT/$name.attempt$attempt.log" 2>&1)
    rc=$?
    echo "$name attempt=$attempt rc=$rc duration=$(( $(date +%s) - start ))s" | tee -a "$OUT/matrix-status.txt"
    [ "$rc" -eq 0 ] && break
    # One retry absorbs transient infrastructure flakes (port-forward death);
    # a second consecutive failure is a real signal worth stopping on.
  done
}

ONLY="${BENCH_ONLY:-}"

if [[ -z "$ONLY" || "$ONLY" == "A" ]]; then
  for N in 1000 5000 10000 50000 100000; do
    run_one "A-n${N}" BENCH_TASK_COUNT="$N"
  done
fi

if [[ -z "$ONLY" || "$ONLY" == "B" ]]; then
  run_one "B-cpus0.5"  BENCH_TASK_COUNT=20000 BENCH_TASK_NUM_CPUS=0.5
  run_one "B-cpus0.2"  BENCH_TASK_COUNT=20000 BENCH_TASK_NUM_CPUS=0.2
  run_one "B-cpus0.1"  BENCH_TASK_COUNT=20000 BENCH_TASK_NUM_CPUS=0.1 BENCH_WORKER_MEMORY_LIMIT=4G
  run_one "B-cpus0.05" BENCH_TASK_COUNT=20000 BENCH_TASK_NUM_CPUS=0.05 BENCH_WORKER_MEMORY_LIMIT=4G
fi

if [[ -z "$ONLY" || "$ONLY" == "C" ]]; then
  for N in 1000 5000 10000 50000 100000; do
    run_one "C-gzip-n${N}" BENCH_TASK_COUNT="$N" BENCH_COMPRESSION=true
  done
fi

# --- 6. aggregate -------------------------------------------------------------
log "aggregating"
python3 "$SCRIPT_DIR/aggregate.py" "$OUT" > "$OUT/SUMMARY.md" || echo "aggregation failed" >&2
log "matrix done — summary: $OUT/SUMMARY.md"

if [[ "${BENCH_TEARDOWN:-0}" == "1" ]]; then
  log "tearing down kind cluster '$CLUSTER'"
  kind delete cluster --name "$CLUSTER"
fi
